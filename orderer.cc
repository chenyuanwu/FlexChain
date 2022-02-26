#include "orderer.h"

#include "log.h"
#include "utils.h"

atomic<unsigned long> commit_index(0);
atomic<unsigned long> last_log_index(0);
deque<atomic<unsigned long>> next_index;
deque<atomic<unsigned long>> match_index;
vector<string> follower_grpc_endpoints;
TransactionQueue tq;
Role role;
pthread_mutex_t logger_lock;
atomic<bool> ready_flag = false;
atomic<bool> end_flag = false;
atomic<long> total_ops = 0;

void *log_replication_thread(void *arg) {
    struct ThreadContext ctx = *(struct ThreadContext *)arg;
    log_info(stderr, "[server_index = %d]log replication thread is running for follower %s.", ctx.server_index, ctx.grpc_endpoint.c_str());
    shared_ptr<grpc::Channel> channel = grpc::CreateChannel(ctx.grpc_endpoint, grpc::InsecureChannelCredentials());
    unique_ptr<ConsensusComm::Stub> stub(ConsensusComm::NewStub(channel));

    ifstream log("./consensus/raft.log", ios::in);
    assert(log.is_open());

    while (true) {
        if (last_log_index >= next_index[ctx.server_index]) {
            // log_debug(stderr, "[server_index = %d]send append_entries RPC. last_log_index = %ld. next_index = %ld.",
            //           ctx.server_index, last_log_index.load(), next_index[ctx.server_index].load());
            /* send AppendEntries RPC */
            ClientContext context;
            AppendRequest app_req;
            AppendResponse app_rsp;

            app_req.set_leader_commit(commit_index);
            int index = 0;
            for (; index < LOG_ENTRY_BATCH && next_index[ctx.server_index] + index <= last_log_index; index++) {
                uint32_t size;
                log.read((char *)&size, sizeof(uint32_t));
                char *entry_ptr = (char *)malloc(size);
                log.read(entry_ptr, size);
                app_req.add_log_entries(entry_ptr, size);
                free(entry_ptr);
            }

            Status status = stub->append_entries(&context, app_req, &app_rsp);
            if (!status.ok()) {
                log_err("[server_index = %d]gRPC failed with error message: %s.", ctx.server_index, status.error_message().c_str());
                continue;
            }

            next_index[ctx.server_index] += index;
            match_index[ctx.server_index] = next_index[ctx.server_index] - 1;
            // log_debug(stderr, "[server_index = %d]match_index is %ld.", ctx.server_index, match_index[ctx.server_index].load());
        }
    }
}

void *block_formation_thread(void *arg) {
    /* set up grpc channels to validators */
    string configfile = *(string *)(arg);
    string validator_grpc_endpoint;
    fstream fs;
    fs.open(configfile, fstream::in);
    for (string line; getline(fs, line);) {
        vector<string> tmp = split(line, "=");
        assert(tmp.size() == 2);
        if (tmp[0] == "validator") {
            validator_grpc_endpoint = tmp[1];
        }
    }
    shared_ptr<grpc::Channel> channel = grpc::CreateChannel(validator_grpc_endpoint, grpc::InsecureChannelCredentials());
    while (channel->GetState(true) != GRPC_CHANNEL_READY)
        ;
    log_info(stderr, "block formation thread: channel for validator is in ready state.");
    unique_ptr<ComputeComm::Stub> stub(ComputeComm::NewStub(channel));

    log_info(stderr, "Block formation thread is running.");
    ifstream log("./consensus/raft.log", ios::in);
    assert(log.is_open());

    unsigned long last_applied = 0;
    int majority = follower_grpc_endpoints.size() / 2;
    int block_index = 0;
    int trans_index = 0;
    size_t max_block_size = 100 * 1024;
    size_t curr_size = 0;
    int local_ops = 0;

    ClientContext context;
    Block block;
    google::protobuf::Empty rsp;

    while (!end_flag) {
        if (role == LEADER) {
            int N = commit_index + 1;
            int count = 0;
            for (int i = 0; i < match_index.size(); i++) {
                if (match_index[i] >= N) {
                    count++;
                }
            }
            if (count >= majority) {
                commit_index = N;
                // log_debug(stderr, "commit_index is updated to %ld.", commit_index.load());
            }
        }

        if (commit_index > last_applied) {
            last_applied++;
            /* put the entry in current block and generate dependency graph */
            uint32_t size;
            log.read((char *)&size, sizeof(uint32_t));
            char *entry_ptr = (char *)malloc(size);
            log.read(entry_ptr, size);
            curr_size += size;
            string serialized_transaction(entry_ptr, size);
            free(entry_ptr);

            log_debug(stderr, "[block_id = %d, trans_id = %d]: log_entry = %s", block_index, trans_index, entry_ptr);
            Endorsement *transaction = block.add_transactions();
            if (!transaction->ParseFromString(serialized_transaction)) {
                log_err("block formation thread: error in deserialising transaction.");
            }
            local_ops++;

            trans_index++;

            if (curr_size >= max_block_size) {
                /* cut the block and send it to all validators */
                block.set_block_id(block_index);
                Status status = stub->send_to_validator(&context, block, &rsp);  // TODO: use client stream + async

                curr_size = 0;
                block_index++;
                trans_index = 0;

                block.clear_block_id();
                block.clear_transactions();
            }
        }
    }
    total_ops = local_ops;
}

class ConsensusCommImpl final : public ConsensusComm::Service {
   public:
    explicit ConsensusCommImpl() : log("./consensus/raft.log", ios::out | ios::binary) {}

    /* implementation of AppendEntriesRPC */
    Status append_entries(ServerContext *context, const AppendRequest *request, AppendResponse *response) override {
        int i = 0;
        for (; i < request->log_entries_size(); i++) {
            uint32_t size = request->log_entries(i).size();
            log.write((char *)&size, sizeof(uint32_t));
            log.write(request->log_entries(i).c_str(), size);
            last_log_index++;
        }
        log.flush();

        uint64_t leader_commit = request->leader_commit();
        if (leader_commit > commit_index) {
            if (leader_commit > last_log_index) {
                commit_index = last_log_index.load();
            } else {
                commit_index = leader_commit;
            }
        }

        log_debug(stderr, "AppendEntriesRPC finished: last_log_index = %ld, commit_index = %ld.", last_log_index.load(), commit_index.load());

        return Status::OK;
    }

    Status send_to_leader(ServerContext *context, const Endorsement *endorsement, google::protobuf::Empty *response) override {
        pthread_mutex_lock(&tq.mutex);
        tq.trans_queue.emplace(endorsement->SerializeAsString());
        pthread_mutex_unlock(&tq.mutex);

        return Status::OK;
    }

   private:
    ofstream log;
};

void run_leader(const std::string &server_address, std::string configfile) {
    std::filesystem::remove_all("./consensus");
    std::filesystem::create_directory("./consensus");

    ofstream log("./consensus/raft.log", ios::out | ios::binary);

    /* spawn replication threads and the block formation thread */
    pthread_t *repl_tids;
    repl_tids = (pthread_t *)malloc(sizeof(pthread_t) * follower_grpc_endpoints.size());
    struct ThreadContext *ctxs = (struct ThreadContext *)calloc(follower_grpc_endpoints.size(), sizeof(struct ThreadContext));
    for (int i = 0; i < follower_grpc_endpoints.size(); i++) {
        next_index.emplace_back(1);
        match_index.emplace_back(0);
        ctxs[i].grpc_endpoint = follower_grpc_endpoints[i];
        ctxs[i].server_index = i;
        pthread_create(&repl_tids[i], NULL, log_replication_thread, &ctxs[i]);
        pthread_detach(repl_tids[i]);
    }
    pthread_t block_form_tid;
    pthread_create(&block_form_tid, NULL, block_formation_thread, &configfile);
    pthread_detach(block_form_tid);

    /* start the grpc server for ConsensusComm */
    ConsensusCommImpl service;
    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    log_info(stderr, "RPC server listening on %s", server_address.c_str());

    ready_flag = true;

    while (true) {
        pthread_mutex_lock(&tq.mutex);
        int i = 0;
        for (; (!tq.trans_queue.empty()) && i < LOG_ENTRY_BATCH; i++) {
            uint32_t size = tq.trans_queue.front().size();
            log.write((char *)&size, sizeof(uint32_t));
            log.write(tq.trans_queue.front().c_str(), tq.trans_queue.front().size());
            tq.trans_queue.pop();
        }
        log.flush();
        last_log_index += i;
        pthread_mutex_unlock(&tq.mutex);
    }
}

void *client_thread(void *arg) {
    int trans_per_interval = 3000;
    int interval = 20000;

    default_random_engine generator;
    uniform_int_distribution<int> distribution(0, 200);

    while (!ready_flag)
        ;

    while (!end_flag) {
        int number = distribution(generator);
        char value[1024] = {0};
        string str = "value" + to_string(number);
        strcpy(value, str.c_str());
        pthread_mutex_lock(&tq.mutex);
        tq.trans_queue.emplace(value, 1024);
        pthread_mutex_unlock(&tq.mutex);
    }
}

void *run_client(void *arg) {
    pthread_t client_tid;
    pthread_create(&client_tid, NULL, client_thread, NULL);

    while (!ready_flag)
        ;

    log_info(stderr, "*******************************benchmarking started*******************************");
    chrono::milliseconds before, after;
    before = chrono::duration_cast<chrono::milliseconds>(chrono::system_clock::now().time_since_epoch());
    sleep(10);
    end_flag = 1;
    after = chrono::duration_cast<chrono::milliseconds>(chrono::system_clock::now().time_since_epoch());

    void *status;
    pthread_join(client_tid, &status);

    while (total_ops == 0)
        ;

    log_info(stderr, "*******************************benchmarking completed*******************************");
    uint64_t time = (after - before).count();
    log_info(stderr, "throughput = %f /seconds.", ((float)total_ops.load() / time) * 1000);
}

void run_follower(const std::string &server_address) {
    std::filesystem::remove_all("./consensus");
    std::filesystem::create_directory("./consensus");

    /* start the grpc server for ConsensusComm */
    ConsensusCommImpl service;
    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    log_info(stderr, "RPC server listening on %s", server_address.c_str());

    pthread_t block_form_tid;
    pthread_create(&block_form_tid, NULL, block_formation_thread, NULL);
    pthread_detach(block_form_tid);

    server->Wait();
}

int main(int argc, char *argv[]) {
    int opt;
    string configfile = "config/consensus.config";
    string server_addr;
    while ((opt = getopt(argc, argv, "hlfa:c:")) != -1) {
        switch (opt) {
            case 'h':
                fprintf(stderr, "compute server usage:\n");
                fprintf(stderr, "\t-h: print this help message\n");
                fprintf(stderr, "\t-l: set role to be the leader\n");
                fprintf(stderr, "\t-f: set role to be a follower\n");
                fprintf(stderr, "\t-a <server_ip:server_port>: the listening addr of grpc server\n");
                fprintf(stderr, "\t-c <path_to_config_file>: path to the configuration file\n");
                exit(0);
            case 'c':
                configfile = string(optarg);
                break;
            case 'l':
                role = LEADER;
                break;
            case 'f':
                role = FOLLOWER;
                break;
            case 'a':
                server_addr = std::string(optarg);
                break;
            default:
                fprintf(stderr, "Invalid option -%c\n", opt);
                exit(1);
                break;
        }
    }
    assert(!server_addr.empty());
    /* init logger */
    pthread_mutex_init(&logger_lock, NULL);

    if (role == LEADER) {
        fstream fs;
        fs.open(configfile, fstream::in);
        for (string line; getline(fs, line);) {
            vector<string> tmp = split(line, "=");
            assert(tmp.size() == 2);
            if (tmp[0] == "follower") {
                follower_grpc_endpoints.push_back(tmp[1]);
            }
        }

        pthread_t client_id;
        pthread_create(&client_id, NULL, run_client, NULL);

        run_leader(server_addr, configfile);
    } else if (role == FOLLOWER) {
        run_follower(server_addr);
    }

    return 0;
}