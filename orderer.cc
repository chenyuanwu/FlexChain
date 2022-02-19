#include "orderer.h"

#include "log.h"

atomic<unsigned long> commit_index(0);
atomic<unsigned long> last_log_index(0);
vector<unsigned long> next_index;
deque<atomic<unsigned long>> match_index;
vector<string> grpc_endpoints;
TransactionQueue tq;
Role role;
pthread_mutex_t logger_lock;

void *log_replication_thread(void *arg) {
    int server_index = *(int *)arg;
    shared_ptr<grpc::Channel> channel = grpc::CreateChannel(grpc_endpoints[server_index], grpc::InsecureChannelCredentials());
    unique_ptr<Consensus::Stub> stub(Consensus::NewStub(channel));

    ifstream log("./consensus/raft.log", ios::in);
    assert(log.is_open());

    while (true) {
        if (last_log_index >= next_index[server_index]) {
            /* send AppendEntries RPC */
            ClientContext context;
            AppendRequest app_req;
            AppendResponse app_rsp;

            app_req.set_leader_commit(commit_index);
            int index = 0;
            for (; index < LOG_ENTRY_BATCH && next_index[server_index] + index <= last_log_index; index++) {
                uint32_t size;
                log.read((char *)&size, sizeof(uint32_t));
                char *entry_ptr = (char *)malloc(size);
                log.read(entry_ptr, size);
                app_req.set_log_entries(index, entry_ptr, size);
                free(entry_ptr);
            }

            Status status = stub->append_entries(&context, app_req, &app_rsp);
            if (!status.ok()) {
                log_err("gRPC failed with error message: %s.", status.error_message().c_str());
                continue;
            }

            next_index[server_index] += index;
            match_index[server_index] += index - 1;
        }
    }
}

void *block_formation_thread(void *arg) {
    log_info(stderr, "Block formation thread is running.");
    ifstream log("./consensus/raft.log", ios::in);
    assert(log.is_open());

    unsigned long last_applied = 0;
    int majority = grpc_endpoints.size() / 2;
    int block_index = 0;
    int trans_index = 0;
    size_t max_block_size = 100 * 1024;
    size_t curr_size = 0;

    while (true) {
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

            log_debug(stderr, "[block_id = %d, trans_id = %d]: log_entry = %s", block_index, trans_index, entry_ptr);

            trans_index++;

            if (curr_size >= max_block_size) {
                /* cut the block and send it to all validators*/
                curr_size = 0;
                block_index++;
                trans_index = 0;
            }
        }
    }
}

void run_leader() {
    std::filesystem::remove_all("./consensus");
    std::filesystem::create_directory("./consensus");

    ofstream log("./consensus/raft.log", ios::out | ios::binary);

    pthread_t *repl_tids;
    repl_tids = (pthread_t *)malloc(sizeof(pthread_t) * grpc_endpoints.size());
    for (int i = 0; i < grpc_endpoints.size(); i++) {
        next_index.emplace_back(1);
        match_index.emplace_back(0);
        pthread_create(&repl_tids[i], NULL, log_replication_thread, &i);
    }
    pthread_t block_form_tid;
    pthread_create(&block_form_tid, NULL, block_formation_thread, NULL);

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

class ConsensusImpl final : public Consensus::Service {
   public:
    explicit ConsensusImpl() : log("./consensus/raft.log", ios::out | ios::binary) {}

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

        return Status::OK;
    }

   private:
    ofstream log;
};

void run_follower(const std::string &server_address) {
    std::filesystem::remove_all("./consensus");
    std::filesystem::create_directory("./consensus");

    ConsensusImpl service;
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
    /* init logger */
    pthread_mutex_init(&logger_lock, NULL);

    if (role == LEADER) {
        fstream fs;
        fs.open(configfile, fstream::in);
        for (string line; getline(fs, line);) {
            grpc_endpoints.push_back(line);
        }
        run_leader();
    } else if (role == FOLLOWER) {
        assert(!server_addr.empty());
        run_follower(server_addr);
    }

    return 0;
}