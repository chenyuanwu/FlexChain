#include "compute_server.h"

#include <assert.h>

#include "config.h"
#include "log.h"
#include "setup_ib.h"
#include "utils.h"

struct CConfigInfo c_config_info;
struct ComputeIBInfo c_ib_info;
RequestQueue rq;
DataCache datacache;
MetaDataCache metadatacache;
shared_ptr<grpc::Channel> channel_ptr;
pthread_mutex_t logger_lock;
FILE *logger_fp;

/* read from the disaggregated key value store */
string kv_get(int thread_index, KVStableClient &client, const string &key) {
    /* find cursor of this key */
    pthread_mutex_lock(&metadatacache.hashtable_lock);
    uint64_t ptr_next = 0;
    auto it = metadatacache.key_hashtable.find(key);
    if (it != metadatacache.key_hashtable.end()) {
        ptr_next = it->second->ptr_next;
        MetaDataCache::EntryHeader h = *(it->second);
        h.key = it->second->key;
        metadatacache.lru_list.erase(it->second);
        auto it_new = metadatacache.lru_list.insert(metadatacache.lru_list.begin(), h);  // update lru list and pointer in hashtable
        it->second = it_new;
        log_info(stderr, "kv_get[key = %s]: remote address 0x%lx is found in local metadata cache.", key.c_str(), ptr_next);
    }
    pthread_mutex_unlock(&metadatacache.hashtable_lock);

    if (ptr_next == 0) {
        /* ask the control plane */
        char *ctrl_buf = c_ib_info.ib_control_buf + thread_index * c_config_info.ctrl_msg_size;
        bzero(ctrl_buf, c_config_info.ctrl_msg_size);
        char *write_buf = ctrl_buf;
        memcpy(write_buf, ADDR_MSG, CTL_MSG_TYPE_SIZE);
        write_buf += CTL_MSG_TYPE_SIZE;
        uint32_t key_len = key.length();
        assert(key_len < c_config_info.ctrl_msg_size);
        memcpy(write_buf, &key_len, sizeof(uint32_t));
        write_buf += sizeof(uint32_t);
        memcpy(write_buf, key.c_str(), key_len);

        post_send(c_config_info.ctrl_msg_size, c_ib_info.mr_control->lkey, (uintptr_t)ctrl_buf, 0,
                  c_ib_info.qp[thread_index], ctrl_buf);
        post_recv(c_config_info.ctrl_msg_size, c_ib_info.mr_control->lkey, (uintptr_t)ctrl_buf,
                  c_ib_info.qp[thread_index], ctrl_buf);

        int ret = poll_completion(thread_index, c_ib_info.cq[thread_index], IBV_WC_RECV);

        char *read_buf = ctrl_buf;
        if (strncmp(read_buf, BACKOFF_MSG, CTL_MSG_TYPE_SIZE) == 0) {
            /* in evection now */
            log_info(stderr, "kv_get[key = %s]: remote buffer of this key is in eviction, retry.", key.c_str());
            usleep(2000);
            return kv_get(thread_index, client, key);
        } else if (strncmp(read_buf, FOUND_MSG, CTL_MSG_TYPE_SIZE) == 0) {
            read_buf += CTL_MSG_TYPE_SIZE;
            memcpy(&ptr_next, read_buf, sizeof(uint64_t));
            /* update local metadata cache */
            struct MetaDataCache::EntryHeader h;
            h.ptr_next = ptr_next;
            h.key = key;
            pthread_mutex_lock(&metadatacache.hashtable_lock);
            auto it_new = metadatacache.lru_list.insert(metadatacache.lru_list.begin(), h);
            metadatacache.key_hashtable[key] = it_new;
            pthread_mutex_unlock(&metadatacache.hashtable_lock);

            log_info(stderr, "kv_get[key = %s]: remote address 0x%lx is found by contacting the control plane.",
                     key.c_str(), ptr_next);
        }
    }

    if (ptr_next != 0) {
        /* have found cursor of this key */
        pthread_mutex_lock(&datacache.hashtable_lock);
        uint64_t local_ptr = 0;
        auto it = datacache.addr_hashtable.find(ptr_next);
        if (it != datacache.addr_hashtable.end()) {
            local_ptr = it->second->l_addr;
            struct DataCache::Frame f = *(it->second);
            datacache.lru_list.erase(it->second);  // update lru list and pointer in hashtable
            auto it_new = datacache.lru_list.insert(datacache.lru_list.begin(), f);
            it->second = it_new;
        }
        pthread_mutex_unlock(&datacache.hashtable_lock);

        if (local_ptr != 0) {
            /* the buffer is cached locally */
            log_info(stderr, "kv_get[key = %s]: found in local data cache (raddr = 0x%lx, laddr = 0x%lx).",
                     key.c_str(), ptr_next, local_ptr);
            unsigned long offset = sizeof(uint64_t) * 2 + sizeof(uint8_t) + sizeof(uint32_t) + key.length();
            char *val_ptr = (char *)local_ptr;
            val_ptr += offset;
            return string(val_ptr, c_config_info.data_msg_size - offset);
        } else {
            /* allocate a buffer in local datacache */
            pthread_mutex_lock(&datacache.hashtable_lock);
            if (datacache.free_addrs.size()) {
                local_ptr = datacache.free_addrs.front();
                datacache.free_addrs.pop();
            } else {
                local_ptr = datacache.lru_list.back().l_addr;
                uint64_t remote_ptr = datacache.lru_list.back().r_addr;
                datacache.lru_list.pop_back();
                auto it = datacache.addr_hashtable.find(remote_ptr);
                if (it != datacache.addr_hashtable.end() && it->second->l_addr == local_ptr) {
                    datacache.addr_hashtable.erase(it);
                }
            }
            pthread_mutex_unlock(&datacache.hashtable_lock);

            /* find the latest version */
            bzero((char *)local_ptr, c_config_info.data_msg_size);
            post_read(c_config_info.data_msg_size, c_ib_info.mr_data->lkey, local_ptr, c_ib_info.qp[thread_index],
                      (char *)local_ptr, ptr_next, c_ib_info.remote_mr_data_rkey);
            // chain walk is not needed for M-C topology
            int ret = poll_completion(thread_index, c_ib_info.cq[thread_index], IBV_WC_RDMA_READ);
            uint64_t ptr;
            memcpy(&ptr, (char *)local_ptr, sizeof(uint64_t));
            assert(ptr == 0);
            log_info(stderr, "kv_get[key = %s]: finished RDMA read from remote memory pool (raddr = 0x%lx).",
                     key.c_str(), ptr_next);

            uint8_t invalid;
            char *val_ptr = (char *)local_ptr;
            val_ptr += sizeof(uint64_t) * 2;
            memcpy(&invalid, val_ptr, sizeof(uint8_t));

            if (invalid) {
                /* in eviction now */
                log_info(stderr, "kv_get[key = %s]: invalid buffer (raddr = 0x%lx) due to eviction, discard and retry.",
                         key.c_str(), ptr_next);
                datacache.free_addrs.push(local_ptr);
                usleep(2000);
                return kv_get(thread_index, client, key);
            } else {
                /* update local cache */
                struct DataCache::Frame f = {
                    .l_addr = local_ptr,
                    .r_addr = ptr_next};
                pthread_mutex_lock(&datacache.hashtable_lock);
                auto it_new = datacache.lru_list.insert(datacache.lru_list.begin(), f);
                datacache.addr_hashtable[ptr_next] = it_new;
                pthread_mutex_unlock(&datacache.hashtable_lock);

                unsigned long offset = sizeof(uint64_t) * 2 + sizeof(uint8_t) + sizeof(uint32_t) + key.length();
                val_ptr = (char *)local_ptr;
                val_ptr += offset;
                return string(val_ptr, c_config_info.data_msg_size - offset);
            }
        }
    } else {
        /* search in SSTables on the storage server */
        log_info(stderr, "kv_get[key = %s]: read from sstables.", key.c_str());
        string value;
        int ret = client.read_sstables(key, value);
        return value;
    }
}

/* put to the disaggregated key value store */
int kv_put(int thread_index, const string &key, const string &value) {
    /* allocate a remote buffer */
    char *ctrl_buf = c_ib_info.ib_control_buf + thread_index * c_config_info.ctrl_msg_size;
    bzero(ctrl_buf, c_config_info.ctrl_msg_size);
    memcpy(ctrl_buf, ALLOC_MSG, CTL_MSG_TYPE_SIZE);

    post_send(c_config_info.ctrl_msg_size, c_ib_info.mr_control->lkey, (uintptr_t)ctrl_buf, 0,
              c_ib_info.qp[thread_index], ctrl_buf);
    post_recv(c_config_info.ctrl_msg_size, c_ib_info.mr_control->lkey, (uintptr_t)ctrl_buf,
              c_ib_info.qp[thread_index], ctrl_buf);

    /* allocate a local buffer, write to local buffer */
    pthread_mutex_lock(&datacache.hashtable_lock);
    uint64_t local_ptr;
    if (datacache.free_addrs.size()) {
        local_ptr = datacache.free_addrs.front();
        datacache.free_addrs.pop();
    } else {
        local_ptr = datacache.lru_list.back().l_addr;
        uint64_t remote_ptr = datacache.lru_list.back().r_addr;
        datacache.lru_list.pop_back();
        auto it = datacache.addr_hashtable.find(remote_ptr);
        if (it != datacache.addr_hashtable.end() && it->second->l_addr == local_ptr) {
            datacache.addr_hashtable.erase(it);
        }
    }
    pthread_mutex_unlock(&datacache.hashtable_lock);

    char *write_ptr = (char *)local_ptr;
    bzero(write_ptr, c_config_info.data_msg_size);
    write_ptr += sizeof(uint64_t) * 2 + sizeof(uint8_t);
    uint32_t key_len = key.length();
    memcpy(write_ptr, &key_len, sizeof(uint32_t));
    write_ptr += sizeof(uint32_t);
    memcpy(write_ptr, key.c_str(), key_len);
    write_ptr += key_len;
    memcpy(write_ptr, value.c_str(), value.length());

    /* write to remote buffer */
    int ret = poll_completion(thread_index, c_ib_info.cq[thread_index], IBV_WC_RECV);
    uint64_t remote_ptr;
    memcpy(&remote_ptr, ctrl_buf, sizeof(uint64_t));
    ctrl_buf += sizeof(uint64_t);
    uint32_t index;
    memcpy(&index, ctrl_buf, sizeof(uint32_t));
    log_info(stderr, "kv_put[key = %s]: remote addr 0x%lx is allocated.", key.c_str(), remote_ptr);

    post_write_with_imm(c_config_info.data_msg_size, c_ib_info.mr_data->lkey, local_ptr, index,
                        c_ib_info.qp[thread_index], (char *)local_ptr, remote_ptr, c_ib_info.remote_mr_data_rkey);

    /* update local caches */
    struct DataCache::Frame f = {
        .l_addr = local_ptr,
        .r_addr = remote_ptr};
    pthread_mutex_lock(&datacache.hashtable_lock);
    auto it_new = datacache.lru_list.insert(datacache.lru_list.begin(), f);
    datacache.addr_hashtable[remote_ptr] = it_new;
    pthread_mutex_unlock(&datacache.hashtable_lock);

    pthread_mutex_lock(&metadatacache.hashtable_lock);
    auto it = metadatacache.key_hashtable.find(key);
    if (it != metadatacache.key_hashtable.end()) {
        metadatacache.lru_list.erase(it->second);
    }
    MetaDataCache::EntryHeader h;
    h.ptr_next = remote_ptr;
    h.key = key;  // the bk_addr field is dummy for now
    auto it_meta = metadatacache.lru_list.insert(metadatacache.lru_list.begin(), h);
    metadatacache.key_hashtable[key] = it_meta;
    pthread_mutex_unlock(&metadatacache.hashtable_lock);
    log_info(stderr, "kv_put[key = %s]: local caches are updated.", key.c_str());

    /* poll RDMA write completion: write-through cache */
    ret = poll_completion(thread_index, c_ib_info.cq[thread_index], IBV_WC_RDMA_WRITE);
    log_info(stderr, "kv_put[key = %s]: finished RDMA write to remote addr 0x%lx.", key.c_str(), remote_ptr);

    /* invalidate all CNs' caches including itself */
    //

    return 0;
}

void *background_handler(void *arg) {
    while (true) {
        /* block wait for incoming message */
        wait_completion(c_ib_info.comp_channel, c_ib_info.bg_cq, IBV_WC_RECV, 1);

        if (strncmp(c_ib_info.ib_bg_buf, EVICT_MSG, CTL_MSG_TYPE_SIZE) == 0) {
            log_info(stderr, "background handler[eviction]: received buffer eviction request, sending my lru keys.");
            /* send my local LRU keys to the memory server */
            char *write_buf = c_ib_info.ib_bg_buf;
            pthread_mutex_lock(&metadatacache.hashtable_lock);
            for (int i = 0; i < LRU_KEY_NUM; i++) {
                string key = metadatacache.lru_list.back().key;
                metadatacache.lru_list.pop_back();
                metadatacache.key_hashtable.erase(key);
                uint32_t key_len = key.length();
                memcpy(write_buf, &key_len, sizeof(uint32_t));
                write_buf += sizeof(uint32_t);
                memcpy(write_buf, key.c_str(), key_len);
                write_buf += key_len;
            }
            pthread_mutex_unlock(&metadatacache.hashtable_lock);

            post_send(c_config_info.bg_msg_size, c_ib_info.mr_bg->lkey, (uintptr_t)c_ib_info.ib_bg_buf, 0,
                      c_ib_info.bg_qp, c_ib_info.ib_bg_buf);
            post_recv(c_config_info.bg_msg_size, c_ib_info.mr_bg->lkey, (uintptr_t)c_ib_info.ib_bg_buf,
                      c_ib_info.bg_qp, c_ib_info.ib_bg_buf);

            wait_completion(c_ib_info.comp_channel, c_ib_info.bg_cq, IBV_WC_RECV, 1);

            /* invalidation */
            log_info(stderr, "background handler[eviction]: received invalidation request, begin invalidation.");
            set<string> keys_to_inval;
            set<uint64_t> r_addrs_to_inval;
            char *read_buf = c_ib_info.ib_bg_buf;
            if (strncmp(read_buf, INVAL_MSG, CTL_MSG_TYPE_SIZE) != 0) {
                log_err("Failed to receive invalidation request.");
            }
            read_buf += CTL_MSG_TYPE_SIZE;
            uint32_t num;
            memcpy(&num, read_buf, sizeof(uint32_t));
            read_buf += sizeof(uint32_t);
            for (int i = 0; i < num; i++) {
                uint32_t key_len;
                memcpy(&key_len, read_buf, sizeof(uint32_t));
                read_buf += sizeof(uint32_t);
                string key_to_inval(read_buf, key_len);
                read_buf += key_len;
                uint64_t r_addr_to_inval;
                memcpy(&r_addr_to_inval, read_buf, sizeof(uint64_t));
                read_buf += sizeof(uint64_t);

                keys_to_inval.insert(key_to_inval);
                r_addrs_to_inval.insert(r_addr_to_inval);
            }

            pthread_mutex_lock(&metadatacache.hashtable_lock);
            for (auto it = keys_to_inval.begin(); it != keys_to_inval.end(); it++) {
                auto it_meta = metadatacache.key_hashtable.find(*it);
                if (it_meta != metadatacache.key_hashtable.end()) {
                    auto it_lru = it_meta->second;
                    metadatacache.lru_list.erase(it_lru);
                    metadatacache.key_hashtable.erase(it_meta);
                }
            }
            pthread_mutex_unlock(&metadatacache.hashtable_lock);

            pthread_mutex_lock(&datacache.hashtable_lock);
            for (auto it = r_addrs_to_inval.begin(); it != r_addrs_to_inval.end(); it++) {
                auto it_data = datacache.addr_hashtable.find(*it);
                if (it_data != datacache.addr_hashtable.end()) {
                    auto it_lru = it_data->second;
                    datacache.lru_list.erase(it_lru);
                    datacache.addr_hashtable.erase(it_data);
                }
            }
            pthread_mutex_unlock(&datacache.hashtable_lock);

            log_info(stderr, "background handler[eviction]: finished invalidating local caches, sending ack.");

            write_buf = c_ib_info.ib_bg_buf;
            bzero(write_buf, c_config_info.bg_msg_size);
            memcpy(write_buf, INVAL_COMP_MSG, CTL_MSG_TYPE_SIZE);
            post_send(CTL_MSG_TYPE_SIZE, c_ib_info.mr_bg->lkey, (uintptr_t)c_ib_info.ib_bg_buf, 0,
                      c_ib_info.bg_qp, c_ib_info.ib_bg_buf);

        } else if (strncmp(c_ib_info.ib_bg_buf, GC_MSG, CTL_MSG_TYPE_SIZE) == 0) {
            log_info(stderr, "background handler[gc]: received gc request, begin invalidation.");
            /* GC: invalidate the GCed keys */
            set<string> keys_to_inval;
            char *read_buf = c_ib_info.ib_bg_buf;
            if (strncmp(read_buf, GC_MSG, CTL_MSG_TYPE_SIZE) != 0) {
                log_err("Failed to receive gc request.");
            }
            read_buf += CTL_MSG_TYPE_SIZE;
            uint32_t num;
            memcpy(&num, read_buf, sizeof(uint32_t));
            read_buf += sizeof(uint32_t);
            for (int i = 0; i < num; i++) {
                uint32_t key_len;
                memcpy(&key_len, read_buf, sizeof(uint32_t));
                read_buf += sizeof(uint32_t);
                string key_to_inval(read_buf, key_len);
                read_buf += key_len;

                keys_to_inval.insert(key_to_inval);
            }

            pthread_mutex_lock(&metadatacache.hashtable_lock);
            for (auto it = keys_to_inval.begin(); it != keys_to_inval.end(); it++) {
                auto it_meta = metadatacache.key_hashtable.find(*it);
                if (it_meta != metadatacache.key_hashtable.end()) {
                    auto it_lru = it_meta->second;
                    metadatacache.lru_list.erase(it_lru);
                    metadatacache.key_hashtable.erase(it_meta);
                }
            }
            pthread_mutex_unlock(&metadatacache.hashtable_lock);

            log_info(stderr, "background handler[gc]: finished invalidating local caches, sending ack.");

            char *write_buf = c_ib_info.ib_bg_buf;
            bzero(write_buf, c_config_info.bg_msg_size);
            memcpy(write_buf, GC_COMP_MSG, CTL_MSG_TYPE_SIZE);
            post_send(CTL_MSG_TYPE_SIZE, c_ib_info.mr_bg->lkey, (uintptr_t)c_ib_info.ib_bg_buf, 0,
                      c_ib_info.bg_qp, c_ib_info.ib_bg_buf);
        }
    }
}

/* simulate smart contract (X stage) */
void *simulation_handler(void *arg) {
    int thread_index = *(int *)arg;
    /* set up grpc client */
    KVStableClient client(channel_ptr);

    while (true) {
        sem_wait(&rq.full);
        pthread_mutex_lock(&rq.mutex);
        struct Request proposal;
        proposal = rq.rq_queue.front();
        rq.rq_queue.pop();
        pthread_mutex_unlock(&rq.mutex);

        /* the smart contract for microbenchmarks */
        if (proposal.type == Request::Type::GET) {
            string value;
            value = kv_get(thread_index, client, proposal.key);
            log_info(logger_fp, "thread_index = #%d\trequest_type = GET\nget_key = %s\nget_value = %s\n",
                     thread_index, proposal.key.c_str(), value.c_str());
        } else if (proposal.type == Request::Type::PUT) {
            kv_put(thread_index, proposal.key, proposal.value);
            log_info(logger_fp, "thread_index = #%d\trequest_type = PUT\nput_key = %s\nput_value = %s\n",
                     thread_index, proposal.key.c_str(), proposal.value.c_str());
        }
    }
}

void run_server() {
    /* init local caches and spawn the thread pool */
    for (int offset = 0; offset < c_config_info.data_cache_size; offset += c_config_info.data_msg_size) {
        char *addr = c_ib_info.ib_data_buf + offset;
        datacache.free_addrs.push((uintptr_t)addr);
    }

    pthread_t bg_tid;
    pthread_create(&bg_tid, NULL, background_handler, NULL);
    pthread_detach(bg_tid);

    int num_threads = c_config_info.num_qps_per_server;
    pthread_t tid[num_threads];
    for (int i = 0; i < num_threads; i++) {
        pthread_create(&tid[i], NULL, simulation_handler, &i);
        pthread_detach(tid[i]);
    }

    /* accept client transaction proposals */
    /* or implement microbenchmark logics */
    struct Request req;
    req.type = Request::Type::PUT;
    req.key = "key_1111111111";
    req.value = "value_1111111111";
    pthread_mutex_lock(&rq.mutex);
    rq.rq_queue.push(req);
    pthread_mutex_unlock(&rq.mutex);
    sem_post(&rq.full);

    sleep(1);

    req.type = Request::Type::GET;
    pthread_mutex_lock(&rq.mutex);
    rq.rq_queue.push(req);
    pthread_mutex_unlock(&rq.mutex);
    sem_post(&rq.full);
}

/* return 1 on success, 0 on not found, -1 on error */
int KVStableClient::read_sstables(const string &key, string &value) {
    ClientContext context;
    GetRequest get_req;
    GetResponse get_rsp;

    get_req.set_key(key);
    Status status = stub_->read_sstables(&context, get_req, &get_rsp);
    if (!status.ok()) {
        log_err("gRPC failed with error message: %s.", status.error_message().c_str());
        return -1;
    } else {
        if (get_rsp.status() == GetResponse_Status_FOUND) {
            value = get_rsp.value();
        } else if (get_rsp.status() == GetResponse_Status_NOTFOUND) {
            log_err("Failed to find key (%s) on SSTables.", key.c_str());
            return 0;
        } else if (get_rsp.status() == GetResponse_Status_ERROR) {
            log_err("Error in reading sstables.");
            return -1;
        }
    }
    return 1;
}

int main(int argc, char *argv[]) {
    /* set config info */
    c_config_info.num_qps_per_server = 64;
    c_config_info.data_msg_size = 4 * 1024;            // 4KB each data entry
    c_config_info.data_cache_size = 50 * 1024 * 1024;  // 50MB data cache
    c_config_info.metadata_cache_size = 10000;         // 10000 cursors in metadata cache
    c_config_info.ctrl_msg_size = 1 * 1024;            // 1KB, maximum size of control message
    c_config_info.bg_msg_size = 4 * 1024;              // 4KB, maximum size of background message
    c_config_info.sock_port = 4711;                    // socket port used by memory server to init RDMA connection
    c_config_info.sock_addr = "127.0.0.1";             // ip address of memory server
    c_config_info.grpc_endpoint = "localhost:50051";   // address:port of the grpc server

    int opt;
    string configfile = "config/compute.config";
    while ((opt = getopt(argc, argv, "hc:")) != -1) {
        switch (opt) {
            case 'h':
                fprintf(stderr, "compute server usage:\n");
                fprintf(stderr, "\t-h: print this help message\n");
                fprintf(stderr, "\t-c <path_to_config_file>: path to the configuration file\n");
                exit(0);
            case 'c':
                configfile = string(optarg);
                break;
            default:
                fprintf(stderr, "Invalid option -%c\n", opt);
                exit(1);
                break;
        }
    }

    fstream fs;
    fs.open(configfile, fstream::in);
    for (string line; getline(fs, line);) {
        vector<string> tmp = split(line, "=");
        assert(tmp.size() == 2);
        std::stringstream sstream(tmp[1]);
        if (tmp[0] == "num_qps_per_server") {
            sstream >> c_config_info.num_qps_per_server;
        } else if (tmp[0] == "data_msg_size") {
            sstream >> c_config_info.data_msg_size;
        } else if (tmp[0] == "data_cache_size") {
            sstream >> c_config_info.data_cache_size;
        } else if (tmp[0] == "metadata_cache_size") {
            sstream >> c_config_info.metadata_cache_size;
        } else if (tmp[0] == "ctrl_msg_size") {
            sstream >> c_config_info.ctrl_msg_size;
        } else if (tmp[0] == "bg_msg_size") {
            sstream >> c_config_info.bg_msg_size;
        } else if (tmp[0] == "sock_port") {
            sstream >> c_config_info.sock_port;
        } else if (tmp[0] == "sock_addr") {
            c_config_info.sock_addr = tmp[1];
        } else if (tmp[0] == "grpc_endpoint") {
            c_config_info.grpc_endpoint = tmp[1];
        } else {
            fprintf(stderr, "Invalid config parameter `%s`.\n", tmp[0].c_str());
            exit(1);
        }
    }
    c_config_info.ctrl_buffer_size = c_config_info.ctrl_msg_size * c_config_info.num_qps_per_server;
    assert(c_config_info.data_cache_size % c_config_info.data_msg_size == 0);

    /* init logger */
    pthread_mutex_init(&logger_lock, NULL);
    logger_fp = fopen("compute_server.log", "w+");

    /* set up grpc channel */
    channel_ptr = grpc::CreateChannel(c_config_info.grpc_endpoint, grpc::InsecureChannelCredentials());

    /* set up RDMA connection with the memory server */
    compute_setup_ib(c_config_info, c_ib_info);

    run_server();
}