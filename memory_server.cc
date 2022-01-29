#include "memory_server.h"

#include "config.h"
#include "log.h"
#include "setup_ib.h"
#include "utils.h"

struct MConfigInfo m_config_info;
struct MemoryIBInfo m_ib_info;
RequestQueue space_manager_rq;
RequestQueue address_manager_rq;
RequestQueue bookkeeping_agent_rq;
SpaceAllocator space_allocator;
GarbageCollector gc;
unordered_map<string, struct EntryHeader> key_to_addr;
shared_ptr<grpc::Channel> channel_ptr;

void *space_manager(void *arg) {
    while (true) {
        sem_wait(&space_manager_rq.full);
        pthread_mutex_lock(&space_manager_rq.mutex);
        struct ibv_wc wc;
        wc = space_manager_rq.wc_queue.front();
        space_manager_rq.wc_queue.pop();
        pthread_mutex_unlock(&space_manager_rq.mutex);

        sem_wait(&space_allocator.full);
        pthread_mutex_lock(&space_allocator.lock);
        uint64_t free_addr = (uintptr_t)space_allocator.free_addrs.front();
        space_allocator.free_addrs.pop();
        /* trigger buffer eviction in remote memory pool */
        if (space_allocator.free_addrs.size() < EVICT_THR) {
            pthread_cond_signal(&space_allocator.cv_below);
        }
        pthread_mutex_unlock(&space_allocator.lock);

        char *msg_ptr = (char *)wc.wr_id;
        bzero(msg_ptr, m_config_info.ctrl_msg_size);
        memcpy(msg_ptr, &free_addr, sizeof(uint64_t));  // assume all servers have the same endianness, otherwise use htonll()
        msg_ptr += sizeof(uint64_t);
        uint32_t index = ((char *)free_addr - m_ib_info.ib_data_buf) / m_config_info.data_msg_size;
        memcpy(msg_ptr, &index, sizeof(uint32_t));
        msg_ptr = (char *)wc.wr_id;

        int qp_idx = m_ib_info.qp_num_to_idx[wc.qp_num];
        post_send(m_config_info.ctrl_msg_size, m_ib_info.mr_control->lkey, wc.wr_id, 0,
                  m_ib_info.qp[qp_idx], msg_ptr);
    }
}

void *eviction_manager(void *arg) {
    /* set up grpc client */
    KVStableClient client(channel_ptr);

    while (true) {
        pthread_mutex_lock(&space_allocator.lock);
        while (space_allocator.free_addrs.size() >= EVICT_THR) {
            pthread_cond_wait(&space_allocator.cv_below, &space_allocator.lock);
        }
        pthread_mutex_unlock(&space_allocator.lock);

        /* run the buffer eviction procedure */
        pthread_mutex_lock(&m_ib_info.bg_buf_lock);
        /* notify all CNs to collect LRU keys */
        for (int i = 0; i < m_config_info.num_compute_servers; i++) {
            char *bg_buf = m_ib_info.ib_bg_buf + i * m_config_info.bg_msg_size;
            bzero(bg_buf, m_config_info.bg_msg_size);
            memcpy(bg_buf, EVICT_MSG, CTL_MSG_TYPE_SIZE);
            post_send(CTL_MSG_TYPE_SIZE, m_ib_info.mr_bg->lkey, (uintptr_t)bg_buf, 0, m_ib_info.bg_qp[i], bg_buf);
            post_recv(m_config_info.bg_msg_size, m_ib_info.mr_bg->lkey, (uintptr_t)bg_buf, m_ib_info.bg_qp[i], bg_buf);
        }

        wait_completion(m_ib_info.comp_channel, m_ib_info.bg_cq, IBV_WC_RECV, m_config_info.num_compute_servers);

        set<string> lru_keys;
        for (int i = 0; i < m_config_info.num_compute_servers; i++) {
            char *read_buf = m_ib_info.ib_bg_buf + i * m_config_info.bg_msg_size;
            for (int num = 0; num < LRU_KEY_NUM; num++) {
                uint32_t key_len;
                memcpy(&key_len, read_buf, sizeof(uint32_t));
                read_buf += sizeof(uint32_t);
                string key(read_buf, key_len);
                lru_keys.insert(key);
                read_buf += key_len;
            }
        }

        /* lock the control plane mapping and set invalid flags */
        for (auto it = lru_keys.begin(); it != lru_keys.end(); it++) {
            pthread_rwlock_wrlock(&key_to_addr[*it].rwlock);
            char *ptr = (char *)key_to_addr[*it].ptr_next;
            ptr += sizeof(uint64_t) * 2;
            uint8_t invalid = 1;
            memcpy(ptr, &invalid, sizeof(uint8_t));
        }

        /* write the latest version in chain to SSTables */
        client.write_sstables(lru_keys);

        /* delete the evicted keys in control plane */
        for (auto it = lru_keys.begin(); it != lru_keys.end(); it++) {
            key_to_addr[*it].in_memory = false;
        }

        /* invalidate cached cursors and data buffers */
        char *write_buf = m_ib_info.ib_bg_buf;
        memcpy(write_buf, INVAL_MSG, CTL_MSG_TYPE_SIZE);
        write_buf += CTL_MSG_TYPE_SIZE;
        uint32_t num, key_len;
        uint64_t r_addr;
        num = lru_keys.size();
        memcpy(write_buf, &num, sizeof(uint32_t));
        write_buf += sizeof(uint32_t);
        for (auto it = lru_keys.begin(); it != lru_keys.end(); it++) {
            key_len = it->length();
            memcpy(write_buf, &key_len, sizeof(uint32_t));
            write_buf += sizeof(uint32_t);
            memcpy(write_buf, it->c_str(), key_len);
            write_buf += key_len;
            r_addr = key_to_addr[*it].ptr_next;
            memcpy(write_buf, &r_addr, sizeof(uint64_t));
            write_buf += sizeof(uint64_t);
        }
        post_send(m_config_info.bg_msg_size, m_ib_info.mr_bg->lkey, (uintptr_t)m_ib_info.ib_bg_buf, 0,
                  m_ib_info.bg_qp[0], m_ib_info.ib_bg_buf);
        post_recv(m_config_info.bg_msg_size, m_ib_info.mr_bg->lkey, (uintptr_t)m_ib_info.ib_bg_buf,
                  m_ib_info.bg_qp[0], m_ib_info.ib_bg_buf);
        for (int i = 1; i < m_config_info.num_compute_servers; i++) {
            write_buf = m_ib_info.ib_bg_buf + i * m_config_info.bg_msg_size;
            memcpy(write_buf, m_ib_info.ib_bg_buf, m_config_info.bg_msg_size);
            post_send(m_config_info.bg_msg_size, m_ib_info.mr_bg->lkey, (uintptr_t)write_buf, 0,
                      m_ib_info.bg_qp[i], write_buf);
            post_recv(m_config_info.bg_msg_size, m_ib_info.mr_bg->lkey, (uintptr_t)write_buf,
                      m_ib_info.bg_qp[i], write_buf);
        }

        wait_completion(m_ib_info.comp_channel, m_ib_info.bg_cq, IBV_WC_RECV, m_config_info.num_compute_servers);

        /* mark the evicted buffers as free */
        for (int i = 0; i < m_config_info.num_compute_servers; i++) {
            char *read_buf = m_ib_info.ib_bg_buf + i * m_config_info.bg_msg_size;
            if (strncmp(read_buf, INVAL_COMP_MSG, CTL_MSG_TYPE_SIZE) != 0) {
                log_err("Failed to receive invalidation ack.");
            }
        }
        pthread_mutex_unlock(&m_ib_info.bg_buf_lock);

        for (auto it = lru_keys.begin(); it != lru_keys.end(); it++) {
            pthread_rwlock_unlock(&key_to_addr[*it].rwlock);
            pthread_mutex_lock(&space_allocator.lock);
            char *free_addr = (char *)key_to_addr[*it].ptr_next;
            space_allocator.free_addrs.push(free_addr);
            pthread_mutex_unlock(&space_allocator.lock);
            sem_post(&space_allocator.full);
        }
    }
}

void *gc_manager(void *arg) {
    while (true) {
        pthread_mutex_lock(&gc.lock);
        while (gc.to_gc_queue.size() < GC_THR) {
            pthread_cond_wait(&gc.cv_above, &gc.lock);
        }

        set<string> gc_keys;
        set<char *> gc_addrs;
        for (int i = 0; i < GC_THR; i++) {
            gc_keys.insert(gc.to_gc_queue.front().key);
            gc_addrs.insert(gc.to_gc_queue.front().addr);
            gc.to_gc_queue.pop();
        }

        pthread_mutex_unlock(&gc.lock);

        /* invalidate keys that corresponds to the recycled buffer */
        pthread_mutex_lock(&m_ib_info.bg_buf_lock);
        char *write_buf = m_ib_info.ib_bg_buf;
        memcpy(write_buf, GC_MSG, CTL_MSG_TYPE_SIZE);
        write_buf += CTL_MSG_TYPE_SIZE;
        uint32_t num, key_len;
        num = gc_keys.size();
        memcpy(write_buf, &num, sizeof(uint32_t));
        write_buf += sizeof(uint32_t);
        for (auto it = gc_keys.begin(); it != gc_keys.end(); it++) {
            key_len = it->length();
            memcpy(write_buf, &key_len, sizeof(uint32_t));
            write_buf += sizeof(uint32_t);
            memcpy(write_buf, it->c_str(), key_len);
            write_buf += key_len;
        }
        post_send(m_config_info.bg_msg_size, m_ib_info.mr_bg->lkey, (uintptr_t)m_ib_info.ib_bg_buf, 0,
                  m_ib_info.bg_qp[0], m_ib_info.ib_bg_buf);
        post_recv(m_config_info.bg_msg_size, m_ib_info.mr_bg->lkey, (uintptr_t)m_ib_info.ib_bg_buf,
                  m_ib_info.bg_qp[0], m_ib_info.ib_bg_buf);
        for (int i = 1; i < m_config_info.num_compute_servers; i++) {
            write_buf = m_ib_info.ib_bg_buf + i * m_config_info.bg_msg_size;
            memcpy(write_buf, m_ib_info.ib_bg_buf, m_config_info.bg_msg_size);
            post_send(m_config_info.bg_msg_size, m_ib_info.mr_bg->lkey, (uintptr_t)write_buf, 0,
                      m_ib_info.bg_qp[i], write_buf);
            post_recv(m_config_info.bg_msg_size, m_ib_info.mr_bg->lkey, (uintptr_t)write_buf,
                      m_ib_info.bg_qp[i], write_buf);
        }

        wait_completion(m_ib_info.comp_channel, m_ib_info.bg_cq, IBV_WC_RECV, m_config_info.num_compute_servers);

        /* mark the buffers as free */
        for (int i = 0; i < m_config_info.num_compute_servers; i++) {
            char *read_buf = m_ib_info.ib_bg_buf + i * m_config_info.bg_msg_size;
            if (strncmp(read_buf, GC_COMP_MSG, CTL_MSG_TYPE_SIZE) != 0) {
                log_err("Failed to receive garbage collection ack.");
            }
        }
        pthread_mutex_unlock(&m_ib_info.bg_buf_lock);

        for (auto it = gc_addrs.begin(); it != gc_addrs.end(); it++) {
            pthread_mutex_lock(&space_allocator.lock);
            space_allocator.free_addrs.push(*it);
            pthread_mutex_unlock(&space_allocator.lock);
            sem_post(&space_allocator.full);
        }
    }
}

void *address_manager(void *arg) {
    while (true) {
        sem_wait(&address_manager_rq.full);
        pthread_mutex_lock(&address_manager_rq.mutex);
        struct ibv_wc wc;
        wc = address_manager_rq.wc_queue.front();
        address_manager_rq.wc_queue.pop();
        pthread_mutex_unlock(&address_manager_rq.mutex);

        char *msg_ptr = (char *)wc.wr_id;
        msg_ptr += CTL_MSG_TYPE_SIZE;
        uint32_t key_len;
        memcpy(&key_len, msg_ptr, sizeof(uint32_t));
        msg_ptr += sizeof(uint32_t);
        string key(msg_ptr, key_len);

        msg_ptr = (char *)wc.wr_id;
        bzero(msg_ptr, m_config_info.ctrl_msg_size);
        if (pthread_rwlock_tryrdlock(&key_to_addr[key].rwlock) == 0) {
            if (key_to_addr[key].in_memory) {
                memcpy(msg_ptr, FOUND_MSG, CTL_MSG_TYPE_SIZE);
                msg_ptr += CTL_MSG_TYPE_SIZE;
                memcpy(msg_ptr, &key_to_addr[key].ptr_next, sizeof(uint64_t));
                msg_ptr += sizeof(uint64_t);
                memcpy(msg_ptr, &key_to_addr[key].bk_addr, sizeof(uint64_t));
                // TODO: add bookkeeping info for this header, or use the GC version approach
            } else {
                memcpy(msg_ptr, NOT_FOUND_MSG, CTL_MSG_TYPE_SIZE);
            }
            pthread_rwlock_unlock(&key_to_addr[key].rwlock);
        } else {
            /* the key is under eviction now */
            memcpy(msg_ptr, BACKOFF_MSG, CTL_MSG_TYPE_SIZE);
        }

        msg_ptr = (char *)wc.wr_id;
        int qp_idx = m_ib_info.qp_num_to_idx[wc.qp_num];
        post_send(m_config_info.ctrl_msg_size, m_ib_info.mr_control->lkey, wc.wr_id, 0,
                  m_ib_info.qp[qp_idx], msg_ptr);
    }
}

void *bookkeeping_agent(void *arg) {
    while (true) {
        sem_wait(&bookkeeping_agent_rq.full);
        pthread_mutex_lock(&bookkeeping_agent_rq.mutex);
        struct ibv_wc wc;
        wc = bookkeeping_agent_rq.wc_queue.front();
        bookkeeping_agent_rq.wc_queue.pop();
        pthread_mutex_unlock(&bookkeeping_agent_rq.mutex);

        uint32_t imm_data = ntohl(wc.imm_data);
        uint64_t offset = imm_data * m_config_info.data_msg_size;
        uint32_t key_len;
        char *data_ptr = m_ib_info.ib_data_buf + offset + sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint8_t);
        memcpy(&key_len, data_ptr, sizeof(uint32_t));
        data_ptr += sizeof(uint32_t);
        string key(data_ptr, key_len);

        char *msg_ptr;
        auto it = key_to_addr.find(key);
        if (it == key_to_addr.end()) {
            /* pre-populate the key value store, no contention */
            key_to_addr[key].ptr_next = (uintptr_t)(m_ib_info.ib_data_buf + offset);
            key_to_addr[key].bk_addr = NULL;
            key_to_addr[key].in_memory = true;
            pthread_rwlock_init(&key_to_addr[key].rwlock, NULL);

            msg_ptr = (char *)wc.wr_id;
            bzero(msg_ptr, m_config_info.ctrl_msg_size);
            memcpy(msg_ptr, COMMITTED_MSG, CTL_MSG_TYPE_SIZE);  // return msg to the compute server
        } else {
            pthread_rwlock_wrlock(&it->second.rwlock);
            if (it->second.in_memory) {
                char *old_addr = (char *)it->second.ptr_next;
                uint64_t new_addr = (uintptr_t)(m_ib_info.ib_data_buf + offset);
                char *flag_addr = old_addr + sizeof(uint64_t) + sizeof(uint64_t);
                *flag_addr = 1;                                 // set the invalid bit
                memcpy(old_addr, &new_addr, sizeof(uint64_t));  // link the latest buffer to the chain
                *flag_addr = 0;
                it->second.ptr_next = new_addr;

                /* retire old buffers in the background */
                GarbageCollector::GCItem item;
                item.addr = old_addr;
                item.key = key;
                pthread_mutex_lock(&gc.lock);
                gc.to_gc_queue.push(item);
                if (gc.to_gc_queue.size() >= GC_THR) {
                    pthread_cond_signal(&gc.cv_above);
                }

                pthread_mutex_unlock(&gc.lock);
            } else {
                it->second.in_memory = true;
                uint64_t new_addr = (uintptr_t)(m_ib_info.ib_data_buf + offset);
                it->second.ptr_next = new_addr;
            }

            pthread_rwlock_unlock(&it->second.rwlock);

            msg_ptr = (char *)wc.wr_id;
            bzero(msg_ptr, m_config_info.ctrl_msg_size);
            memcpy(msg_ptr, COMMITTED_MSG, CTL_MSG_TYPE_SIZE);
            // return msg to the compute server: referencing CNs of second latest version when multiple compute servers
        }
        int qp_idx = m_ib_info.qp_num_to_idx[wc.qp_num];
        post_send(m_config_info.ctrl_msg_size, m_ib_info.mr_control->lkey, wc.wr_id, 0,
                  m_ib_info.qp[qp_idx], msg_ptr);
    }
}

int run_server() {
    /* spawn agent threads: each agent is single threaded for now. */
    for (int offset = 0; offset < m_config_info.data_slab_size; offset += m_config_info.data_msg_size) {
        space_allocator.free_addrs.push(&m_ib_info.ib_data_buf[offset]);
    }
    sem_init(&space_allocator.full, 0, space_allocator.free_addrs.size());

    pthread_t tid_bg;
    pthread_create(&tid_bg, NULL, eviction_manager, NULL);
    pthread_detach(tid_bg);
    pthread_t tid_gc;
    pthread_create(&tid_gc, NULL, gc_manager, NULL);
    pthread_detach(tid_gc);

    pthread_t tid[3];
    pthread_create(&tid[0], NULL, space_manager, NULL);
    pthread_detach(tid[0]);
    pthread_create(&tid[1], NULL, address_manager, NULL);
    pthread_detach(tid[1]);
    pthread_create(&tid[2], NULL, bookkeeping_agent, NULL);
    pthread_detach(tid[2]);

    /* main thread polls cq to detect incoming message */
    int num_wc = 20;
    struct ibv_wc *wc;
    wc = (struct ibv_wc *)calloc(num_wc, sizeof(struct ibv_wc));
    while (true) {
        int n = ibv_poll_cq(m_ib_info.cq, num_wc, wc);
        if (n < 0) {
            log_err("main thread: Failed to poll cq.");
        }

        for (int i = 0; i < n; i++) {
            if (wc[i].status != IBV_WC_SUCCESS) {
                if (wc[i].opcode == IBV_WC_SEND) {
                    log_err("main thread: send failed with status %s", ibv_wc_status_str(wc[i].status));
                } else {
                    log_err("main thread: recv failed with status %s", ibv_wc_status_str(wc[i].status));
                }
                continue;
            }

            if (wc[i].opcode == IBV_WC_SEND) {
                char *msg_ptr = (char *)wc[i].wr_id;
                bzero(msg_ptr, m_config_info.ctrl_msg_size);
                post_srq_recv(m_config_info.ctrl_msg_size, m_ib_info.mr_control->lkey, wc[i].wr_id,
                              m_ib_info.srq, msg_ptr);
            } else {
                RequestQueue *rq;
                if (wc[i].opcode == IBV_WC_RECV) {
                    /* received a message via send */
                    char *msg_ptr = (char *)wc[i].wr_id;
                    if (strncmp(msg_ptr, ALLOC_MSG, CTL_MSG_TYPE_SIZE) == 0) {
                        rq = &space_manager_rq;
                    } else if (strncmp(msg_ptr, ADDR_MSG, CTL_MSG_TYPE_SIZE) == 0) {
                        rq = &address_manager_rq;
                    }
                } else if (wc[i].opcode == IBV_WC_RECV_RDMA_WITH_IMM) {
                    /* received a message via RDMA write with immediate (sg_list is unused) */
                    rq = &bookkeeping_agent_rq;
                }

                pthread_mutex_lock(&rq->mutex);
                rq->wc_queue.push(wc[i]);
                pthread_mutex_unlock(&rq->mutex);
                sem_post(&rq->full);
            }
        }
    }
}

void KVStableClient::write_sstables(const set<string> &keys) {
    ClientContext context;
    EvictedBuffers ev;
    EvictionResponse rsp;

    for (auto it = keys.begin(); it != keys.end(); it++) {
        string key(*it);
        char *ptr = (char *)key_to_addr[*it].ptr_next;
        string value(ptr, m_config_info.data_msg_size);
        (*ev.mutable_eviction())[key] = value;
    }

    Status status = stub_->write_sstables(&context, ev, &rsp);
    if (!status.ok()) {
        log_err("gRPC failed with error message: %s.", status.error_message().c_str());
    }
}

int main(int argc, char *argv[]) {
    /* set config info */
    m_config_info.num_compute_servers = 1;
    m_config_info.num_qps_per_server = 64;
    m_config_info.data_msg_size = 4 * 1024;                  // 4KB each data entry
    m_config_info.data_slab_size = 32 * 1024 * 1024 * 1024;  // 32GB per slab
    m_config_info.ctrl_msg_size = 1 * 1024;                  // 1KB, maximum size of control message
    m_config_info.bg_msg_size = 4 * 1024;                    // 4KB, maximum size of background message
    m_config_info.sock_port = 4711;                          // socket port used by memory server to init RDMA connection
    m_config_info.grpc_endpoint = "localhost:50051";         // address:port of the grpc server

    int opt;
    string configfile = "config/memory.config";
    while ((opt = getopt(argc, argv, "hc:")) != -1) {
        switch (opt) {
            case 'h':
                fprintf(stderr, "memory server usage:\n");
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
        if (tmp[0] == "num_compute_servers") {
            sstream >> m_config_info.num_compute_servers;
        } else if (tmp[0] == "num_qps_per_server") {
            sstream >> m_config_info.num_qps_per_server;
        } else if (tmp[0] == "data_msg_size") {
            sstream >> m_config_info.data_msg_size;
        } else if (tmp[0] == "data_slab_size") {
            sstream >> m_config_info.data_slab_size;
        } else if (tmp[0] == "ctrl_msg_size") {
            sstream >> m_config_info.ctrl_msg_size;
        } else if (tmp[0] == "bg_msg_size") {
            sstream >> m_config_info.bg_msg_size;
        } else if (tmp[0] == "sock_port") {
            sstream >> m_config_info.sock_port;
        } else if (tmp[0] == "grpc_endpoint") {
            m_config_info.grpc_endpoint = tmp[1];
        } else {
            fprintf(stderr, "Invalid config parameter `%s`.\n", tmp[0].c_str());
            exit(1);
        }
    }
    m_config_info.ctrl_buffer_size = m_config_info.ctrl_msg_size * m_config_info.num_qps_per_server * m_config_info.num_compute_servers;
    m_config_info.bg_buffer_size = m_config_info.bg_msg_size * m_config_info.num_compute_servers;

    /* set up grpc channel */
    channel_ptr = grpc::CreateChannel(m_config_info.grpc_endpoint, grpc::InsecureChannelCredentials());

    /* set up RDMA connection with compute servers */
    memory_setup_ib();

    run_server();
}