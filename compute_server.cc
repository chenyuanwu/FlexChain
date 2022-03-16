#include "compute_server.h"

#include <assert.h>
#include <signal.h>

#include "benchmark.h"
#include "config.h"
#include "leveldb/db.h"
#include "log.h"
#include "setup_ib.h"
#include "utils.h"

struct CConfigInfo c_config_info;
struct ComputeIBInfo c_ib_info;
CompletionSet C;
ValidationQueue vq;
BlockQueue bq;
RequestQueue rq;
DataCache datacache;
MetaDataCache metadatacache;
shared_ptr<grpc::Channel> storage_channel_ptr;
shared_ptr<grpc::Channel> orderer_channel_ptr;
pthread_mutex_t logger_lock;
FILE *logger_fp;
volatile int end_flag = 0;
volatile int start_flag = 0;
atomic<long> total_ops = 0;

atomic<long> cache_hit = 0;
atomic<long> sst_count = 0;
atomic<long> cache_total = 0;
atomic<long> abort_count = 0;

leveldb::DB *db;
leveldb::Options options;

/* read from the disaggregated key value store */
string kv_get(struct ThreadContext &ctx, KVStableClient &client, const string &key) {
    cache_total++;
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
        log_debug(stderr, "kv_get[thread_index = %d, key = %s]: remote address 0x%lx is found in local metadata cache.",
                  ctx.thread_index, key.c_str(), ptr_next);
    }
    pthread_mutex_unlock(&metadatacache.hashtable_lock);

    if (ptr_next == 0) {
        /* ask the control plane */
        char *ctrl_buf = c_ib_info.ib_control_buf + ctx.thread_index * c_config_info.ctrl_msg_size;
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
                  ctx.m_qp, ctrl_buf, to_string(ctx.thread_index), to_string(__LINE__));
        post_recv(c_config_info.ctrl_msg_size, c_ib_info.mr_control->lkey, (uintptr_t)ctrl_buf,
                  ctx.m_qp, ctrl_buf, to_string(ctx.thread_index), to_string(__LINE__));

        int ret = poll_completion(ctx.thread_index, ctx.m_cq, IBV_WC_RECV, __LINE__);

        char *read_buf = ctrl_buf;
        if (strncmp(read_buf, BACKOFF_MSG, CTL_MSG_TYPE_SIZE) == 0) {
            /* in evection now */
            log_debug(stderr, "kv_get[thread_index = %d, key = %s]: remote buffer of this key is in eviction, retry.",
                      ctx.thread_index, key.c_str());
            usleep(2000);
            return kv_get(ctx, client, key);
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

            log_debug(stderr, "kv_get[thread_index = %d, key = %s]: remote address 0x%lx is found by contacting the control plane.",
                      ctx.thread_index, key.c_str(), ptr_next);
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
            cache_hit++;
            /* the buffer is cached locally */
            log_debug(stderr, "kv_get[thread_index = %d, key = %s]: found in local data cache (raddr = 0x%lx, laddr = 0x%lx).",
                      ctx.thread_index, key.c_str(), ptr_next, local_ptr);
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
            post_read(c_config_info.data_msg_size, c_ib_info.mr_data->lkey, local_ptr, ctx.m_qp,
                      (char *)local_ptr, ptr_next, c_ib_info.remote_mr_data_rkey, to_string(ctx.thread_index), to_string(__LINE__));
            // chain walk is not needed for M-C topology
            int ret = poll_completion(ctx.thread_index, ctx.m_cq, IBV_WC_RDMA_READ, __LINE__);
            log_debug(stderr, "kv_get[thread_index = %d, key = %s]: finished RDMA read from remote memory pool (raddr = 0x%lx).",
                      ctx.thread_index, key.c_str(), ptr_next);

            uint8_t invalid = 0;
            char *val_ptr = (char *)local_ptr;
            val_ptr += sizeof(uint64_t) * 2;
            memcpy(&invalid, val_ptr, sizeof(uint8_t));

            if (invalid) {
                /* in eviction now */
                log_debug(stderr, "kv_get[thread_index = %d, key = %s]: invalid buffer (raddr = 0x%lx) due to eviction, discard and retry.",
                          ctx.thread_index, key.c_str(), ptr_next);
                datacache.free_addrs.push(local_ptr);
                usleep(500000);
                return kv_get(ctx, client, key);
            } else {
                // uint64_t ptr;
                // memcpy(&ptr, (char *)local_ptr, sizeof(uint64_t));
                // assert(ptr == 0); // not true under contention

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
        sst_count++;
        /* search in SSTables on the storage server */
        log_debug(stderr, "kv_get[thread_index = %d, key = %s]: read from sstables.", ctx.thread_index, key.c_str());
        string value;
        int ret = client.read_sstables(key, value);

        unsigned long offset = sizeof(uint64_t) * 2 + sizeof(uint8_t) + sizeof(uint32_t) + key.length();
        value = value.substr(offset);
        return value;
    }
}

/* put to the disaggregated key value store */
int kv_put(struct ThreadContext &ctx, const string &key, const string &value) {
    /* allocate a remote buffer */
    char *ctrl_buf = c_ib_info.ib_control_buf + ctx.thread_index * c_config_info.ctrl_msg_size;
    bzero(ctrl_buf, c_config_info.ctrl_msg_size);
    memcpy(ctrl_buf, ALLOC_MSG, CTL_MSG_TYPE_SIZE);

    post_send(c_config_info.ctrl_msg_size, c_ib_info.mr_control->lkey, (uintptr_t)ctrl_buf, 0,
              ctx.m_qp, ctrl_buf, to_string(ctx.thread_index), to_string(__LINE__));
    post_recv(c_config_info.ctrl_msg_size, c_ib_info.mr_control->lkey, (uintptr_t)ctrl_buf,
              ctx.m_qp, ctrl_buf, to_string(ctx.thread_index), to_string(__LINE__));

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
    int ret = poll_completion(ctx.thread_index, ctx.m_cq, IBV_WC_RECV, __LINE__);
    uint64_t remote_ptr;
    char *read_ptr = ctrl_buf;
    memcpy(&remote_ptr, read_ptr, sizeof(uint64_t));
    read_ptr += sizeof(uint64_t);
    uint32_t index;
    memcpy(&index, read_ptr, sizeof(uint32_t));
    log_debug(stderr, "kv_put[thread_index = %d, key = %s]: remote addr 0x%lx is allocated.",
              ctx.thread_index, key.c_str(), remote_ptr);

    post_write_with_imm(c_config_info.data_msg_size, c_ib_info.mr_data->lkey, local_ptr, index, ctx.m_qp,
                        (char *)local_ptr, remote_ptr, c_ib_info.remote_mr_data_rkey, to_string(ctx.thread_index), to_string(__LINE__));
    post_recv(c_config_info.ctrl_msg_size, c_ib_info.mr_control->lkey, (uintptr_t)ctrl_buf,
              ctx.m_qp, ctrl_buf, to_string(ctx.thread_index), to_string(__LINE__));

    /* poll RDMA write completion: write-through cache */
    ret = poll_completion(ctx.thread_index, ctx.m_cq, IBV_WC_RECV, __LINE__);
    if (strncmp(ctrl_buf, COMMITTED_MSG, CTL_MSG_TYPE_SIZE) != 0) {
        log_err("thread[%d]: failed to receive message committed ack.", ctx.thread_index);
        return -1;
    }

    log_debug(stderr, "kv_put[thread_index = %d, key = %s]: finished RDMA write to remote addr 0x%lx, committed.",
              ctx.thread_index, key.c_str(), remote_ptr);

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
    log_debug(stderr, "kv_put[thread_index = %d, key = %s]: local caches are updated.", ctx.thread_index, key.c_str());

    /* invalidate all CNs' caches including itself */
    //

    return 0;
}

void *background_handler(void *arg) {
    while (true) {
        /* block wait for incoming message */
        log_debug(stderr, "background handler: listening for background requests.");
        wait_completion(c_ib_info.comp_channel, c_ib_info.bg_cq, IBV_WC_RECV, 1, __LINE__);

        if (strncmp(c_ib_info.ib_bg_buf, EVICT_MSG, CTL_MSG_TYPE_SIZE) == 0) {
            log_debug(stderr, "background handler[eviction]: received buffer eviction request, sending my lru keys.");
            /* send my local LRU keys to the memory server */
            char *write_buf = c_ib_info.ib_bg_buf;
            pthread_mutex_lock(&metadatacache.hashtable_lock);

            uint32_t num;
            num = (metadatacache.lru_list.size() >= LRU_KEY_NUM) ? LRU_KEY_NUM : metadatacache.lru_list.size();
            memcpy(write_buf, &num, sizeof(uint32_t));
            write_buf += sizeof(uint32_t);

            for (int i = 0; i < num; i++) {
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
                      c_ib_info.bg_qp, c_ib_info.ib_bg_buf, __func__, to_string(__LINE__));
            post_recv(c_config_info.bg_msg_size, c_ib_info.mr_bg->lkey, (uintptr_t)c_ib_info.ib_bg_buf,
                      c_ib_info.bg_qp, c_ib_info.ib_bg_buf, __func__, to_string(__LINE__));

            wait_completion(c_ib_info.comp_channel, c_ib_info.bg_cq, IBV_WC_RECV, 1, __LINE__);

            /* invalidation */
            log_debug(stderr, "background handler[eviction]: received invalidation request, begin invalidation.");
            set<string> keys_to_inval;
            set<uint64_t> r_addrs_to_inval;
            char *read_buf = c_ib_info.ib_bg_buf;
            if (strncmp(read_buf, INVAL_MSG, CTL_MSG_TYPE_SIZE) != 0) {
                log_err("Failed to receive invalidation request.");
            }
            read_buf += CTL_MSG_TYPE_SIZE;
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
                    uint64_t l_addr = it_lru->l_addr;
                    datacache.free_addrs.push(l_addr);
                    datacache.lru_list.erase(it_lru);
                    datacache.addr_hashtable.erase(it_data);
                }
            }
            pthread_mutex_unlock(&datacache.hashtable_lock);

            log_debug(stderr, "background handler[eviction]: finished invalidating local caches, sending ack.");

            write_buf = c_ib_info.ib_bg_buf;
            bzero(write_buf, c_config_info.bg_msg_size);
            memcpy(write_buf, INVAL_COMP_MSG, CTL_MSG_TYPE_SIZE);
            post_send(CTL_MSG_TYPE_SIZE, c_ib_info.mr_bg->lkey, (uintptr_t)c_ib_info.ib_bg_buf, 0,
                      c_ib_info.bg_qp, c_ib_info.ib_bg_buf, __func__, to_string(__LINE__));
            post_recv(c_config_info.bg_msg_size, c_ib_info.mr_bg->lkey, (uintptr_t)c_ib_info.ib_bg_buf,
                      c_ib_info.bg_qp, c_ib_info.ib_bg_buf, __func__, to_string(__LINE__));
        } else if (strncmp(c_ib_info.ib_bg_buf, GC_MSG, CTL_MSG_TYPE_SIZE) == 0) {
            log_debug(stderr, "background handler[gc]: received gc request, begin invalidation.");
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

            log_debug(stderr, "background handler[gc]: finished invalidating local caches, sending ack.");

            char *write_buf = c_ib_info.ib_bg_buf;
            bzero(write_buf, c_config_info.bg_msg_size);
            memcpy(write_buf, GC_COMP_MSG, CTL_MSG_TYPE_SIZE);
            post_send(CTL_MSG_TYPE_SIZE, c_ib_info.mr_bg->lkey, (uintptr_t)c_ib_info.ib_bg_buf, 0,
                      c_ib_info.bg_qp, c_ib_info.ib_bg_buf, __func__, to_string(__LINE__));
            post_recv(c_config_info.bg_msg_size, c_ib_info.mr_bg->lkey, (uintptr_t)c_ib_info.ib_bg_buf,
                      c_ib_info.bg_qp, c_ib_info.ib_bg_buf, __func__, to_string(__LINE__));
        }
    }
}

/* wrappers for the kv interface, to be used within the smart contracts */
string s_kv_get(struct ThreadContext &ctx, KVStableClient &client, const string &key, Endorsement &endorsement) {
    string value;
    value = kv_get(ctx, client, key);
    // leveldb::Status s = db->Get(leveldb::ReadOptions(), key, &value);

    uint64_t read_version_blockid = 0;
    uint64_t read_version_transid = 0;
    memcpy(&read_version_blockid, value.c_str(), sizeof(uint64_t));
    memcpy(&read_version_transid, value.c_str() + sizeof(uint64_t), sizeof(uint64_t));

    ReadItem *read_item = endorsement.add_read_set();
    read_item->set_read_key(key);
    read_item->set_block_seq_num(read_version_blockid);
    read_item->set_trans_seq_num(read_version_transid);

    /* the value returned to smart contracts should not contain version numbers */
    value.erase(0, 128);

    log_debug(logger_fp,
              "s_kv_get[thread_index = %d, key = %s]:\n"
              "version [block_id = %ld, trans_id = %ld] is stored in read set.\n",
              ctx.thread_index, key.c_str(), read_item->block_seq_num(), read_item->trans_seq_num());

    return value;
}

int s_kv_put(const string &key, const string &value, Endorsement &endorsement) {
    WriteItem *write_item = endorsement.add_write_set();
    write_item->set_write_key(key);
    write_item->set_write_value(value);

    return 0;
}

/* simulate smart contract (X stage) */
void *simulation_handler(void *arg) {
    struct ThreadContext ctx = *(struct ThreadContext *)arg;
    assert(ctx.m_cq != NULL);
    assert(ctx.m_qp != NULL);
    /* set up grpc client for storage server */
    KVStableClient storage_client(storage_channel_ptr);
    /* set up grpc client for ordering service */
    unique_ptr<ConsensusComm::Stub> orderer_stub(ConsensusComm::NewStub(orderer_channel_ptr));

    char *buf = (char *)malloc(c_config_info.data_msg_size);
    long local_ops = 0;
    while (!end_flag) {
        sem_wait(&rq.full);
        pthread_mutex_lock(&rq.mutex);
        struct Request proposal;
        proposal = rq.rq_queue.front();
        rq.rq_queue.pop();
        pthread_mutex_unlock(&rq.mutex);

        ClientContext context;
        Endorsement endorsement;
        google::protobuf::Empty rsp;

        /* the smart contract */
        if (proposal.type == Request::Type::GET) {
            // string value;
            // value = kv_get(ctx, storage_client, proposal.key);
            // storage_client.read_sstables(proposal.key, value);
            // leveldb::Status s = db->Get(leveldb::ReadOptions(), proposal.key, &value);
            // log_debug(logger_fp, "thread_index = #%d\trequest_type = GET\nget_key = %s\nget_value = %s\nget_size = %ld\n",
            //           ctx.thread_index, proposal.key.c_str(), value.c_str(), value.size());
            // local_ops++;

            s_kv_get(ctx, storage_client, proposal.key, endorsement);
        } else if (proposal.type == Request::Type::PUT) {
            // int ret = kv_put(ctx, proposal.key, proposal.value);
            // int ret = storage_client.write_sstables(proposal.key, proposal.value);
            // bzero(buf, 1024 * 10);
            // strcpy(buf, proposal.value.c_str());
            // string value(buf, 1024 * 10);

            // leveldb::Status s = db->Put(leveldb::WriteOptions(), proposal.key, value);
            // int ret = 0;
            // if (!s.ok()) {
            //     ret = 1;
            // }
            // if (!ret) {
            //     log_debug(logger_fp, "thread_index = #%d\trequest_type = PUT\nput_key = %s\nput_value = %s\n",
            //               ctx.thread_index, proposal.key.c_str(), proposal.value.c_str());
            //     if (!proposal.is_prep) {
            //         local_ops++;
            //     }
            // } else {
            //     log_debug(logger_fp, "thread_index = #%d\trequest_type = PUT\nput_key = %s\noperation failed...\n",
            //               ctx.thread_index, proposal.key.c_str());
            // }

            bzero(buf, c_config_info.data_msg_size);
            strcpy(buf, proposal.value.c_str());
            size_t meta_data_size = sizeof(uint64_t) * 2 + sizeof(uint8_t) + sizeof(uint32_t) + proposal.key.length() + 2 * sizeof(uint64_t);
            // size_t meta_data_size = 0;
            proposal.value.assign(buf, c_config_info.data_msg_size - meta_data_size);
            /* if it is prepopulation */
            if (proposal.is_prep) {
                uint64_t curr_version_blockid = 0;
                uint64_t curr_version_transid = 0;
                char *ver = (char *)malloc(2 * sizeof(uint64_t));
                bzero(ver, 2 * sizeof(uint64_t));
                memcpy(ver, &curr_version_blockid, sizeof(uint64_t));
                memcpy(ver + sizeof(uint64_t), &curr_version_transid, sizeof(uint64_t));
                string value(ver, 2 * sizeof(uint64_t));
                value += proposal.value;

                int ret = kv_put(ctx, proposal.key, value);
                // leveldb::Status s = db->Put(leveldb::WriteOptions(), proposal.key, value);
                // int ret = 0;
                // if (!s.ok()) {
                //     ret = 1;
                // }
                if (!ret) {
                    log_debug(logger_fp, "thread_index = #%d\trequest_type = PUT\nput_key = %s\nput_value = %s\n",
                              ctx.thread_index, proposal.key.c_str(), proposal.value.c_str());
                } else {
                    log_debug(logger_fp, "thread_index = #%d\trequest_type = PUT\nput_key = %s\noperation failed...\n",
                              ctx.thread_index, proposal.key.c_str());
                }
                continue;
            } else {
                s_kv_put(proposal.key, proposal.value, endorsement);
            }
        }

        /* send the generated endorsement to the client/orderer */
        Status status = orderer_stub->send_to_leader(&context, endorsement, &rsp);
        if (!status.ok()) {
            log_err("gRPC failed with error message: %s.", status.error_message().c_str());
        }
    }
    // total_ops += local_ops;
    free(buf);
    return NULL;
}

/* validate and commit transactions (V stage) */
bool validate_transaction(struct ThreadContext &ctx, KVStableClient &storage_client, 
                          uint64_t block_id, uint64_t trans_id, const Endorsement &transaction) {
    // log_debug(stderr, "******validating transaction[block_id = %ld, trans_id = %ld, thread_id = %d]******",
    //           block_id, trans_id, ctx.thread_index);
    usleep(5000);
    bool is_valid = true;

    for (int read_id = 0; read_id < transaction.read_set_size(); read_id++) {
        uint64_t curr_version_blockid = 0;
        uint64_t curr_version_transid = 0;

        string value;
        value = kv_get(ctx, storage_client, transaction.read_set(read_id).read_key());
        // leveldb::Status s = db->Get(leveldb::ReadOptions(), transaction.read_set(read_id).read_key(), &value);
        memcpy(&curr_version_blockid, value.c_str(), sizeof(uint64_t));
        memcpy(&curr_version_transid, value.c_str() + sizeof(uint64_t), sizeof(uint64_t));

        // log_debug(logger_fp,
        //           "read_key = %s\nstored_read_version = [block_id = %ld, trans_id = %ld]\n"
        //           "current_key_version = [block_id = %ld, trans_id = %ld]",
        //           transaction.read_set(read_id).read_key().c_str(),
        //           transaction.read_set(read_id).block_seq_num(),
        //           transaction.read_set(read_id).trans_seq_num(),
        //           curr_version_blockid, curr_version_transid);

        if (curr_version_blockid != transaction.read_set(read_id).block_seq_num() ||
            curr_version_transid != transaction.read_set(read_id).trans_seq_num()) {
            is_valid = false;
            break;
        }
    }

    if (is_valid) {
        uint64_t curr_version_blockid = block_id;
        char *ver = (char *)malloc(2 * sizeof(uint64_t));
        for (int write_id = 0; write_id < transaction.write_set_size(); write_id++) {
            uint64_t curr_version_transid = trans_id;

            bzero(ver, 2 * sizeof(uint64_t));
            memcpy(ver, &curr_version_blockid, sizeof(uint64_t));
            memcpy(ver + sizeof(uint64_t), &curr_version_transid, sizeof(uint64_t));
            string value(ver, 2 * sizeof(uint64_t));
            value += transaction.write_set(write_id).write_value();

            int ret = kv_put(ctx, transaction.write_set(write_id).write_key(), value);
            // leveldb::Status s = db->Put(leveldb::WriteOptions(), transaction.write_set(write_id).write_key(), value);

            // log_debug(logger_fp, "write_key = %s\nwrite_value = %s",
            //           transaction.write_set(write_id).write_key().c_str(),
            //           transaction.write_set(write_id).write_value().c_str());
        }
        free(ver);
        // log_debug(logger_fp, "transaction is committed.\n");
        total_ops++;
    } else {
        abort_count++;
        // log_debug(logger_fp, "transaction is aborted: found stale read version.\n");
    }
    return is_valid;
}

void *validation_handler(void *arg) {
    struct ThreadContext ctx = *(struct ThreadContext *)arg;
    /* set up grpc client for storage server */
    KVStableClient storage_client(storage_channel_ptr);
    string serialised_block;

    while (!end_flag) {
        sem_wait(&bq.full);
        pthread_mutex_lock(&bq.mutex);
        Block block = bq.bq_queue.front();
        bq.bq_queue.pop();
        pthread_mutex_unlock(&bq.mutex);

        for (int trans_id = 0; trans_id < block.transactions_size(); trans_id++) {
            validate_transaction(ctx, storage_client, block.block_id(), trans_id, block.transactions(trans_id));
        }

        /* store the block with its bit mask in remote block store */
        if (!block.SerializeToString(&serialised_block)) {
            log_err("validator: failed to serialize block.");
        }
        storage_client.write_blocks(serialised_block);
    }

    return NULL;
}

void *parallel_validation_worker(void *arg) {
    struct ThreadContext ctx = *(struct ThreadContext *)arg;
    /* set up grpc client for storage server */
    KVStableClient storage_client(storage_channel_ptr);

    while (!end_flag) {
        sem_wait(&vq.full);

        pthread_mutex_lock(&vq.mutex);
        uint64_t trans_id = vq.id_queue.front();
        Endorsement transaction = vq.trans_queue.front();
        uint64_t curr_block_id = vq.curr_block_id;
        vq.id_queue.pop();
        vq.trans_queue.pop();
        pthread_mutex_unlock(&vq.mutex);

        validate_transaction(ctx, storage_client, curr_block_id, trans_id, transaction);

        // add this transaction to C
        C.add(trans_id);
    }    

    return NULL;
}

void *parallel_validation_manager(void *arg) {
    /* set up grpc client for storage server */
    KVStableClient storage_client(storage_channel_ptr);
    string serialised_block;
    set<uint64_t> W;

    while (!end_flag) {
        sem_wait(&bq.full);
        pthread_mutex_lock(&bq.mutex);
        Block block = bq.bq_queue.front();
        bq.bq_queue.pop();
        pthread_mutex_unlock(&bq.mutex);

        W.clear();
        C.clear();
        for (int trans_id = 0; trans_id < block.transactions_size(); trans_id++) {
            W.insert(trans_id);
        }

        while (!W.empty()) {
            for (auto it = W.begin(); it != W.end(); ) {
                uint64_t trans_id = *it;
                bool all_pred_in_c = true;

                // if (block.transactions(trans_id).adjacency_list_size() > 0) {
                //     log_info(stderr, "[block_id = %ld, trans_id = %ld] size of adjacency list = %d.",
                //             block.block_id(), trans_id, block.transactions(trans_id).adjacency_list_size());
                // }

                for (int i = 0; i < block.transactions(trans_id).adjacency_list_size(); i++) {
                    uint64_t pred_id = block.transactions(trans_id).adjacency_list(i);
                    if (!C.find(pred_id)) {
                        all_pred_in_c = false;
                        break;
                    }
                }

                if (all_pred_in_c) {
                    it = W.erase(it);

                    /* trigger parallel validation worker for this transaction */
                    pthread_mutex_lock(&vq.mutex);
                    vq.curr_block_id = block.block_id();
                    vq.id_queue.push(trans_id);
                    vq.trans_queue.emplace(block.transactions(trans_id));
                    pthread_mutex_unlock(&vq.mutex);
                    sem_post(&vq.full);
                } else {
                    it++;
                }
            }
        }

        while (C.size() != block.transactions_size())
            ;

        /* store the block with its bit mask in remote block store */
        if (!block.SerializeToString(&serialised_block)) {
            log_err("validator: failed to serialize block.");
        }
        storage_client.write_blocks(serialised_block);
    }

    return NULL;
}

class ComputeCommImpl final : public ComputeComm::Service {
   public:
    Status send_to_validator(ServerContext *context, const Block *request, google::protobuf::Empty *response) override {
        pthread_mutex_lock(&bq.mutex);
        bq.bq_queue.push(*request);
        pthread_mutex_unlock(&bq.mutex);
        sem_post(&bq.full);
        // total_ops += request->transactions_size();

        return Status::OK;
    }
};

void run_server(const string &server_address) {
    std::filesystem::remove_all("./testdb");

    // options.create_if_missing = true;
    // options.error_if_exists = true;
    // options.write_buffer_size = 4096000000;
    // leveldb::Status s = leveldb::DB::Open(options, "./testdb", &db);
    // assert(s.ok());

    /* start the grpc server for ComputeComm service */
    ComputeCommImpl service;
    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    log_info(stderr, "RPC server listening on %s", server_address.c_str());

    /* init local caches and spawn the thread pool */
    total_ops = 0;
    for (int offset = 0; offset < c_config_info.data_cache_size; offset += c_config_info.data_msg_size) {
        char *addr = c_ib_info.ib_data_buf + offset;
        datacache.free_addrs.push((uintptr_t)addr);
    }

    pthread_t bg_tid;
    pthread_create(&bg_tid, NULL, background_handler, NULL);
    pthread_t validation_manager_tid;
    pthread_create(&validation_manager_tid, NULL, parallel_validation_manager, NULL);
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(0, &cpuset);
    int ret = pthread_setaffinity_np(validation_manager_tid, sizeof(cpu_set_t), &cpuset);
    if (ret) {
        log_err("pthread_setaffinity_np failed with '%s'.", strerror(ret));
    }

    int num_threads = c_config_info.num_qps_per_server;
    int num_sim_threads = c_config_info.num_sim_threads;
    pthread_t tid[num_threads];
    struct ThreadContext *ctxs = (struct ThreadContext *)calloc(num_threads, sizeof(struct ThreadContext));
    for (int i = 0; i < num_threads; i++) {
        assert(c_ib_info.cq[i] != NULL);
        assert(c_ib_info.qp[i] != NULL);
        ctxs[i].thread_index = i;
        ctxs[i].m_qp = c_ib_info.qp[i];
        ctxs[i].m_cq = c_ib_info.cq[i];
        if (i < num_sim_threads) {
            pthread_create(&tid[i], NULL, simulation_handler, &ctxs[i]);
        } else {
            pthread_create(&tid[i], NULL, parallel_validation_worker, &ctxs[i]);
            // pthread_create(&tid[i], NULL, validation_handler, &ctxs[i]);
        }
        /* stick thread to a core for better performance */
        int num_cores = sysconf(_SC_NPROCESSORS_ONLN);
        int core_id = i % num_cores;
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(core_id, &cpuset);
        int ret = pthread_setaffinity_np(tid[i], sizeof(cpu_set_t), &cpuset);
        if (ret) {
            log_err("pthread_setaffinity_np failed with '%s'.", strerror(ret));
        }
    }

    /* microbenchmark logics */
    uint64_t time = benchmark_throughput();
    // test_get_put_mix();

    /* output stats */
    void *status;
    // for (int i = 0; i < num_threads; i++) {
    //     pthread_join(tid[i], &status);
    // }

    log_info(stderr, "throughput = %f /seconds.", ((float)total_ops.load() / time) * 1000);
    log_info(stderr, "abort rate = %f.", ((float)abort_count.load() / ((float)total_ops.load() + (float)abort_count.load())) * 100);
    // log_info(stderr, "cache hit ratio = %f.", ((float)cache_hit.load() / (float)cache_total.load()) * 100);
    // log_info(stderr, "sstable ratio = %f.", ((float)sst_count.load() / (float)cache_total.load()) * 100);

    // pthread_join(validation_manager_tid, &status);
    pthread_join(bg_tid, &status);
    free(ctxs);
}

/* return 0 on success, 1 on not found, -1 on error */
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
            return 1;
        } else if (get_rsp.status() == GetResponse_Status_ERROR) {
            log_err("Error in reading sstables.");
            return -1;
        }
    }
    return 0;
}

int KVStableClient::write_sstables(const string &key, const string &value) {
    ClientContext context;
    EvictedBuffers ev;
    EvictionResponse rsp;

    char *buf = (char *)malloc(c_config_info.data_msg_size);
    bzero(buf, c_config_info.data_msg_size);
    memcpy(buf, value.c_str(), value.size());
    string actual_value(buf, c_config_info.data_msg_size);

    (*ev.mutable_eviction())[key] = actual_value;

    Status status = stub_->write_sstables(&context, ev, &rsp);
    if (!status.ok()) {
        log_err("gRPC failed with error message: %s.", status.error_message().c_str());
        free(buf);
        return -1;
    }
    free(buf);
    return 0;
}

int KVStableClient::write_blocks(const string &block) {
    ClientContext context;
    SerialisedBlock serialised_block;
    google::protobuf::Empty rsp;

    serialised_block.set_block(block);

    Status status = stub_->write_blocks(&context, serialised_block, &rsp);

    return 0;
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

    int opt;
    string configfile = "config/compute.config";
    string server_address;
    while ((opt = getopt(argc, argv, "ha:c:")) != -1) {
        switch (opt) {
            case 'h':
                fprintf(stderr, "compute server usage:\n");
                fprintf(stderr, "\t-h: print this help message\n");
                fprintf(stderr, "\t-a <server_ip:server_port>: the listening addr of grpc server\n");
                fprintf(stderr, "\t-c <path_to_config_file>: path to the configuration file\n");
                exit(0);
            case 'a':
                server_address = std::string(optarg);
                break;
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
        } else if (tmp[0] == "num_sim_threads") {
            sstream >> c_config_info.num_sim_threads;
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
        } else if (tmp[0] == "storage_grpc_endpoint") {
            c_config_info.storage_grpc_endpoint = tmp[1];
        } else if (tmp[0] == "orderer_grpc_endpoint") {
            c_config_info.orderer_grpc_endpoint = tmp[1];
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

    /* set up grpc channels */
    storage_channel_ptr = grpc::CreateChannel(c_config_info.storage_grpc_endpoint, grpc::InsecureChannelCredentials());
    orderer_channel_ptr = grpc::CreateChannel(c_config_info.orderer_grpc_endpoint, grpc::InsecureChannelCredentials());

    /* set up RDMA connection with the memory server */
    compute_setup_ib(c_config_info, c_ib_info);

    run_server(server_address);
}