#ifndef COMPUTE_SERVER_H
#define COMPUTE_SERVER_H

#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdio.h>
#include <unistd.h>

#include <list>
#include <memory>
#include <queue>
#include <set>
#include <unordered_map>
#include <iostream>
#include <fstream>
#include <string>
#include <sstream>
#include <atomic>

#include "storage.grpc.pb.h"

using namespace std;
using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

struct ThreadContext {
    int thread_index;
    struct ibv_qp *m_qp;
    struct ibv_cq *m_cq;
};

class DataCache {
   public:
    struct Frame {
        uint64_t l_addr;
        uint64_t r_addr;
    };
    unordered_map<uint64_t, list<struct Frame>::iterator> addr_hashtable;
    pthread_mutex_t hashtable_lock;

    list<struct Frame> lru_list;

    queue<uint64_t> free_addrs;
    // since we use write-through, no need to use a background thread for flushing pages

    DataCache() {
        pthread_mutex_init(&hashtable_lock, NULL);
    }
    ~DataCache() {
        pthread_mutex_destroy(&hashtable_lock);
    }
};

class MetaDataCache {
   public:
    struct EntryHeader {
        uint64_t ptr_next;
        uint64_t bk_addr;
        string key;
    };
    unordered_map<string, list<struct EntryHeader>::iterator> key_hashtable;
    pthread_mutex_t hashtable_lock;

    list<struct EntryHeader> lru_list;

    // assume no space limit for metadata cache for now

    MetaDataCache() {
        pthread_mutex_init(&hashtable_lock, NULL);
    }
    ~MetaDataCache() {
        pthread_mutex_destroy(&hashtable_lock);
    }
};

class KVStableClient {
   public:
    KVStableClient(std::shared_ptr<Channel> channel)
        : stub_(KVStable::NewStub(channel)) {}

    int read_sstables(const string& key, string& value);

   private:
    unique_ptr<KVStable::Stub> stub_;
};

#endif