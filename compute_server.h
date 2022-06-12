#ifndef COMPUTE_SERVER_H
#define COMPUTE_SERVER_H

#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdio.h>
#include <unistd.h>

#include <atomic>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <list>
#include <memory>
#include <queue>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "blockchain.grpc.pb.h"
#include "storage.grpc.pb.h"

using namespace std;
using grpc::Channel;
using grpc::ClientAsyncWriter;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::Status;

struct ThreadContext {
    int thread_index;
    struct ibv_qp* m_qp;
    struct ibv_cq* m_cq;
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
    int write_sstables(const string& key, const string& value);
    int write_blocks(const string& block);

   private:
    unique_ptr<KVStable::Stub> stub_;
};

class ComputeCommClient {
   public:
    ComputeCommClient(std::shared_ptr<Channel> channel)
        : stub_(ComputeComm::NewStub(channel)) {}

    int invalidate_cn(const string& key);
    void start_benchmarking();

   private:
    unique_ptr<ComputeComm::Stub> stub_;
};

class BlockQueue {
   public:
    queue<Block> bq_queue;
    pthread_mutex_t mutex;
    sem_t full;

    BlockQueue() {
        pthread_mutex_init(&mutex, NULL);
        sem_init(&full, 0, 0);
    }

    ~BlockQueue() {
        pthread_mutex_destroy(&mutex);
        sem_destroy(&full);
    }
};

class ValidationQueue {
   public:
    queue<uint64_t> id_queue;
    queue<Endorsement> trans_queue;
    uint64_t curr_block_id;
    pthread_mutex_t mutex;
    sem_t full;

    ValidationQueue() {
        pthread_mutex_init(&mutex, NULL);
        sem_init(&full, 0, 0);
    }

    ~ValidationQueue() {
        pthread_mutex_destroy(&mutex);
        sem_destroy(&full);
    }
};

class CompletionSet {
   private:
    set<uint64_t> C;
    pthread_mutex_t mutex;

   public:
    CompletionSet() {
        pthread_mutex_init(&mutex, NULL);
    }

    ~CompletionSet() {
        pthread_mutex_destroy(&mutex);
    }

    void add(uint64_t trans_id) {
        pthread_mutex_lock(&mutex);
        C.insert(trans_id);
        pthread_mutex_unlock(&mutex);
    }

    void clear() {
        pthread_mutex_lock(&mutex);
        C.clear();
        pthread_mutex_unlock(&mutex);
    }

    bool find(uint64_t trans_id) {
        bool find = false;
        pthread_mutex_lock(&mutex);
        if (C.find(trans_id) != C.end()) {
            find = true;
        }
        pthread_mutex_unlock(&mutex);

        return find;
    }

    size_t size() {
        size_t size;
        // pthread_mutex_lock(&mutex);
        size = C.size();
        // pthread_mutex_unlock(&mutex);

        return size;
    }
};

#endif