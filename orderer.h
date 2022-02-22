#ifndef ORDERER_H
#define ORDERER_H

#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <pthread.h>

#include <atomic>
#include <vector>
#include <deque>
#include <set>
#include <queue>
#include <string>
#include <filesystem>
#include <fstream>
#include <chrono>
#include <random>

#include "blockchain.grpc.pb.h"

#define LOG_ENTRY_BATCH 10

using namespace std;
using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;

enum Role {
    LEADER,
    FOLLOWER,
    CANDIDATE,
};

struct ThreadContext {
    string grpc_endpoint;
    int server_index;
    int majority;
};

class TransactionQueue {
   public:
    queue<string> trans_queue;
    pthread_mutex_t mutex;

    TransactionQueue() {
        pthread_mutex_init(&mutex, NULL);
    }

    ~TransactionQueue() {
        pthread_mutex_destroy(&mutex);
    }
};

#endif