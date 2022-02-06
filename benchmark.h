#ifndef BENCHMARK_H
#define BENCHMARK_H

#include <pthread.h>
#include <semaphore.h>
#include <stdio.h>
#include <unistd.h>

#include <string>
#include <queue>
#include <random>
#include <chrono>

using namespace std;

struct Request {
    enum Type {
        GET,
        PUT,
    };
    Type type;
    string key;
    string value;
    bool is_prep;
};

class RequestQueue {
   public:
    queue<struct Request> rq_queue;
    pthread_mutex_t mutex;
    sem_t full;

    RequestQueue() {
        pthread_mutex_init(&mutex, NULL);
        sem_init(&full, 0, 0);
    }

    ~RequestQueue() {
        pthread_mutex_destroy(&mutex);
        sem_destroy(&full);
    }
};

int test_get_only();
int test_get_put_mix();
int64_t benchmark_throughput();

#endif