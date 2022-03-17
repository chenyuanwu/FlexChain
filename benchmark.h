#ifndef BENCHMARK_H
#define BENCHMARK_H

#include <assert.h>  
#include <math.h>   
#include <pthread.h>
#include <semaphore.h>
#include <stdio.h>
#include <stdlib.h>  
#include <unistd.h>
#include <math.h>

#include <chrono>
#include <queue>
#include <random>
#include <string>
#include <unordered_set>

using namespace std;

#define FALSE 0  
#define TRUE 1   

struct Request {
    enum Type {
        GET,
        PUT,
        KMEANS
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
int zipf(double alpha, int n);
double rand_val(int seed);
void kmeans(vector<int> &A, int K);

#endif