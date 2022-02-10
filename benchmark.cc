#include "benchmark.h"

#include "log.h"

extern RequestQueue rq;
extern volatile int end_flag;

/* functionality tests */
int test_get_only() {
    for (int i = 0; i < 1000; i++) {
        struct Request req;
        req.type = Request::Type::PUT;
        req.key = "key_" + to_string(i);
        req.value = "value_" + to_string(i);
        req.is_prep = true;
        pthread_mutex_lock(&rq.mutex);
        rq.rq_queue.push(req);
        pthread_mutex_unlock(&rq.mutex);
        sem_post(&rq.full);
    }

    sleep(2);

    for (int i = 1000 - 1; i >= 0; i--) {
        struct Request req;
        req.type = Request::Type::GET;
        req.key = "key_" + to_string(i);
        req.is_prep = false;
        pthread_mutex_lock(&rq.mutex);
        rq.rq_queue.push(req);
        pthread_mutex_unlock(&rq.mutex);
        sem_post(&rq.full);
    }
    return 0;
}

int test_get_put_mix() {
    for (int i = 0; i < 1000; i++) {
        struct Request req;
        req.type = Request::Type::PUT;
        req.key = "key_" + to_string(i);
        req.value = "value_" + to_string(i) + "_v0";
        req.is_prep = true;
        pthread_mutex_lock(&rq.mutex);
        rq.rq_queue.push(req);
        pthread_mutex_unlock(&rq.mutex);
        sem_post(&rq.full);
    }

    sleep(2);

    for (int i = 1000 - 1; i >= 0; i--) {
        struct Request req;
        req.type = Request::Type::PUT;
        req.key = "key_" + to_string(i);
        req.value = "value_" + to_string(i) + "_v1";
        req.is_prep = false;
        pthread_mutex_lock(&rq.mutex);
        rq.rq_queue.push(req);
        pthread_mutex_unlock(&rq.mutex);
        sem_post(&rq.full);
    }

    sleep(2);

    for (int i = 0; i < 1000; i++) {
        struct Request req;
        req.type = Request::Type::PUT;
        req.key = "key_" + to_string(i);
        req.value = "value_" + to_string(i) + "_v2";
        req.is_prep = false;
        pthread_mutex_lock(&rq.mutex);
        rq.rq_queue.push(req);
        pthread_mutex_unlock(&rq.mutex);
        sem_post(&rq.full);
    }

    sleep(2);

    for (int i = 1000 - 1; i >= 0; i--) {
        struct Request req;
        req.type = Request::Type::GET;
        req.key = "key_" + to_string(i);
        req.is_prep = false;
        pthread_mutex_lock(&rq.mutex);
        rq.rq_queue.push(req);
        pthread_mutex_unlock(&rq.mutex);
        sem_post(&rq.full);
    }

    return 0;
}

/* throughput tests */
void *client_thread(void *arg) {
    int key_num = *(int *)arg;
    int trans_per_interval = 2000;
    int interval = 50000;

    default_random_engine generator;
    uniform_int_distribution<int> distribution(0, key_num - 1);

    while (!end_flag) {
        usleep(interval);

        for (int i = 0; i < trans_per_interval; i++) {
            int number = distribution(generator);
            struct Request req;
            req.type = Request::Type::GET;
            req.key = "key_" + to_string(number);
            req.is_prep = false;
            pthread_mutex_lock(&rq.mutex);
            rq.rq_queue.push(req);
            pthread_mutex_unlock(&rq.mutex);
            sem_post(&rq.full);

            number = distribution(generator);
            req.type = Request::Type::PUT;
            req.key = "key_" + to_string(number);
            req.value = "value_" + to_string(number);
            req.is_prep = false;
            pthread_mutex_lock(&rq.mutex);
            rq.rq_queue.push(req);
            pthread_mutex_unlock(&rq.mutex);
            sem_post(&rq.full);
        }
    }
    return NULL;
}

int64_t benchmark_throughput() {
    int key_num = 1000;
    for (int i = 0; i < key_num; i++) {
        struct Request req;
        req.type = Request::Type::PUT;
        req.key = "key_" + to_string(i);
        req.value = "value_" + to_string(i);
        req.is_prep = true;
        pthread_mutex_lock(&rq.mutex);
        rq.rq_queue.push(req);
        pthread_mutex_unlock(&rq.mutex);
        sem_post(&rq.full);
    }

    sleep(5);

    while (!rq.rq_queue.empty())
        ;
    log_info(stderr, "*******************************prepopulation completed*******************************");

    pthread_t client_tid;
    pthread_create(&client_tid, NULL, client_thread, &key_num);

    chrono::milliseconds before, after;
    before = chrono::duration_cast<chrono::milliseconds>(chrono::system_clock::now().time_since_epoch());
    sleep(10);
    end_flag = 1;
    after = chrono::duration_cast<chrono::milliseconds>(chrono::system_clock::now().time_since_epoch());

    void *status;
    pthread_join(client_tid, &status);
    log_info(stderr, "*******************************benchmarking completed*******************************");
    return (after - before).count();
}
