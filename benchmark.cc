#include "benchmark.h"

#include "log.h"

extern RequestQueue rq;
extern volatile int end_flag;
extern atomic_bool warmup_completed;

/* functionality tests */
int test_get_only() {
    default_random_engine generator;
    uniform_int_distribution<int> distribution(0, 50000);

    for (int i = 0; i < 1000; i++) {
        // struct Request req;
        // req.type = Request::Type::PUT;
        // req.key = "key_" + to_string(i);
        // req.value = "value_" + to_string(i);
        // req.is_prep = true;
        // pthread_mutex_lock(&rq.mutex);
        // rq.rq_queue.push(req);
        // pthread_mutex_unlock(&rq.mutex);
        // sem_post(&rq.full);

        struct Request req;
        req.type = Request::Type::PUT;
        req.key = "key_" + to_string(i);
        uint64_t val_number = distribution(generator);
        char *buf = (char *)malloc(sizeof(uint64_t));
        memcpy(buf, &val_number, sizeof(uint64_t));
        req.value.assign(buf, sizeof(uint64_t));
        req.is_prep = true;
        free(buf);

        pthread_mutex_lock(&rq.mutex);
        rq.rq_queue.push(req);
        pthread_mutex_unlock(&rq.mutex);
        sem_post(&rq.full);
    }

    sleep(2);

    for (int i = 100 - 1; i >= 0; i--) {
        // struct Request req;
        // req.type = Request::Type::GET;
        // req.key = "key_" + to_string(i);
        // req.is_prep = false;
        // pthread_mutex_lock(&rq.mutex);
        // rq.rq_queue.push(req);
        // pthread_mutex_unlock(&rq.mutex);
        // sem_post(&rq.full);

        struct Request req;
        req.type = Request::Type::KMEANS;
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
double rand_val(int seed) {
    const long a = 16807;       // Multiplier
    const long m = 2147483647;  // Modulus
    const long q = 127773;      // m div a
    const long r = 2836;        // m mod a
    static long x;              // Random int value
    long x_div_q;               // x divided by q
    long x_mod_q;               // x modulo q
    long x_new;                 // New x value

    // Set the seed if argument is non-zero and then return zero
    if (seed > 0) {
        x = seed;
        return (0.0);
    }

    // RNG using integer arithmetic
    x_div_q = x / q;
    x_mod_q = x % q;
    x_new = (a * x_mod_q) - (r * x_div_q);
    if (x_new > 0)
        x = x_new;
    else
        x = x_new + m;

    // Return a random value between 0.0 and 1.0
    return ((double)x / m);
}

int zipf(double alpha, int n) {
    static int first = TRUE;   // Static first time flag
    static double c = 0;       // Normalization constant
    static double *sum_probs;  // Pre-calculated sum of probabilities
    double z;                  // Uniform random number (0 < z < 1)
    int zipf_value;            // Computed exponential value to be returned
    int i;                     // Loop counter
    int low, high, mid;        // Binary-search bounds

    // Compute normalization constant on first call only
    if (first == TRUE) {
        for (i = 1; i <= n; i++)
            c = c + (1.0 / pow((double)i, alpha));
        c = 1.0 / c;

        sum_probs = (double *)malloc((n + 1) * sizeof(*sum_probs));
        sum_probs[0] = 0;
        for (i = 1; i <= n; i++) {
            sum_probs[i] = sum_probs[i - 1] + c / pow((double)i, alpha);
        }
        first = FALSE;
    }

    // Pull a uniform random number (0 < z < 1)
    do {
        z = rand_val(0);
    } while ((z == 0) || (z == 1));

    // Map z to the value
    low = 1, high = n, mid;
    do {
        mid = floor((low + high) / 2);
        if (sum_probs[mid] >= z && sum_probs[mid - 1] < z) {
            zipf_value = mid;
            break;
        } else if (sum_probs[mid] >= z) {
            high = mid - 1;
        } else {
            low = mid + 1;
        }
    } while (low <= high);

    // Assert that zipf_value is between 1 and N
    assert((zipf_value >= 1) && (zipf_value <= n));

    return (zipf_value);
}

int find_cluster_id(vector<float> &centers, int target_point) {
    assert(centers.size() > 0);
    float min_distance = abs(centers[0] - target_point);
    int cluster_id = 0;

    for (int id = 0; id < centers.size(); id++) {
        float cur_distance;
        cur_distance = abs(centers[id] - target_point);

        if (cur_distance < min_distance) {
            cluster_id = id;
        }
    }

    return cluster_id;
}

void kmeans(vector<int> &A, int K) {
    // init
    vector<float> centers;
    unordered_set<int> dedup;
    while (centers.size() < K) {
        int rid = rand() % A.size();
        if (dedup.find(A[rid]) == dedup.end()) {
            centers.push_back(A[rid]);
            dedup.insert(A[rid]);
        }
    }
    dedup.clear();
    vector<int> cluster(A.size(), -1);
    float last_err = 0;
    int epoch = 0;
    for (; epoch < 200; epoch++) {
        // assigning to cluster
        for (int i = 0; i < A.size(); i++) {
            int cid = find_cluster_id(centers, A[i]);
            cluster[i] = cid;
        }
        // recalculate centers per cluster
        vector<int> cnt(K, 0);
        vector<int> sum(K, 0);
        float err = 0;
        for (int i = 0; i < A.size(); i++) {
            int cid = cluster[i];
            cnt[cid]++;
            sum[cid] += A[i];
            // error
            err += abs(static_cast<float>(A[i]) - centers[cid]);
        }
        float delta = abs(last_err - err);
        // if (delta < 0.01) {
        //     break;
        // }
        last_err = err;
        // assign new centers
        for (int i = 0; i < K; i++) {
            centers[i] = (static_cast<float>(sum[i]) / cnt[i]);
        }
    }
    // log_info(stderr, "kmeans computation for %ld nodes completed in %d epoch.", A.size(), epoch);
    // for (int i = 0; i < K; i++) {
    //     log_debug(stderr, "Cluster Center %d : %f", i, centers[i]);
    //     log_debug(stderr, "Cluster Elements : ");
    //     for (int j = 0; j < cluster.size(); j++) {
    //         if (cluster[j] == i) {
    //             log_debug(stderr, "%d", A[j]);
    //         }
    //     }
    //     log_debug(stderr, "******************************************");
    // }
}

string get_balance_str(uint64_t balance, size_t length) {
    uint64_t new_balance = balance;
    char *buf = (char *)malloc(length);
    memcpy(buf, &new_balance, sizeof(uint64_t));
    string new_balance_str(buf, length);
    free(buf);
    
    return new_balance_str; 
}

void *client_thread(void *arg) {
    int trans_per_interval = 500;
    int interval = 50000;

    default_random_engine generator;
    uniform_int_distribution<int> ycsb_distribution(0, YCSB_KEY_NUM - 1);
    bernoulli_distribution ycsb_pw_distribution(0.5);
    uniform_int_distribution<int> kmeans_distribution(0, KMEANS_KEY_NUM - 1);
    bernoulli_distribution kmeans_pw_distribution(0.95);
    uniform_int_distribution<int> trans_distribution(0, 4);
    rand_val(1);

    while (!end_flag) {
        usleep(interval);

        for (int i = 0; i < trans_per_interval; i++) {
            /* YCSB workload */
            struct Request req1;
            int number = ycsb_distribution(generator);
            // int number = zipf(2.0, key_num);
            req1.key = "key_y_" + to_string(number);
            req1.value = "value_" + to_string(number);
            req1.is_prep = false;
            if (ycsb_pw_distribution(generator)) {
                req1.type = Request::Type::PUT;
            } else {
                req1.type = Request::Type::GET;
            }
            pthread_mutex_lock(&rq.mutex);
            rq.rq_queue.push(req1);
            pthread_mutex_unlock(&rq.mutex);
            sem_post(&rq.full);

            /* machine learning workload */
            // struct Request req2;
            // if (kmeans_pw_distribution(generator)) {
            //     req2.type = Request::Type::KMEANS;
            // } else {
            //     req2.type = Request::Type::PUT;
            //     int key_number = kmeans_distribution(generator);
            //     req2.key = "key_k_" + to_string(key_number);
            //     uint64_t val_number = kmeans_distribution(generator);
            //     char *buf = (char *)malloc(sizeof(uint64_t));
            //     memcpy(buf, &val_number, sizeof(uint64_t));
            //     req2.value = string(buf, sizeof(uint64_t));
            //     free(buf);
            // }
            // req2.is_prep = false;
            // pthread_mutex_lock(&rq.mutex);
            // rq.rq_queue.push(req2);
            // pthread_mutex_unlock(&rq.mutex);
            // sem_post(&rq.full);

            /* smallbank workload */
            // struct Request req;
            // if (pw_distribution(generator)) {
            //     int transaction = trans_distribution(generator);
            //     if (transaction == 0) {
            //         req.type = Request::Type::TransactSavings;
            //     } else if (transaction == 1) {
            //         req.type = Request::Type::DepositChecking;
            //     } else if (transaction == 2) {
            //         req.type = Request::Type::SendPayment; 
            //     } else if (transaction == 3) {
            //         req.type = Request::Type::WriteCheck;
            //     } else if (transaction == 4) {
            //         req.type = Request::Type::Amalgamate;
            //     }
            // } else {
            //     req.type = Request::Type::Query;
            // }
            // req.is_prep = false;
            // pthread_mutex_lock(&rq.mutex);
            // rq.rq_queue.push(req);
            // pthread_mutex_unlock(&rq.mutex);
            // sem_post(&rq.full);
        }
    }
    return NULL;
}

void prepopulate() {
    default_random_engine generator;
    uniform_int_distribution<int> distribution(0, KMEANS_KEY_NUM - 1);

    for (int i = YCSB_KEY_NUM; i >= 0; i--) {
        /* prepopulate - YSCB workload */
        struct Request req;
        req.type = Request::Type::PUT;
        req.key = "key_y_" + to_string(i);
        req.value = "value_" + to_string(i);
        req.is_prep = true;
        pthread_mutex_lock(&rq.mutex);
        rq.rq_queue.push(req);
        pthread_mutex_unlock(&rq.mutex);
        sem_post(&rq.full);

        /* prepopulate - machine learning workload */
        // struct Request req;
        // req.type = Request::Type::PUT;
        // req.key = "key_" + to_string(i);
        // uint64_t val_number = distribution(generator);
        // char *buf = (char *)malloc(sizeof(uint64_t));
        // memcpy(buf, &val_number, sizeof(uint64_t));
        // req.value = string(buf, sizeof(uint64_t));
        // free(buf);
        // req.is_prep = true;
        // pthread_mutex_lock(&rq.mutex);
        // rq.rq_queue.push(req);
        // pthread_mutex_unlock(&rq.mutex);
        // sem_post(&rq.full);

        /* prepopulate - smallbank workload */
        // struct Request req;
        // req.type = Request::Type::PUT;
        // req.key = "checking_" + to_string(i);
        // uint64_t balance = distribution(generator);
        // char *buf = (char *)malloc(sizeof(uint64_t));
        // memcpy(buf, &balance, sizeof(uint64_t));
        // req.value = string(buf, sizeof(uint64_t));
        // req.is_prep = true;
        // pthread_mutex_lock(&rq.mutex);
        // rq.rq_queue.push(req);
        // pthread_mutex_unlock(&rq.mutex);
        // sem_post(&rq.full);

        // req.key = "saving_" + to_string(i);
        // balance = distribution(generator);
        // memcpy(buf, &balance, sizeof(uint64_t));
        // req.value = string(buf, sizeof(uint64_t));
        // free(buf);
        // pthread_mutex_lock(&rq.mutex);
        // rq.rq_queue.push(req);
        // pthread_mutex_unlock(&rq.mutex);
        // sem_post(&rq.full);
    }

    // for (int i = KMEANS_KEY_NUM; i >= 0; i--) {
    //     /* prepopulate - machine learning workload */
    //     struct Request req;
    //     req.type = Request::Type::PUT;
    //     req.key = "key_k_" + to_string(i);
    //     uint64_t val_number = distribution(generator);
    //     char *buf = (char *)malloc(sizeof(uint64_t));
    //     memcpy(buf, &val_number, sizeof(uint64_t));
    //     req.value = string(buf, sizeof(uint64_t));
    //     free(buf);
    //     req.is_prep = true;
    //     pthread_mutex_lock(&rq.mutex);
    //     rq.rq_queue.push(req);
    //     pthread_mutex_unlock(&rq.mutex);
    //     sem_post(&rq.full);
    // }

    sleep(5);

    while (!rq.rq_queue.empty())
        ;
    log_info(stderr, "*******************************prepopulation completed*******************************");
}

int64_t benchmark_throughput(bool is_validator) {
    log_info(stderr, "*******************************benchmarking started*******************************");
    pthread_t client_tid;
    pthread_create(&client_tid, NULL, client_thread, NULL);

    // if (is_validator) {
    //     sleep(2);
    //     warmup_completed = true;
    // }

    chrono::milliseconds before, after;
    before = chrono::duration_cast<chrono::milliseconds>(chrono::system_clock::now().time_since_epoch());
    if (is_validator) {
        sleep(10);
    } else {
        sleep(10);
    }
    
    end_flag = 1;
    after = chrono::duration_cast<chrono::milliseconds>(chrono::system_clock::now().time_since_epoch());

    void *status;
    pthread_join(client_tid, &status);
    log_info(stderr, "*******************************benchmarking completed*******************************");
    return (after - before).count();
}
