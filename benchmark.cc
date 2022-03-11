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

void *client_thread(void *arg) {
    int key_num = *(int *)arg;
    int trans_per_interval = 2000;
    int interval = 30000;

    default_random_engine generator;
    uniform_int_distribution<int> distribution(0, key_num - 1);
    rand_val(1);

    while (!end_flag) {
        usleep(interval);

        for (int i = 0; i < trans_per_interval; i++) {
            int number = distribution(generator);
            // int number = zipf(2.0, key_num);
            struct Request req;
            req.type = Request::Type::GET;
            req.key = "key_" + to_string(number);
            req.is_prep = false;
            pthread_mutex_lock(&rq.mutex);
            rq.rq_queue.push(req);
            pthread_mutex_unlock(&rq.mutex);
            sem_post(&rq.full);

            number = distribution(generator);
            // number = zipf(2.0, key_num);
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
    int key_num = 400000;
    for (int i = key_num; i >= 0; i--) {
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
