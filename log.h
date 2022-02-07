#ifndef LOG_H
#define LOG_H

#include <pthread.h>
#include <stdio.h>
#include <string.h>

extern pthread_mutex_t logger_lock;

#define log_err(M, ...)                                       \
    do {                                                      \
        pthread_mutex_lock(&logger_lock);                     \
        fprintf(stderr, "[ERROR] (%s:%d:%s) " M "\n",         \
                __FILE__, __LINE__, __func__, ##__VA_ARGS__); \
        pthread_mutex_unlock(&logger_lock);                   \
    } while (0)

#define log_warn(M, ...)                                      \
    do {                                                      \
        pthread_mutex_lock(&logger_lock);                     \
        fprintf(stderr, "[WARN] (%s:%d:%s) " M "\n",          \
                __FILE__, __LINE__, __func__, ##__VA_ARGS__); \
        pthread_mutex_unlock(&logger_lock);                   \
    } while (0)

#define log_info(fp, M, ...)                         \
    do {                                              \
        pthread_mutex_lock(&logger_lock);             \
        fprintf(fp, "[INFO]" M "\n", ##__VA_ARGS__); \
        pthread_mutex_unlock(&logger_lock);           \
        fflush(fp);                                   \
    } while (0)

#ifdef DEBUG
#define log_debug(fp, M, ...)                         \
    do {                                             \
        pthread_mutex_lock(&logger_lock);            \
        fprintf(fp, "[DEBUG]" M "\n", ##__VA_ARGS__); \
        pthread_mutex_unlock(&logger_lock);          \
        fflush(fp);                                  \
    } while (0)
#else
#define log_debug(fp, M, ...)
#endif

#endif