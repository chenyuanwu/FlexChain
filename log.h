#ifndef LOG_H
#define LOG_H

#include <stdio.h>
#include <string.h>

#define log_err(M, ...) fprintf(stderr, "[ERROR] (%s:%d:%s) " M "\n", \
                                __FILE__, __LINE__, __func__, ##__VA_ARGS__)

#define log_warn(M, ...) fprintf(stderr, "[WARN] (%s:%d:%s) " M "\n", \
                                 __FILE__, __LINE__, __func__, ##__VA_ARGS__)

#define log_info(M, ...) fprintf(stderr, "[INFO]" M "\n", ##__VA_ARGS__)

#endif