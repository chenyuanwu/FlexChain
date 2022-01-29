#ifndef CONFIG_H
#define CONFIG_H

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>

#include <string>

#define ALLOC_MSG "ALLO"
#define ADDR_MSG "ADDR"
#define FOUND_MSG "FOUN"
#define NOT_FOUND_MSG "NOTF"
#define BACKOFF_MSG "BACK"
#define COMMITTED_MSG "COMM"
#define EVICT_MSG "EVIC"
#define INVAL_MSG "INVA"
#define INVAL_COMP_MSG "ICOM"
#define GC_MSG "GMSG"
#define GC_COMP_MSG "GCOM"
#define CTL_MSG_TYPE_SIZE 4
#define LRU_KEY_NUM 100
#define GC_THR 100

using namespace std;

struct MConfigInfo {
    int num_compute_servers;
    int num_qps_per_server;
    size_t data_msg_size;
    size_t data_slab_size;
    size_t ctrl_msg_size;
    size_t ctrl_buffer_size;
    size_t bg_msg_size;
    size_t bg_buffer_size;
    uint16_t sock_port;
    string grpc_endpoint;
};

struct CConfigInfo {
    int num_qps_per_server;
    size_t data_msg_size;
    size_t data_cache_size;
    size_t metadata_cache_size;
    size_t ctrl_msg_size;
    size_t ctrl_buffer_size;
    size_t bg_msg_size;
    uint16_t sock_port;
    string sock_addr;
    string grpc_endpoint;
};

#endif