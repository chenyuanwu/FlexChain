#ifndef SETUP_IB_H
#define SETUP_IB_H

#include <infiniband/verbs.h>
#include <pthread.h>
#include <map>

#define SOCK_SYNC_MSG "sync"
#define IB_PORT 1
#define IB_MTU IBV_MTU_4096
#define IB_SL 0
#define C_MAX_CQE 10
#define M_MAX_CQE 100
#define MAX_SRQ_WR 100
#define MAX_QP_WR 10

using namespace std;

struct MemoryIBInfo {
    /* ib setup information on memory server */
    struct ibv_context *ctx;
    struct ibv_pd *pd;
    struct ibv_mr *mr_data;
    struct ibv_mr *mr_control;
    struct ibv_cq *cq;
    struct ibv_qp **qp;
    map<uint32_t, int> qp_num_to_idx;
    int num_qps;
    struct ibv_srq *srq;
    struct ibv_port_attr port_attr;
    struct ibv_device_attr dev_attr;

    struct ibv_comp_channel *comp_channel;
    struct ibv_cq *bg_cq;
    struct ibv_qp **bg_qp;
    struct ibv_mr *mr_bg;

    char *ib_data_buf;
    char *ib_control_buf;
    char *ib_bg_buf;
    pthread_mutex_t bg_buf_lock;
};

struct ComputeIBInfo {
    /* ib setup information on compute server */
    struct ibv_context *ctx;
    struct ibv_pd *pd;
    struct ibv_mr *mr_data;
    uint32_t remote_mr_data_rkey;
    struct ibv_mr *mr_control;
    struct ibv_cq **cq;
    struct ibv_qp **qp;
    int num_qps;
    struct ibv_port_attr port_attr;
    struct ibv_device_attr dev_attr;

    struct ibv_comp_channel *comp_channel;
    struct ibv_cq *bg_cq;
    struct ibv_qp *bg_qp;
    struct ibv_mr *mr_bg;

    char *ib_data_buf;
    char *ib_control_buf;
    char *ib_bg_buf;
};

int memory_setup_ib(struct MConfigInfo &m_config_info, struct MemoryIBInfo &m_ib_info);
int compute_setup_ib(struct CConfigInfo &c_config_info, struct ComputeIBInfo &c_ib_info);

// void memory_close_ib();
// void compute_close_ib();

int connect_qp_server(struct MConfigInfo &m_config_info, struct MemoryIBInfo &m_ib_info);
int connect_qp_client(struct CConfigInfo &c_config_info, struct ComputeIBInfo &c_ib_info);

int modify_qp_to_rts(struct ibv_qp *qp, uint32_t target_qp_num, uint16_t target_lid);

#endif