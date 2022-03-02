#include "setup_ib.h"

#include <arpa/inet.h>
#include <malloc.h>
#include <unistd.h>
#include <errno.h>

#include "config.h"
#include "log.h"
#include "utils.h"

int modify_qp_to_rts(struct ibv_qp *qp, uint32_t target_qp_num, uint16_t target_lid) {
    /* change QP state to INIT */
    struct ibv_qp_attr init_attr = {
        .qp_state = IBV_QPS_INIT,
        .qp_access_flags = IBV_ACCESS_LOCAL_WRITE |
                           IBV_ACCESS_REMOTE_READ |
                           IBV_ACCESS_REMOTE_ATOMIC |
                           IBV_ACCESS_REMOTE_WRITE,
        .pkey_index = 0,
        .port_num = IB_PORT,
    };

    int ret = 0;
    ret = ibv_modify_qp(qp, &init_attr,
                        IBV_QP_STATE | IBV_QP_PKEY_INDEX |
                            IBV_QP_PORT | IBV_QP_ACCESS_FLAGS);
    if (ret != 0) {
        log_err("Failed to modify qp to INIT.");
        return -1;
    }

    /* Change QP state to RTR */
    struct ibv_qp_attr rtr_attr = {
        .qp_state = IBV_QPS_RTR,
        .path_mtu = IB_MTU,
        .rq_psn = 0,
        .dest_qp_num = target_qp_num,
        .ah_attr = {
            .dlid = target_lid,
            .sl = IB_SL,
            .src_path_bits = 0,
            .is_global = 0,
            .port_num = IB_PORT,
        },
        .max_dest_rd_atomic = 1,
        .min_rnr_timer = 12,
    };

    ret = ibv_modify_qp(qp, &rtr_attr,
                        IBV_QP_STATE | IBV_QP_AV |
                            IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
                            IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC |
                            IBV_QP_MIN_RNR_TIMER);
    if (ret != 0) {
        log_err("Failed to change qp to rtr.");
        return -1;
    }

    /* Change QP state to RTS */
    struct ibv_qp_attr rts_attr = {
        .qp_state = IBV_QPS_RTS,
        .sq_psn = 0,
        .max_rd_atomic = 1,
        .timeout = 14,
        .retry_cnt = 7,
        .rnr_retry = 7,
    };

    ret = ibv_modify_qp(qp, &rts_attr,
                        IBV_QP_STATE | IBV_QP_TIMEOUT |
                            IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY |
                            IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC);
    if (ret != 0) {
        log_err("Failed to change qp to rts.");
        return -1;
    }

    return 0;
}

int connect_qp_server(struct MConfigInfo& m_config_info, struct MemoryIBInfo& m_ib_info) {
    /* accept connections from compute servers (clients) */
    int listen_fd = socket(PF_INET, SOCK_STREAM, 0);

    struct sockaddr_in server_addr;
    bzero(&server_addr, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = htons(INADDR_ANY);
    server_addr.sin_port = htons(m_config_info.sock_port);
    if (bind(listen_fd, (struct sockaddr *)&server_addr, sizeof(server_addr))) {
        log_err("Failed to bind server_addr to socket. %s.", strerror(errno));
        return -1;
    }

    if (listen(listen_fd, m_config_info.num_compute_servers)) {
        log_err("Failed to listen. %s.", strerror(errno));
        return -1;
    }

    int *comm_fds;
    comm_fds = (int *)calloc(m_config_info.num_compute_servers, sizeof(int));
    struct sockaddr_in client_addr;
    socklen_t client_addr_len = sizeof(client_addr);
    for (int i = 0; i < m_config_info.num_compute_servers; i++) {
        comm_fds[i] = accept(listen_fd, (struct sockaddr *)&client_addr, &client_addr_len);
        if (comm_fds[i] > 0) {
            log_info(stderr, "Compute server #%d connected: IP = %s.", i, inet_ntoa(client_addr.sin_addr));
        } else {
            log_err("Failed to create comm_fds[%d].", i);
            return -1;
        }
    }

    /* init local qp_info */
    struct QPInfo *local_qp_info;
    struct QPInfo *remote_qp_info;
    local_qp_info = (struct QPInfo *)calloc(m_ib_info.num_qps + m_config_info.num_compute_servers, sizeof(struct QPInfo));
    remote_qp_info = (struct QPInfo *)calloc(m_ib_info.num_qps + m_config_info.num_compute_servers, sizeof(struct QPInfo));

    for (int i = 0; i < m_ib_info.num_qps; i++) {
        local_qp_info[i].lid = m_ib_info.port_attr.lid;
        local_qp_info[i].qp_num = m_ib_info.qp[i]->qp_num;
    }

    for (int i = 0; i < m_config_info.num_compute_servers; i++) {
        local_qp_info[i + m_ib_info.num_qps].lid = m_ib_info.port_attr.lid;
        local_qp_info[i + m_ib_info.num_qps].qp_num = m_ib_info.bg_qp[i]->qp_num;
    }

    /* get qp_info from client */
    for (int i = 0; i < m_config_info.num_compute_servers; i++) {
        for (int j = 0; j < m_config_info.num_qps_per_server; j++) {
            struct QPInfo tmp_qp_info;

            int n = sock_read(comm_fds[i], (char *)&tmp_qp_info, sizeof(struct QPInfo));
            if (n != sizeof(struct QPInfo)) {
                log_err("Error in reading qp_info from socket.");
                return -1;
            }

            int index = i * m_config_info.num_qps_per_server + j;
            remote_qp_info[index].lid = ntohs(tmp_qp_info.lid);
            remote_qp_info[index].qp_num = ntohl(tmp_qp_info.qp_num);
        }
        struct QPInfo tmp_qp_info;

        int n = sock_read(comm_fds[i], (char *)&tmp_qp_info, sizeof(struct QPInfo));
        if (n != sizeof(struct QPInfo)) {
            log_err("Error in reading qp_info from socket.");
            return -1;
        }

        remote_qp_info[i + m_ib_info.num_qps].lid = ntohs(tmp_qp_info.lid);
        remote_qp_info[i + m_ib_info.num_qps].qp_num = ntohl(tmp_qp_info.qp_num);
    }

    /* send qp_info and rkey to client */
    for (int i = 0; i < m_config_info.num_compute_servers; i++) {
        uint32_t rkey = htonl(m_ib_info.mr_data->rkey);
        int n = sock_write(comm_fds[i], (char *)&rkey, sizeof(uint32_t));
        if (n != sizeof(uint32_t)) {
            log_err("Error in writing rkey to socket.");
            return -1;
        }

        for (int j = 0; j < m_config_info.num_qps_per_server; j++) {
            struct QPInfo tmp_qp_info;

            int index = i * m_config_info.num_qps_per_server + j;
            tmp_qp_info.lid = htons(local_qp_info[index].lid);
            tmp_qp_info.qp_num = htonl(local_qp_info[index].qp_num);

            int n = sock_write(comm_fds[i], (char *)&tmp_qp_info, sizeof(struct QPInfo));
            if (n != sizeof(struct QPInfo)) {
                log_err("Error in writing qp_info to socket.");
                return -1;
            }
        }
        struct QPInfo tmp_qp_info;

        tmp_qp_info.lid = htons(local_qp_info[i + m_ib_info.num_qps].lid);
        tmp_qp_info.qp_num = htonl(local_qp_info[i + m_ib_info.num_qps].qp_num);

        n = sock_write(comm_fds[i], (char *)&tmp_qp_info, sizeof(struct QPInfo));
        if (n != sizeof(struct QPInfo)) {
            log_err("Error in writing qp_info to socket.");
            return -1;
        }
    }

    /* change all local QP state to RTS */
    for (int i = 0; i < m_ib_info.num_qps; i++) {
        modify_qp_to_rts(m_ib_info.qp[i], remote_qp_info[i].qp_num, remote_qp_info[i].lid);
    }
    for (int i = 0; i < m_config_info.num_compute_servers; i++) {
        modify_qp_to_rts(m_ib_info.bg_qp[i], remote_qp_info[i + m_ib_info.num_qps].qp_num,
                         remote_qp_info[i + m_ib_info.num_qps].lid);
    }

    /* pre-post recvs */
    char *ctrl_buf = m_ib_info.ib_control_buf;
    bzero(ctrl_buf, m_config_info.ctrl_buffer_size);
    for (int i = 0; i < m_config_info.num_compute_servers; i++) {
        for (int j = 0; j < m_config_info.num_qps_per_server; j++) {
            int ret = post_srq_recv(m_config_info.ctrl_msg_size, m_ib_info.mr_control->lkey,
                                    (uintptr_t)ctrl_buf, m_ib_info.srq, ctrl_buf);
            if (ret != 0) {
                log_err("Failed to pre-post recvs into srq.");
                return -1;
            }
            ctrl_buf += m_config_info.ctrl_msg_size;
        }
    }

    /* request notification */
    if (ibv_req_notify_cq(m_ib_info.bg_cq, 0)) {
        log_err("Failed to request for a notification.");
        return -1;
    }

    /* sync with clients */
    char sock_buf[64];
    for (int i = 0; i < m_config_info.num_compute_servers; i++) {
        bzero(sock_buf, 64);
        int n = sock_read(comm_fds[i], sock_buf, sizeof(SOCK_SYNC_MSG));
        if (strcmp(sock_buf, SOCK_SYNC_MSG) != 0) {
            log_err("Failed to receive sync from client#%d.", i);
            return -1;
        }
    }

    bzero(sock_buf, 64);
    strcpy(sock_buf, SOCK_SYNC_MSG);
    for (int i = 0; i < m_config_info.num_compute_servers; i++) {
        int n = sock_write(comm_fds[i], sock_buf, sizeof(SOCK_SYNC_MSG));
    }
    log_info(stderr, "Memory server has synced with all compute servers.");

    return 0;
}

int memory_setup_ib(struct MConfigInfo &m_config_info, struct MemoryIBInfo &m_ib_info) {
    struct ibv_device **dev_list = NULL;

    /* get IB device list */
    dev_list = ibv_get_device_list(NULL);
    if (dev_list == NULL) {
        log_err("Failed to get ib device list.");
    }

    /* create IB context */
    m_ib_info.ctx = ibv_open_device(*dev_list);
    if (m_ib_info.ctx == NULL) {
        log_err("Failed to open ib device.");
    }

    /* allocate protection domain */
    m_ib_info.pd = ibv_alloc_pd(m_ib_info.ctx);
    if (m_ib_info.pd == NULL) {
        log_err("Failed to allocate protection domain.");
    }

    /* query IB port attribute */
    if (ibv_query_port(m_ib_info.ctx, IB_PORT, &m_ib_info.port_attr)) {
        log_err("Failed to query IB port information.");
    }

    /* register mr */
    m_ib_info.ib_data_buf = (char *)memalign(4096, m_config_info.data_slab_size);
    m_ib_info.ib_control_buf = (char *)memalign(4096, m_config_info.ctrl_buffer_size);
    m_ib_info.ib_bg_buf = (char *)memalign(4096, m_config_info.bg_buffer_size);
    m_ib_info.mr_data = ibv_reg_mr(m_ib_info.pd, (void *)m_ib_info.ib_data_buf, m_config_info.data_slab_size,
                                   IBV_ACCESS_LOCAL_WRITE |
                                       IBV_ACCESS_REMOTE_READ |
                                       IBV_ACCESS_REMOTE_WRITE);
    if (m_ib_info.mr_data == NULL) {
        log_err("Failed to register mr_data.");
    }
    m_ib_info.mr_control = ibv_reg_mr(m_ib_info.pd, (void *)m_ib_info.ib_control_buf, m_config_info.ctrl_buffer_size,
                                      IBV_ACCESS_LOCAL_WRITE |
                                          IBV_ACCESS_REMOTE_READ |
                                          IBV_ACCESS_REMOTE_WRITE);
    if (m_ib_info.mr_control == NULL) {
        log_err("Failed to register mr_control.");
    }
    m_ib_info.mr_bg = ibv_reg_mr(m_ib_info.pd, (void *)m_ib_info.ib_bg_buf, m_config_info.bg_buffer_size,
                                 IBV_ACCESS_LOCAL_WRITE |
                                     IBV_ACCESS_REMOTE_READ |
                                     IBV_ACCESS_REMOTE_WRITE);
    if (m_ib_info.mr_bg == NULL) {
        log_err("Failed to register mr_bg.");
    }

    pthread_mutex_init(&m_ib_info.bg_buf_lock, NULL);

    /* query IB device attr */
    if (ibv_query_device(m_ib_info.ctx, &m_ib_info.dev_attr)) {
        log_err("Failed to query device.");
    }

    /* create cq */
    m_ib_info.cq = ibv_create_cq(m_ib_info.ctx, M_MAX_CQE,
                                 NULL, NULL, 0);
    if (m_ib_info.cq == NULL) {
        log_err("Failed to create cq.");
    }

    m_ib_info.comp_channel = ibv_create_comp_channel(m_ib_info.ctx);
    if (m_ib_info.comp_channel == NULL) {
        log_err("Failed to create completion channel.");
    }
    m_ib_info.bg_cq = ibv_create_cq(m_ib_info.ctx, M_MAX_CQE,
                                    NULL, m_ib_info.comp_channel, 0);
    if (m_ib_info.bg_cq == NULL) {
        log_err("Failed to create bg_cq.");
    }

    /* create srq */
    struct ibv_srq_init_attr srq_init_attr = {
        .attr = {
            .max_wr = MAX_SRQ_WR,
            .max_sge = 1,
        },
    };

    m_ib_info.srq = ibv_create_srq(m_ib_info.pd, &srq_init_attr);
    if (m_ib_info.srq == NULL) {
        log_err("Failed to create srq.");
    }

    /* create qp */
    struct ibv_qp_init_attr qp_init_attr = {
        .send_cq = m_ib_info.cq,
        .recv_cq = m_ib_info.cq,
        .srq = m_ib_info.srq,
        .cap = {
            .max_send_wr = MAX_QP_WR,
            .max_recv_wr = MAX_QP_WR,
            .max_send_sge = 1,
            .max_recv_sge = 1,
        },
        .qp_type = IBV_QPT_RC,
    };

    m_ib_info.num_qps = m_config_info.num_compute_servers * m_config_info.num_qps_per_server;
    m_ib_info.qp = (struct ibv_qp **)calloc(m_ib_info.num_qps, sizeof(struct ibv_qp *));
    for (int i = 0; i < m_ib_info.num_qps; i++) {
        m_ib_info.qp[i] = ibv_create_qp(m_ib_info.pd, &qp_init_attr);
        if (m_ib_info.qp[i] == NULL) {
            log_err("Failed to create qp[%d].", i);
        } else {
            uint32_t qp_num = m_ib_info.qp[i]->qp_num;
            m_ib_info.qp_num_to_idx[qp_num] = i;
        }
    }

    m_ib_info.bg_qp = (struct ibv_qp **)calloc(m_config_info.num_compute_servers, sizeof(struct ibv_qp *));
    for (int i = 0; i < m_config_info.num_compute_servers; i++) {
        struct ibv_qp_init_attr qp_init_attr = {
            .send_cq = m_ib_info.bg_cq,
            .recv_cq = m_ib_info.bg_cq,
            .cap = {
                .max_send_wr = MAX_QP_WR,
                .max_recv_wr = MAX_QP_WR,
                .max_send_sge = 1,
                .max_recv_sge = 1,
            },
            .qp_type = IBV_QPT_RC,
        };
        m_ib_info.bg_qp[i] = ibv_create_qp(m_ib_info.pd, &qp_init_attr);
        if (m_ib_info.bg_qp[i] == NULL) {
            log_err("Failed to create bg_qp[%d].", i);
        }
    }

    /* connect QP */
    connect_qp_server(m_config_info, m_ib_info);

    ibv_free_device_list(dev_list);
    return 0;
}

int connect_qp_client(struct CConfigInfo& c_config_info, struct ComputeIBInfo& c_ib_info) {
    /* establish connection to the memory server */
    int sockfd = socket(PF_INET, SOCK_STREAM, 0);

    struct sockaddr_in server_addr;
    bzero(&server_addr, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(c_config_info.sock_port);
    inet_pton(AF_INET, c_config_info.sock_addr.c_str(), &(server_addr.sin_addr));
    if (connect(sockfd, (struct sockaddr *)&server_addr, sizeof(server_addr))) {
        log_err("Failed to connect to memory server. %s.", strerror(errno));
        return -1;
    }

    /* init local qp_info */
    struct QPInfo *local_qp_info;
    struct QPInfo *remote_qp_info;
    local_qp_info = (struct QPInfo *)calloc(c_ib_info.num_qps + 1, sizeof(struct QPInfo));
    remote_qp_info = (struct QPInfo *)calloc(c_ib_info.num_qps + 1, sizeof(struct QPInfo));

    for (int i = 0; i < c_ib_info.num_qps; i++) {
        local_qp_info[i].lid = c_ib_info.port_attr.lid;
        local_qp_info[i].qp_num = c_ib_info.qp[i]->qp_num;
    }
    local_qp_info[c_ib_info.num_qps].lid = c_ib_info.port_attr.lid;
    local_qp_info[c_ib_info.num_qps].qp_num = c_ib_info.bg_qp->qp_num;

    /* send qp_info to server */
    for (int i = 0; i < c_config_info.num_qps_per_server + 1; i++) {
        struct QPInfo tmp_qp_info;

        tmp_qp_info.lid = htons(local_qp_info[i].lid);
        tmp_qp_info.qp_num = htonl(local_qp_info[i].qp_num);

        int n = sock_write(sockfd, (char *)&tmp_qp_info, sizeof(struct QPInfo));
        if (n != sizeof(struct QPInfo)) {
            log_err("Error in writing qp_info to socket.");
            return -1;
        }
    }

    /* get qp_info and rkey from server */
    uint32_t rkey;
    int n = sock_read(sockfd, (char *)&rkey, sizeof(uint32_t));
    if (n != sizeof(uint32_t)) {
        log_err("Error in reading rkey from socket.");
        return -1;
    }
    c_ib_info.remote_mr_data_rkey = ntohl(rkey);
    for (int i = 0; i < c_config_info.num_qps_per_server + 1; i++) {
        struct QPInfo tmp_qp_info;

        int n = sock_read(sockfd, (char *)&tmp_qp_info, sizeof(struct QPInfo));
        if (n != sizeof(struct QPInfo)) {
            log_err("Error in reading qp_info from socket.");
            return -1;
        }

        remote_qp_info[i].lid = ntohs(tmp_qp_info.lid);
        remote_qp_info[i].qp_num = ntohl(tmp_qp_info.qp_num);
    }

    /* change all local QP state to RTS */
    for (int i = 0; i < c_ib_info.num_qps; i++) {
        modify_qp_to_rts(c_ib_info.qp[i], remote_qp_info[i].qp_num, remote_qp_info[i].lid);
    }
    modify_qp_to_rts(c_ib_info.bg_qp, remote_qp_info[c_ib_info.num_qps].qp_num,
                     remote_qp_info[c_ib_info.num_qps].lid);

    /* pre-post recvs in bg_qp */
    int ret = post_recv(c_config_info.bg_msg_size, c_ib_info.mr_bg->lkey, (uintptr_t)c_ib_info.ib_bg_buf,
                        c_ib_info.bg_qp, c_ib_info.ib_bg_buf);
    if (ret != 0) {
        log_err("Failed to pre-post recvs to bg_qp.");
        return -1;
    }

    /* request notification */
    if (ibv_req_notify_cq(c_ib_info.bg_cq, 0)) {
        log_err("Failed to request for a notification.");
        return -1;
    }

    /* sync with server */
    char sock_buf[64];
    bzero(sock_buf, 64);
    strcpy(sock_buf, SOCK_SYNC_MSG);
    n = sock_write(sockfd, sock_buf, sizeof(SOCK_SYNC_MSG));

    bzero(sock_buf, 64);
    n = sock_read(sockfd, sock_buf, sizeof(SOCK_SYNC_MSG));
    if (strcmp(sock_buf, SOCK_SYNC_MSG) != 0) {
        log_err("Failed to receive sync from server.");
        return -1;
    }
    log_info(stderr, "Computer server has synced with the memory server.");

    return 0;
}

int compute_setup_ib(struct CConfigInfo &c_config_info, struct ComputeIBInfo &c_ib_info) {
    struct ibv_device **dev_list = NULL;

    /* get IB device list */
    dev_list = ibv_get_device_list(NULL);
    if (dev_list == NULL) {
        log_err("Failed to get ib device list.");
    }

    /* create IB context */
    c_ib_info.ctx = ibv_open_device(*dev_list);
    if (c_ib_info.ctx == NULL) {
        log_err("Failed to open ib device.");
    }

    /* allocate protection domain */
    c_ib_info.pd = ibv_alloc_pd(c_ib_info.ctx);
    if (c_ib_info.pd == NULL) {
        log_err("Failed to allocate protection domain.");
    }

    /* query IB port attribute */
    if (ibv_query_port(c_ib_info.ctx, IB_PORT, &c_ib_info.port_attr)) {
        log_err("Failed to query IB port information.");
    }

    /* register mr */
    c_ib_info.ib_data_buf = (char *)memalign(4096, c_config_info.data_cache_size);
    if (c_ib_info.ib_data_buf == NULL) {
        log_err("Failed to allocate ib_data_buf.");
    }
    c_ib_info.ib_control_buf = (char *)memalign(4096, c_config_info.ctrl_buffer_size);
    c_ib_info.ib_bg_buf = (char *)memalign(4096, c_config_info.bg_msg_size);
    c_ib_info.mr_data = ibv_reg_mr(c_ib_info.pd, (void *)c_ib_info.ib_data_buf, c_config_info.data_cache_size,
                                   IBV_ACCESS_LOCAL_WRITE |
                                       IBV_ACCESS_REMOTE_READ |
                                       IBV_ACCESS_REMOTE_WRITE);
    if (c_ib_info.mr_data == NULL) {
        log_err("Failed to register mr_data.");
    }
    c_ib_info.mr_control = ibv_reg_mr(c_ib_info.pd, (void *)c_ib_info.ib_control_buf, c_config_info.ctrl_buffer_size,
                                      IBV_ACCESS_LOCAL_WRITE |
                                          IBV_ACCESS_REMOTE_READ |
                                          IBV_ACCESS_REMOTE_WRITE);
    if (c_ib_info.mr_control == NULL) {
        log_err("Failed to register mr_control.");
    }
    c_ib_info.mr_bg = ibv_reg_mr(c_ib_info.pd, (void *)c_ib_info.ib_bg_buf, c_config_info.bg_msg_size,
                                 IBV_ACCESS_LOCAL_WRITE |
                                     IBV_ACCESS_REMOTE_READ |
                                     IBV_ACCESS_REMOTE_WRITE);
    if (c_ib_info.mr_bg == NULL) {
        log_err("Failed to register mr_bg.");
    }

    /* query IB device attr */
    if (ibv_query_device(c_ib_info.ctx, &c_ib_info.dev_attr)) {
        log_err("Failed to query device.");
    }

    /* create cq */
    c_ib_info.num_qps = c_config_info.num_qps_per_server;
    c_ib_info.cq = (struct ibv_cq **)calloc(c_ib_info.num_qps, sizeof(struct ibv_cq *));
    for (int i = 0; i < c_ib_info.num_qps; i++) {
        c_ib_info.cq[i] = ibv_create_cq(c_ib_info.ctx, C_MAX_CQE,
                                        NULL, NULL, 0);
        if (c_ib_info.cq[i] == NULL) {
            log_err("Failed to create cq[%d]. %s.", i, strerror(errno));
        }
    }

    c_ib_info.comp_channel = ibv_create_comp_channel(c_ib_info.ctx);
    if (c_ib_info.comp_channel == NULL) {
        log_err("Failed to create completion channel.");
    }
    c_ib_info.bg_cq = ibv_create_cq(c_ib_info.ctx, C_MAX_CQE,
                                    NULL, c_ib_info.comp_channel, 0);
    if (c_ib_info.bg_cq == NULL) {
        log_err("Failed to create bg_cq.");
    }

    /* create qp */
    c_ib_info.qp = (struct ibv_qp **)calloc(c_ib_info.num_qps, sizeof(struct ibv_qp *));
    for (int i = 0; i < c_ib_info.num_qps; i++) {
        struct ibv_qp_init_attr qp_init_attr = {
            .send_cq = c_ib_info.cq[i],
            .recv_cq = c_ib_info.cq[i],
            .cap = {
                .max_send_wr = MAX_QP_WR,
                .max_recv_wr = MAX_QP_WR,
                .max_send_sge = 1,
                .max_recv_sge = 1,
            },
            .qp_type = IBV_QPT_RC,
        };
        c_ib_info.qp[i] = ibv_create_qp(c_ib_info.pd, &qp_init_attr);
        if (c_ib_info.qp[i] == NULL) {
            log_err("Failed to create qp[%d].", i);
        }
    }

    struct ibv_qp_init_attr qp_init_attr = {
        .send_cq = c_ib_info.bg_cq,
        .recv_cq = c_ib_info.bg_cq,
        .cap = {
            .max_send_wr = MAX_QP_WR,
            .max_recv_wr = MAX_QP_WR,
            .max_send_sge = 1,
            .max_recv_sge = 1,
        },
        .qp_type = IBV_QPT_RC,
    };
    c_ib_info.bg_qp = ibv_create_qp(c_ib_info.pd, &qp_init_attr);
    if (c_ib_info.bg_qp == NULL) {
        log_err("Failed to create bg_qp.");
    }

    /* connect QP */
    connect_qp_client(c_config_info, c_ib_info);

    ibv_free_device_list(dev_list);
    return 0;
}
