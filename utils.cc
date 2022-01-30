#include "utils.h"

#include "log.h"

vector<string> split(const string &s, const string &delim) {
    vector<string> results;
    if (s == "") {
        return results;
    }
    size_t prev = 0, cur = 0;
    do {
        cur = s.find(delim, prev);
        if (cur == string::npos) {
            cur = s.length();
        }
        string part = s.substr(prev, cur - prev);
        auto no_space_end = remove(part.begin(), part.end(), ' ');
        part.erase(no_space_end, part.end());
        if (!part.empty()) {
            results.emplace_back(part);
        }
        prev = cur + delim.length();
    } while (cur < s.length() && prev < s.length());
    return results;
}

int sock_read(int sock_fd, char *buf, size_t len) {
    int rcvd = 0;
    while (rcvd < len) {
        int n = read(sock_fd, &buf[rcvd], len - rcvd);
        if (n < 0) {
            return -1;
        }
        rcvd += n;
    }
    return rcvd;
}

int sock_write(int sock_fd, char *buf, size_t len) {
    int sent = 0;
    while (sent < len) {
        int n = write(sock_fd, &buf[sent], len - sent);
        if (n < 0) {
            return -1;
        }
        sent += n;
    }
    return sent;
}

int post_srq_recv(uint32_t req_size, uint32_t lkey, uint64_t wr_id,
                  struct ibv_srq *srq, char *buf) {
    int ret = 0;
    struct ibv_recv_wr *bad_recv_wr;

    struct ibv_sge list = {
        .addr = (uintptr_t)buf,
        .length = req_size,
        .lkey = lkey};

    struct ibv_recv_wr recv_wr = {
        .wr_id = wr_id,
        .sg_list = &list,
        .num_sge = 1};

    ret = ibv_post_srq_recv(srq, &recv_wr, &bad_recv_wr);
    return ret;
}

int post_recv(uint32_t req_size, uint32_t lkey, uint64_t wr_id,
              struct ibv_qp *qp, char *buf) {
    int ret = 0;
    struct ibv_recv_wr *bad_recv_wr;

    struct ibv_sge list = {
        .addr = (uintptr_t)buf,
        .length = req_size,
        .lkey = lkey};

    struct ibv_recv_wr recv_wr = {
        .wr_id = wr_id,
        .sg_list = &list,
        .num_sge = 1};

    ret = ibv_post_recv(qp, &recv_wr, &bad_recv_wr);
    return ret;
}

int post_send(uint32_t req_size, uint32_t lkey, uint64_t wr_id,
              uint32_t imm_data, struct ibv_qp *qp, char *buf) {
    int ret = 0;
    struct ibv_send_wr *bad_send_wr;

    struct ibv_sge list = {
        .addr = (uintptr_t)buf,
        .length = req_size,
        .lkey = lkey};

    struct ibv_send_wr send_wr = {
        .wr_id = wr_id,
        .sg_list = &list,
        .num_sge = 1,
        .opcode = IBV_WR_SEND_WITH_IMM,
        .send_flags = IBV_SEND_SIGNALED,
        .imm_data = htonl(imm_data)};

    ret = ibv_post_send(qp, &send_wr, &bad_send_wr);
    return ret;
}

int post_write_with_imm(uint32_t req_size, uint32_t lkey, uint64_t wr_id, uint32_t imm_data,
                        struct ibv_qp *qp, char *buf, uint64_t raddr, uint32_t rkey) {
    int ret = 0;
    struct ibv_send_wr *bad_send_wr;

    struct ibv_sge list = {
        .addr = (uintptr_t)buf,
        .length = req_size,
        .lkey = lkey};

    struct ibv_send_wr send_wr = {
        .wr_id = wr_id,
        .sg_list = &list,
        .num_sge = 1,
        .opcode = IBV_WR_RDMA_WRITE_WITH_IMM,
        .send_flags = IBV_SEND_SIGNALED,
        .imm_data = htonl(imm_data),
        .wr = {
            .rdma = {
                .remote_addr = raddr,
                .rkey = rkey,
            }
        }
    };

    ret = ibv_post_send(qp, &send_wr, &bad_send_wr);
    return ret;
}

int post_read(uint32_t req_size, uint32_t lkey, uint64_t wr_id,
              struct ibv_qp *qp, char *buf, uint64_t raddr, uint32_t rkey) {
    int ret = 0;
    struct ibv_send_wr *bad_send_wr;

    struct ibv_sge list = {
        .addr = (uintptr_t)buf,
        .length = req_size,
        .lkey = lkey};

    struct ibv_send_wr send_wr = {
        .wr_id = wr_id,
        .sg_list = &list,
        .num_sge = 1,
        .opcode = IBV_WR_RDMA_READ,
        .send_flags = IBV_SEND_SIGNALED,
        .wr = {
            .rdma = {
                .remote_addr = raddr,
                .rkey = rkey,
            }
        }
    };

    ret = ibv_post_send(qp, &send_wr, &bad_send_wr);
    return ret;
}

int poll_completion(int thread_index, struct ibv_cq *cq, enum ibv_wc_opcode target_opcode) {
    int num_wc = 20;
    struct ibv_wc *wc;
    wc = (struct ibv_wc *)calloc(num_wc, sizeof(struct ibv_wc));
    int target_count = 0;
    while (true) {
        int n = ibv_poll_cq(cq, num_wc, wc);
        if (n < 0) {
            log_err("thread[%d]: Failed to poll cq.", thread_index);
            free(wc);
            return -1;
        }
        for (int i = 0; i < n; i++) {
            if (wc[i].status != IBV_WC_SUCCESS) {
                if (wc[i].opcode == IBV_WC_SEND) {
                    log_err("thread[%d]: send failed with status %s", thread_index, ibv_wc_status_str(wc[i].status));
                } else {
                    log_err("thread[%d]: recv failed with status %s", thread_index, ibv_wc_status_str(wc[i].status));
                }
                free(wc);
                return -1;
            }

            if (wc[i].opcode == target_opcode) {
                target_count++;
            }
        }
        if (target_count) {
            free(wc);
            return target_count;
        }
    }
}

/* blocks the calling thread until work completion */
void wait_completion(ibv_comp_channel *comp_channel, ibv_cq *cq, enum ibv_wc_opcode target_opcode, int target) {
    int target_count = 0;
    while (target_count < target) {
        struct ibv_cq *ev_cq;
        void *ev_ctx;

        if (ibv_get_cq_event(comp_channel, &ev_cq, &ev_ctx)) {
            log_err("Failed to wait for a completion event.");
        }

        ibv_ack_cq_events(cq, 1);

        if (ibv_req_notify_cq(cq, 0)) {
            log_err("Failed to request for a notification.");
        }

        int ne;
        struct ibv_wc wc;
        do {
            ne = ibv_poll_cq(cq, 1, &wc);
            if (ne == 0) {
                continue;
            }
            if (ne < 0) {
                log_err("Failed to poll bg_cq.");
                break;
            }
            if (wc.status != IBV_WC_SUCCESS) {
                log_err("WC failed with status %s in bg_cq.", ibv_wc_status_str(wc.status));
                break;
            } else {
                if (wc.opcode == target_opcode) {
                    target_count++;
                }
            }
        } while (ne);
    }
}