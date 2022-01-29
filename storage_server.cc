#include <grpc/grpc.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <cassert>
#include <filesystem>
#include <iostream>

#include "storage.grpc.pb.h"
#include "leveldb/db.h"
#include "leveldb/write_batch.h"
#include "log.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

leveldb::DB* db;
leveldb::Options options;

class KVStableImpl final : public KVStable::Service {
   public:
    Status write_sstables(ServerContext* context, const EvictedBuffers* request, EvictionResponse* response) override {
        leveldb::WriteBatch batch;
        for (auto it = request->eviction().begin(); it != request->eviction().end(); it++) {
            batch.Put(it->first, it->second);
        }
        leveldb::Status s = db->Write(leveldb::WriteOptions(), &batch);
        if (!s.ok()) {
            log_err("%s", s.ToString().c_str());
        }
        return Status::OK;
    }

    Status read_sstables(ServerContext* context, const GetRequest* request, GetResponse* response) override {
        std::string value;
        leveldb::Status s = db->Get(leveldb::ReadOptions(), request->key(), &value);
        
        if (s.ok()) {
            response->set_value(value);
            response->set_status(GetResponse_Status_FOUND);
        } else {
            if (s.IsNotFound()) {
                response->set_status(GetResponse_Status_NOTFOUND);
            } else {
                response->set_status(GetResponse_Status_ERROR);
                log_err("%s", s.ToString().c_str());
            }
        }
        return Status::OK;
    }

};

void run_server(const std::string& db_name, const std::string& server_address) {
    std::filesystem::remove_all(db_name);

    options.create_if_missing = true;
    options.error_if_exists = true;
    leveldb::Status status = leveldb::DB::Open(options, db_name, &db);
    assert(status.ok());

    KVStableImpl service;
    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    log_info("Server listening on %s", server_address.c_str());
    server->Wait();
}

int main(int argc, char* argv[]) {
    int opt;
    std::string db_name = "/tmp/testdb";
    std::string server_address = "0.0.0.0:50051";

    while ((opt = getopt(argc, argv, "ha:d:")) != -1) {
        switch (opt) {
            case 'h':
                fprintf(stderr, "storage server usage:\n");
                fprintf(stderr, "\t-h: print this help message\n");
                fprintf(stderr, "\t-a <server_ip:server_port>: the listening addr of grpc server\n");
                fprintf(stderr, "\t-d <directory>: the directory for leveldb\n");
                exit(0);
            case 'a':
                server_address = std::string(optarg);
                break;
            case 'd':
                db_name = std::string(optarg);
                break;
            default:
                fprintf(stderr, "Invalid option -%c\n", opt);
                exit(1);
                break;
        }
    }

    run_server(db_name, server_address);

    return 0;
}
