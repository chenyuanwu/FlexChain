// Generated by the gRPC C++ plugin.
// If you make any local change, they will be lost.
// source: blockchain.proto

#include "blockchain.pb.h"
#include "blockchain.grpc.pb.h"

#include <functional>
#include <grpcpp/impl/codegen/async_stream.h>
#include <grpcpp/impl/codegen/async_unary_call.h>
#include <grpcpp/impl/codegen/channel_interface.h>
#include <grpcpp/impl/codegen/client_unary_call.h>
#include <grpcpp/impl/codegen/client_callback.h>
#include <grpcpp/impl/codegen/message_allocator.h>
#include <grpcpp/impl/codegen/method_handler.h>
#include <grpcpp/impl/codegen/rpc_service_method.h>
#include <grpcpp/impl/codegen/server_callback.h>
#include <grpcpp/impl/codegen/server_callback_handlers.h>
#include <grpcpp/impl/codegen/server_context.h>
#include <grpcpp/impl/codegen/service_type.h>
#include <grpcpp/impl/codegen/sync_stream.h>

static const char* Consensus_method_names[] = {
  "/Consensus/append_entries",
};

std::unique_ptr< Consensus::Stub> Consensus::NewStub(const std::shared_ptr< ::grpc::ChannelInterface>& channel, const ::grpc::StubOptions& options) {
  (void)options;
  std::unique_ptr< Consensus::Stub> stub(new Consensus::Stub(channel, options));
  return stub;
}

Consensus::Stub::Stub(const std::shared_ptr< ::grpc::ChannelInterface>& channel, const ::grpc::StubOptions& options)
  : channel_(channel), rpcmethod_append_entries_(Consensus_method_names[0], options.suffix_for_stats(),::grpc::internal::RpcMethod::NORMAL_RPC, channel)
  {}

::grpc::Status Consensus::Stub::append_entries(::grpc::ClientContext* context, const ::AppendRequest& request, ::AppendResponse* response) {
  return ::grpc::internal::BlockingUnaryCall< ::AppendRequest, ::AppendResponse, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(channel_.get(), rpcmethod_append_entries_, context, request, response);
}

void Consensus::Stub::async::append_entries(::grpc::ClientContext* context, const ::AppendRequest* request, ::AppendResponse* response, std::function<void(::grpc::Status)> f) {
  ::grpc::internal::CallbackUnaryCall< ::AppendRequest, ::AppendResponse, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(stub_->channel_.get(), stub_->rpcmethod_append_entries_, context, request, response, std::move(f));
}

void Consensus::Stub::async::append_entries(::grpc::ClientContext* context, const ::AppendRequest* request, ::AppendResponse* response, ::grpc::ClientUnaryReactor* reactor) {
  ::grpc::internal::ClientCallbackUnaryFactory::Create< ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(stub_->channel_.get(), stub_->rpcmethod_append_entries_, context, request, response, reactor);
}

::grpc::ClientAsyncResponseReader< ::AppendResponse>* Consensus::Stub::PrepareAsyncappend_entriesRaw(::grpc::ClientContext* context, const ::AppendRequest& request, ::grpc::CompletionQueue* cq) {
  return ::grpc::internal::ClientAsyncResponseReaderHelper::Create< ::AppendResponse, ::AppendRequest, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(channel_.get(), cq, rpcmethod_append_entries_, context, request);
}

::grpc::ClientAsyncResponseReader< ::AppendResponse>* Consensus::Stub::Asyncappend_entriesRaw(::grpc::ClientContext* context, const ::AppendRequest& request, ::grpc::CompletionQueue* cq) {
  auto* result =
    this->PrepareAsyncappend_entriesRaw(context, request, cq);
  result->StartCall();
  return result;
}

Consensus::Service::Service() {
  AddMethod(new ::grpc::internal::RpcServiceMethod(
      Consensus_method_names[0],
      ::grpc::internal::RpcMethod::NORMAL_RPC,
      new ::grpc::internal::RpcMethodHandler< Consensus::Service, ::AppendRequest, ::AppendResponse, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(
          [](Consensus::Service* service,
             ::grpc::ServerContext* ctx,
             const ::AppendRequest* req,
             ::AppendResponse* resp) {
               return service->append_entries(ctx, req, resp);
             }, this)));
}

Consensus::Service::~Service() {
}

::grpc::Status Consensus::Service::append_entries(::grpc::ServerContext* context, const ::AppendRequest* request, ::AppendResponse* response) {
  (void) context;
  (void) request;
  (void) response;
  return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "");
}

