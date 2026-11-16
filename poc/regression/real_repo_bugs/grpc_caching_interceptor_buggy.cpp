// gRPC Official Example — Real Bug in CachingInterceptor::Intercept
// Source: examples/cpp/interceptors/caching_interceptor.h:39-108 (keyvaluestore::KeyValueStore::GetValues)
// On cache miss in PRE_SEND_MESSAGE: performs single stream_->Write(req) + stream_->Read(&resp)
// without checking return values; in PRE_SEND_CLOSE: calls stream_->WritesDone();
// in PRE_RECV_STATUS: overwrites *status = grpc::Status::OK without EVER calling stream_->Finish()!

#include <memory>
#include <grpcpp/grpcpp.h>
#include "real_repos.grpc.pb.h"

using grpc::ClientContext;
using grpc::ClientReaderWriter;
using grpc::Status;
using real_repos::Greeter;
using real_repos::HelloReply;
using real_repos::HelloRequest;

Status RunGrpcCachingInterceptorBuggy(Greeter::Stub* stub) {
    ClientContext context;
    std::unique_ptr<ClientReaderWriter<HelloRequest, HelloReply>> stream =
        stub->SayHelloStream(&context);
    if (!stream) {
        return Status::CANCELLED;
    }

    // Mirrors PRE_SEND_MESSAGE cache miss (caching_interceptor.h:83-89)
    HelloRequest req;
    req.set_name("key1");
    stream->Write(req);

    HelloReply resp;
    stream->Read(&resp);

    // Mirrors PRE_SEND_CLOSE (caching_interceptor.h:90-92) + PRE_RECV_STATUS (lines 101-105)
    // BUG: Fabricates Status::OK without calling stream->Finish()!
    stream->WritesDone();
    return Status::OK;
}
