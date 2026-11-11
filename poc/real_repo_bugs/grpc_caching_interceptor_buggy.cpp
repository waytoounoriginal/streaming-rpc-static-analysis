// gRPC Official Example — Real Bug in CachingInterceptor::Intercept
// Source: examples/cpp/interceptors/caching_interceptor.h:83-102
// Initiates proxy stream_ = stub_->SayHelloStream(&context_), calls Write(), WritesDone(),
// drains responses with Read(), and sets Status::OK without ever calling stream_->Finish()!

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

    HelloRequest req;
    req.set_name("world");
    stream->Write(req);
    stream->WritesDone();

    HelloReply resp;
    while (stream->Read(&resp)) {
        // Cache response in map
    }

    // BUG (caching_interceptor.h:97): fabricates Status::OK without calling stream->Finish()!
    return Status::OK;
}
