// Apache Arrow Flight — Real Bug in GrpcClientImpl::AuthenticateInternal (Handshake)
// Source: cpp/src/arrow/flight/transport/grpc/grpc_client.cc:592-624
// Calls stub->Handshake(&context) (ClientReaderWriter), calls stream->WritesDone() at line 614,
// and returns Status::OK at line 623 without EVER calling stream->Finish()!

#include <memory>
#include <grpcpp/grpcpp.h>
#include "real_repos.grpc.pb.h"

using grpc::ClientContext;
using grpc::ClientReaderWriter;
using grpc::Status;
using real_repos::FlightService;
using real_repos::HandshakeRequest;
using real_repos::HandshakeResponse;

Status RunArrowHandshakeBuggy(FlightService::Stub* stub) {
    ClientContext context;
    std::unique_ptr<ClientReaderWriter<HandshakeRequest, HandshakeResponse>> stream =
        stub->Handshake(&context);
    if (!stream) {
        return Status::CANCELLED;
    }

    HandshakeRequest req;
    req.set_protocol_version(1);
    if (!stream->Write(req)) {
        return Status::CANCELLED;
    }

    HandshakeResponse resp;
    while (stream->Read(&resp)) {
        // Process server handshake challenge
    }

    // grpc_client.cc:614: calls WritesDone(), then returns Status::OK at line 623
    // WITHOUT calling stream->Finish()!
    stream->WritesDone();
    return Status::OK;
}
