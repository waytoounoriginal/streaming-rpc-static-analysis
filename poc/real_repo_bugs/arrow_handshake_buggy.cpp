// Apache Arrow Flight — Residual Bugs in GrpcClientImpl::AuthenticateInternal
// Source: cpp/src/arrow/flight/transport/grpc/grpc_client.cc:1067-1091
// Normal completion calls stream->Finish() at line 1089, but two error exits omit Finish():
//   - 1E-R1 (line 1074): auth_handler_->Authenticate(&outgoing, &incoming) failure returns early
//   - 1E-R2 (lines 1078-1081): !finished_writes returns early before reaching line 1089

#include <memory>
#include <grpcpp/grpcpp.h>
#include "real_repos.grpc.pb.h"

using grpc::ClientContext;
using grpc::ClientReaderWriter;
using grpc::Status;
using real_repos::FlightService;
using real_repos::HandshakeRequest;
using real_repos::HandshakeResponse;

Status AuthenticateChallenge(bool challenge_accepted) {
    return challenge_accepted ? Status::OK : Status(grpc::StatusCode::UNAUTHENTICATED, "rejected");
}

Status RunArrowHandshakeBuggy(FlightService::Stub* stub, bool challenge_accepted) {
    ClientContext context;
    std::unique_ptr<ClientReaderWriter<HandshakeRequest, HandshakeResponse>> stream =
        stub->Handshake(&context);
    if (!stream) {
        return Status::CANCELLED;
    }

    HandshakeRequest req;
    req.set_protocol_version(1);
    if (!stream->Write(req)) {
        return stream->Finish();
    }

    HandshakeResponse resp;
    if (!stream->Read(&resp)) {
        return stream->Finish();
    }

    // 1E-R1 (grpc_client.cc:1074): RETURN_NOT_OK(auth_handler_->Authenticate(...)) exits
    // after clean Write()/Read() without calling WritesDone() or stream->Finish()!
    Status auth_status = AuthenticateChallenge(challenge_accepted);
    if (!auth_status.ok()) {
        return auth_status;
    }

    // 1E-R2 (grpc_client.cc:1078-1081): !finished_writes returns early without Finish()
    bool finished_writes = stream->WritesDone();
    if (!finished_writes) {
        return Status(grpc::StatusCode::INTERNAL, "unfinished writes");
    }

    // Main path (grpc_client.cc:1089) DOES invoke Finish()
    return stream->Finish();
}
