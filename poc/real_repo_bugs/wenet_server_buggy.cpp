// WeNet ASR Server — Real Empty-Stream Bug in GrpcConnectionHandler::operator()
// Source: runtime/core/grpc/grpc_server.cc:52, 145-155
// Server endpoint dual session type: S0_WAIT_CONFIG -> S1_PIPELINE_INITIALIZED.
// When a client connects and closes the stream with 0 messages, while (stream->Read(&req))
// exits immediately in state S0_WAIT_CONFIG, and OnFinish() writes final recognition results
// (crashing on CHECK(feature_pipeline_ != nullptr)) while still in S0_WAIT_CONFIG!

#include <grpcpp/grpcpp.h>
#include "../wenet.grpc.pb.h"

using grpc::ServerContext;
using grpc::ServerReaderWriter;
using grpc::Status;
using wenet::Request;
using wenet::Response;

Status RunWeNetServerEmptyStreamBug(ServerContext* context,
                                    ServerReaderWriter<Response, Request>* stream) {
    Request req;
    while (stream->Read(&req)) {
        // If 0 messages are sent by client, this loop body NEVER executes,
        // leaving `stream` in S0_WAIT_CONFIG (pipeline uninitialized)!
    }

    // BUG (grpc_server.cc:52, 149): OnFinish() executes unconditionally after the Read loop,
    // attempting to finalize recognition and Write() final Response in S0_WAIT_CONFIG!
    Response final_resp;
    final_resp.set_type(Response::SPEECH_END);
    stream->Write(final_resp);
    return Status::OK;
}
