#include <string>

#include <grpcpp/grpcpp.h>
#include "handshake.grpc.pb.h"

using grpc::ServerContext;
using grpc::ServerReaderWriter;
using grpc::Status;
using grpc::StatusCode;
using minimal::handshake::AckMsg;
using minimal::handshake::HandshakeMsg;

// Dual Level 1 Server Violation: omits final summary Write() before returning Status::OK.
Status RunBadMissingSummaryServer(ServerContext* /*context*/,
                                  ServerReaderWriter<AckMsg, HandshakeMsg>* stream) {
    HandshakeMsg init;
    if (!stream->Read(&init)) {
        return Status(StatusCode::INVALID_ARGUMENT, "missing initial HandshakeMsg");
    }

    AckMsg ack;
    ack.set_message("ACK:" + init.tag());
    if (!stream->Write(ack)) {
        return Status(StatusCode::ABORTED, "failed sending ack");
    }

    HandshakeMsg item;
    while (stream->Read(&item)) {}

    // Violation: returns OK in S2 without sending final summary AckMsg (S3 terminal)!
    return Status::OK;
}
