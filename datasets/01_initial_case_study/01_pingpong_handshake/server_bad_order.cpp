#include <string>

#include <grpcpp/grpcpp.h>
#include "handshake.grpc.pb.h"

using grpc::ServerContext;
using grpc::ServerReaderWriter;
using grpc::Status;
using minimal::handshake::AckMsg;
using minimal::handshake::HandshakeMsg;

// Dual Level 1 Server Violation: writes AckMsg before reading client's initial HandshakeMsg.
Status RunBadOrderServer(ServerContext* /*context*/,
                         ServerReaderWriter<AckMsg, HandshakeMsg>* stream) {
    AckMsg ack;
    ack.set_message("PREMATURE_ACK");
    stream->Write(ack); // Violation: Illegal Write() in server S0 (expected Read first)

    HandshakeMsg req;
    while (stream->Read(&req)) {}
    return Status::OK;
}
