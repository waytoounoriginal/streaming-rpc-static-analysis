#include <string>

#include <grpcpp/grpcpp.h>
#include "handshake.grpc.pb.h"

using grpc::ServerContext;
using grpc::ServerReaderWriter;
using grpc::Status;
using grpc::StatusCode;
using minimal::handshake::AckMsg;
using minimal::handshake::HandshakeMsg;

Status RunHandshakeServer(ServerContext* /*context*/,
                          ServerReaderWriter<AckMsg, HandshakeMsg>* stream) {
    HandshakeMsg init;
    if (!stream->Read(&init)) {
        return Status(StatusCode::INVALID_ARGUMENT, "missing initial HandshakeMsg");
    }

    AckMsg ack;
    ack.set_message("ACK:" + init.tag());
    ack.set_count(0);
    if (!stream->Write(ack)) {
        return Status(StatusCode::ABORTED, "failed sending handshake ack");
    }

    int items = 0;
    HandshakeMsg item;
    while (stream->Read(&item)) {
        ++items;
    }

    AckMsg summary;
    summary.set_message("SUMMARY");
    summary.set_count(items);
    stream->Write(summary);
    return Status::OK;
}
