#include <string>

#include <grpcpp/grpcpp.h>
#include "config_stream.grpc.pb.h"

using grpc::ServerContext;
using grpc::ServerReaderWriter;
using grpc::Status;
using grpc::StatusCode;
using minimal::config::Frame;
using minimal::config::Summary;

Status RunConfigUploadServer(ServerContext* /*context*/,
                             ServerReaderWriter<Summary, Frame>* stream) {
    Frame first;
    if (!stream->Read(&first)) {
        return Status(StatusCode::INVALID_ARGUMENT, "empty Upload stream");
    }
    if (!first.has_init()) {
        return Status(StatusCode::INVALID_ARGUMENT, "Level-2 contract violation: first Frame must contain init");
    }

    int chunks = 0;
    int total_bytes = 0;
    Frame frame;
    while (stream->Read(&frame)) {
        if (!frame.has_chunk()) {
            return Status(StatusCode::INVALID_ARGUMENT, "Level-2 contract violation: expected chunk in streaming phase");
        }
        ++chunks;
        total_bytes += static_cast<int>(frame.chunk().size());
    }

    Summary summary;
    summary.set_chunks_received(chunks);
    summary.set_total_bytes(total_bytes);
    summary.set_status_note("codec=" + first.init().codec());
    stream->Write(summary);
    return Status::OK;
}
