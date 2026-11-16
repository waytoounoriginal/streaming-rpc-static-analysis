#include <memory>
#include <string>
#include <vector>

#include <grpcpp/grpcpp.h>
#include "config_stream.grpc.pb.h"

using grpc::ClientContext;
using grpc::ClientReaderWriter;
using grpc::Status;
using minimal::config::ConfigStreamer;
using minimal::config::Frame;
using minimal::config::Summary;

struct ConfigUploadResult {
    bool ok = false;
    int grpc_code = -1;
    int chunks_received = 0;
    int total_bytes = 0;
    std::string note;
};

ConfigUploadResult RunConfigUploadClient(ConfigStreamer::Stub* stub,
                                         const std::vector<std::string>& chunks) {
    ConfigUploadResult res;
    ClientContext context;
    std::unique_ptr<ClientReaderWriter<Frame, Summary>> stream = stub->Upload(&context);
    if (!stream) {
        return res;
    }

    // State S0 -> S1: Must send InitConfig (oneof body case 1: init) first
    Frame init_frame;
    init_frame.mutable_init()->set_batch_size(static_cast<int>(chunks.size()));
    init_frame.mutable_init()->set_codec("pcm16");
    if (!stream->Write(init_frame)) {
        Status s = stream->Finish();
        res.grpc_code = static_cast<int>(s.error_code());
        return res;
    }

    // State S1 -> S1: Stream audio/byte chunks (oneof body case 2: chunk)
    for (const auto& payload : chunks) {
        Frame data_frame;
        data_frame.set_chunk(payload);
        if (!stream->Write(data_frame)) {
            break;
        }
    }

    // State S1 -> S2 -> S3 -> S4
    stream->WritesDone();

    Summary summary;
    bool got_summary = stream->Read(&summary);
    Status status = stream->Finish();

    res.grpc_code = static_cast<int>(status.error_code());
    res.ok = status.ok() && got_summary;
    if (got_summary) {
        res.chunks_received = summary.chunks_received();
        res.total_bytes = summary.total_bytes();
        res.note = summary.status_note();
    } else {
        res.note = status.error_message();
    }
    return res;
}
