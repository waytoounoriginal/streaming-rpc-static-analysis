#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>
#include "config_stream.grpc.pb.h"

using grpc::ClientContext;
using grpc::ClientReaderWriter;
using grpc::Status;
using minimal::config::ConfigStreamer;
using minimal::config::Frame;
using minimal::config::Summary;

// Level 2 Protocol Violation: sends chunk (case 2) before required InitConfig (case 1).
int RunBadOneofUploadClient(ConfigStreamer::Stub* stub, std::string* err_out) {
    ClientContext context;
    std::unique_ptr<ClientReaderWriter<Frame, Summary>> stream = stub->Upload(&context);
    if (!stream) {
        return -1;
    }

    Frame bad_frame;
    bad_frame.set_chunk("RAW_BYTES_BEFORE_INIT");
    stream->Write(bad_frame); // Violation: Level 2 oneof expected case 1 (init), got case 2 (chunk)

    stream->WritesDone();
    Summary summary;
    while (stream->Read(&summary)) {}
    Status status = stream->Finish();
    if (err_out) {
        *err_out = status.error_message();
    }
    return static_cast<int>(status.error_code());
}
