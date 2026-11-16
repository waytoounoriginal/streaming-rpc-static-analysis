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

// Level 1 Protocol Violation: calls Write() after WritesDone() (in state S2).
bool RunBadPostCloseUploadClient(ConfigStreamer::Stub* stub, int* grpc_code_out) {
    ClientContext context;
    std::unique_ptr<ClientReaderWriter<Frame, Summary>> stream = stub->Upload(&context);
    if (!stream) {
        return false;
    }

    Frame init_frame;
    init_frame.mutable_init()->set_batch_size(1);
    init_frame.mutable_init()->set_codec("pcm16");
    if (!stream->Write(init_frame)) {
        return false;
    }

    stream->WritesDone();

    Frame late_frame;
    late_frame.set_chunk("AFTER_WRITES_DONE");
    bool write_after_done_ok = stream->Write(late_frame); // Violation: Write() after WritesDone() in S2!

    Summary summary;
    stream->Read(&summary);
    Status status = stream->Finish();
    if (grpc_code_out) {
        *grpc_code_out = static_cast<int>(status.error_code());
    }
    return write_after_done_ok;
}
