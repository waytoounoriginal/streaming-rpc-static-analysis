#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>
#include "handshake.grpc.pb.h"

using grpc::ClientContext;
using grpc::ClientReaderWriter;
using grpc::Status;
using minimal::handshake::AckMsg;
using minimal::handshake::HandshakeMsg;
using minimal::handshake::HandshakeService;

// Level 1 Protocol Violation: early return leaks stream in S2 without WritesDone()/Finish().
int RunBadLeakClient(HandshakeService::Stub* stub, bool abort_early) {
    ClientContext context;
    std::unique_ptr<ClientReaderWriter<HandshakeMsg, AckMsg>> stream = stub->Exchange(&context);
    if (!stream) {
        return 1;
    }

    HandshakeMsg init;
    init.set_tag("HELLO");
    if (!stream->Write(init)) {
        return 1;
    }

    AckMsg ack;
    if (!stream->Read(&ack)) {
        return 1;
    }

    if (abort_early) {
        return 2; // Violation: stream destroyed in S2 without WritesDone() / Finish()
    }

    stream->WritesDone();
    AckMsg summary;
    if (!stream->Read(&summary)) {
        return 1;
    }
    Status status = stream->Finish();
    return status.ok() ? 0 : 1;
}
