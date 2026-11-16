#include <chrono>
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

// Level 1 Protocol Violation: calls Read() before sending initial HandshakeMsg.
int RunBadSwapClient(HandshakeService::Stub* stub) {
    ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(200));
    std::unique_ptr<ClientReaderWriter<HandshakeMsg, AckMsg>> stream = stub->Exchange(&context);
    if (!stream) {
        return -1;
    }

    AckMsg ack;
    stream->Read(&ack); // Violation: Illegal Read() in S0 (expected Write first)

    HandshakeMsg init;
    init.set_tag("TOO_LATE");
    stream->Write(init);
    stream->WritesDone();
    Status status = stream->Finish();
    return static_cast<int>(status.error_code());
}
