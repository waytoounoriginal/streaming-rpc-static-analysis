#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include <grpcpp/grpcpp.h>
#include "handshake.grpc.pb.h"

using grpc::ClientContext;
using grpc::ClientReaderWriter;
using grpc::Status;
using minimal::handshake::AckMsg;
using minimal::handshake::HandshakeMsg;
using minimal::handshake::HandshakeService;

struct HandshakeClientResult {
    bool ok = false;
    int grpc_code = -1;
    bool got_ack = false;
    bool got_summary = false;
    int summary_count = 0;
    std::string summary_msg;
};

HandshakeClientResult RunHandshakeClient(HandshakeService::Stub* stub,
                                         const std::vector<std::string>& items) {
    HandshakeClientResult res;
    ClientContext context;
    std::unique_ptr<ClientReaderWriter<HandshakeMsg, AckMsg>> stream = stub->Exchange(&context);
    if (!stream) {
        return res;
    }

    HandshakeMsg init;
    init.set_tag("HELLO");
    init.set_seq(0);
    if (!stream->Write(init)) {
        Status s = stream->Finish();
        res.grpc_code = static_cast<int>(s.error_code());
        return res;
    }

    AckMsg ack;
    if (!stream->Read(&ack)) {
        Status s = stream->Finish();
        res.grpc_code = static_cast<int>(s.error_code());
        return res;
    }
    res.got_ack = true;

    for (size_t i = 0; i < items.size(); ++i) {
        HandshakeMsg msg;
        msg.set_tag(items[i]);
        msg.set_seq(static_cast<int>(i + 1));
        if (!stream->Write(msg)) {
            break;
        }
    }

    stream->WritesDone();

    AckMsg summary;
    if (stream->Read(&summary)) {
        res.got_summary = true;
        res.summary_count = summary.count();
        res.summary_msg = summary.message();
    }

    Status status = stream->Finish();
    res.grpc_code = static_cast<int>(status.error_code());
    res.ok = status.ok() && res.got_ack && res.got_summary;
    return res;
}
