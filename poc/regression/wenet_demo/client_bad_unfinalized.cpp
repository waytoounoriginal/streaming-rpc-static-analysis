// Mutated client violating Level 1 stream lifecycle invariant
// Early return leaks the open gRPC stream without calling WritesDone() or Finish()!
// This leaves the HTTP/2 stream hung on the server until timeout.

#include <iostream>
#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>
#include "wenet.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReaderWriter;
using grpc::Status;
using wenet::ASR;
using wenet::Request;
using wenet::Response;

int RunUnfinalizedStreamRecognition(ASR::Stub* stub) {
    ClientContext context;
    std::unique_ptr<ClientReaderWriter<Request, Response>> stream = stub->Recognize(&context);
    if (!stream) {
        return 1;
    }

    Request config_req;
    config_req.mutable_decode_config()->set_nbest_config(1);
    stream->Write(config_req);

    Request audio_req;
    audio_req.set_audio_data("INTERRUPTED_AUDIO_CHUNK");
    bool write_ok = stream->Write(audio_req);

    if (write_ok) {
        std::cout << "[Client Bad Unfinalized] Early return without calling WritesDone() or Finish()!\n";
        // PROTOCOL VIOLATION (Level 1):
        // Stream goes out of scope here while in S1_STREAMING. Finish() was never invoked!
        return -1;
    }

    stream->WritesDone();
    Response response;
    while (stream->Read(&response)) {}
    stream->Finish();
    return 0;
}

int main() {
    std::cout << "=== Mutated Client: Level 1 Stream Lifecycle Leak ===\n";
    std::shared_ptr<Channel> channel = grpc::CreateChannel(
        "localhost:10086", grpc::InsecureChannelCredentials());
    std::unique_ptr<ASR::Stub> stub = ASR::NewStub(channel);
    return RunUnfinalizedStreamRecognition(stub.get());
}
