// Mutated client violating Level 2 oneof ordering invariant
// Sends audio_data immediately in S0_CONFIG instead of the required DecodeConfig!
// In WeNet ASR, this causes a server-side crash/error due to uninitialized pipeline.

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

int RunViolatingOrderingRecognition(ASR::Stub* stub) {
    ClientContext context;
    std::unique_ptr<ClientReaderWriter<Request, Response>> stream = stub->Recognize(&context);
    if (!stream) {
        return 1;
    }

    // PROTOCOL VIOLATION (Level 2):
    // In state S0_CONFIG, client sends audio_data (case 2) without sending DecodeConfig (case 1)!
    Request audio_req;
    audio_req.set_audio_data("PREMATURE_AUDIO_CHUNK_WITHOUT_CONFIG");
    std::cout << "[Client Bad Ordering] Sending audio chunk prematurely in S0 (case = "
              << audio_req.RequestPayload_case() << ")...\n";
    stream->Write(audio_req); // Bug: Level 2 protocol violation!

    stream->WritesDone();
    Response response;
    while (stream->Read(&response)) {}
    Status status = stream->Finish();
    return 0;
}

int main() {
    std::cout << "=== Mutated Client: Level 2 oneof Ordering Violation ===\n";
    std::shared_ptr<Channel> channel = grpc::CreateChannel(
        "localhost:10086", grpc::InsecureChannelCredentials());
    std::unique_ptr<ASR::Stub> stub = ASR::NewStub(channel);
    return RunViolatingOrderingRecognition(stub.get());
}
