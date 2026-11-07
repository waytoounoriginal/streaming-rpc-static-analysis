// Conforming client for WeNet ASR Streaming RPC
// Adheres strictly to the binary session type protocol:
// S0_CONFIG (!DecodeConfig) -> S1_STREAMING (!AudioChunk*) -> S2_WRITES_DONE (?Response*) -> S3_FINISHED (end)

#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include <grpcpp/grpcpp.h>
#include "wenet.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReaderWriter;
using grpc::Status;
using wenet::ASR;
using wenet::Request;
using wenet::Response;

int RunConformingRecognition(ASR::Stub* stub) {
    ClientContext context;
    std::unique_ptr<ClientReaderWriter<Request, Response>> stream = stub->Recognize(&context);
    if (!stream) {
        std::cerr << "Failed to initiate Recognize stream\n";
        return 1;
    }

    // Step 1: Protocol state S0_CONFIG -> Send DecodeConfig first
    Request config_req;
    config_req.mutable_decode_config()->set_nbest_config(1);
    config_req.mutable_decode_config()->set_continuous_decoding_config(false);
    std::cout << "[Client] Sending initial DecodeConfig (case = "
              << config_req.RequestPayload_case() << ")...\n";
    if (!stream->Write(config_req)) {
        return 1;
    }

    // Step 2: Protocol state S1_STREAMING -> Send audio chunks
    std::vector<std::string> audio_chunks = {
        "RIFF_AUDIO_CHUNK_001",
        "RIFF_AUDIO_CHUNK_002",
        "RIFF_AUDIO_CHUNK_003"
    };

    for (const auto& chunk_bytes : audio_chunks) {
        Request audio_req;
        audio_req.set_audio_data(chunk_bytes);
        std::cout << "[Client] Sending audio chunk (case = "
                  << audio_req.RequestPayload_case() << ")...\n";
        if (!stream->Write(audio_req)) {
            break;
        }
    }

    // Step 3: Half-close the write stream -> transition to S2_WRITES_DONE
    std::cout << "[Client] Finished audio transmission, calling WritesDone()...\n";
    stream->WritesDone();

    // Step 4: Drain server responses
    Response response;
    while (stream->Read(&response)) {
        std::cout << "[Client] Received server response\n";
    }

    // Step 5: Finalize stream -> transition to S3_FINISHED (end)
    Status status = stream->Finish();
    std::cout << "[Client] Stream finalized, status = " << (status.ok() ? "OK" : "FAILED") << "\n";
    return 0;
}

int main() {
    std::cout << "=== Conforming WeNet ASR Streaming Client ===\n";
    std::shared_ptr<Channel> channel = grpc::CreateChannel(
        "localhost:10086", grpc::InsecureChannelCredentials());
    std::unique_ptr<ASR::Stub> stub = ASR::NewStub(channel);
    return RunConformingRecognition(stub.get());
}
