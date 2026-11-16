// Google Cloud C++ Storage — Buggy Loop Missing clear_write_object_spec()
// Caught via k=2 bounded loop unrolling:
//   Iteration 1: Write(req) has case 1 (write_object_spec_populated), transitioning S0_FIRST_CHUNK -> S1_SUBSEQUENT_CHUNKS.
//   Iteration 2: Because req.clear_write_object_spec() was omitted, req STILL has case 1 in S1_SUBSEQUENT_CHUNKS!

#include <memory>
#include <vector>
#include <string>
#include <grpcpp/grpcpp.h>
#include "real_repos.grpc.pb.h"

using grpc::ClientContext;
using grpc::ClientReaderWriter;
using grpc::Status;
using real_repos::BidiWriteObjectRequest;
using real_repos::BidiWriteObjectResponse;
using real_repos::Storage;

Status RunGCloudStorageBuggyLoop(Storage::Stub* stub) {
    ClientContext context;
    std::unique_ptr<ClientReaderWriter<BidiWriteObjectRequest, BidiWriteObjectResponse>> stream =
        stub->BidiWriteObject(&context);
    if (!stream) {
        return Status::CANCELLED;
    }

    BidiWriteObjectRequest req;
    req.mutable_write_object_spec()->set_bucket("my-bucket");
    req.mutable_write_object_spec()->set_object_name("data.bin");

    std::vector<std::string> chunks = {"chunk_1", "chunk_2", "chunk_3"};
    for (const auto& chunk : chunks) {
        req.set_checksummed_data(chunk);
        // Iteration 1 succeeds (S0 -> S1), Iteration 2 FAILS (still has write_object_spec in S1)!
        stream->Write(req);
        // BUG: Omitted req.clear_write_object_spec()!
    }

    stream->WritesDone();
    BidiWriteObjectResponse resp;
    stream->Read(&resp);
    Status status = stream->Finish();
    return status;
}
