// Google Cloud C++ Storage — Conforming Single-Loop Field Progression
// Source: google/cloud/storage/internal/grpc/stub.cc:328-400 (InsertObjectMedia / BidiWriteObject)
// Populates mutable_write_object_spec() before the loop (case 1), writes first chunk in
// S0_FIRST_CHUNK -> S1_SUBSEQUENT_CHUNKS, then calls clear_write_object_spec() (case 2)
// at the bottom of the loop so subsequent loop iterations satisfy S1_SUBSEQUENT_CHUNKS!

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

Status RunGCloudStorageConforming(Storage::Stub* stub) {
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
        if (!stream->Write(req)) {
            return Status::CANCELLED;
        }
        // Crucial single-loop progression (stub.cc:374): clear write_object_spec after 1st write!
        req.clear_write_object_spec();
    }

    stream->WritesDone();
    BidiWriteObjectResponse resp;
    stream->Read(&resp);
    Status status = stream->Finish();
    return status;
}
