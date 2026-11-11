// Triton Inference Server C++ Client — Real Bugs in InferenceServerGrpcClient
// Source: src/c++/library/grpc_client.cc:1346-1460
// Defect 1 (line 1447): AsyncStreamInfer() calls grpc_stream_->Write(req) without checking
//   if StartStream() was called (stream is in S_UNINITIALIZED).
// Defect 2 (lines 1402-1420): StopStream() calls WritesDone() and Finish() (reaching S2_FINISHED)
//   but forgets grpc_stream_.reset(), so a subsequent AsyncStreamInfer() calls Write(req)
//   in terminal state S2_FINISHED!

#include <memory>
#include <grpcpp/grpcpp.h>
#include "real_repos.grpc.pb.h"

using grpc::ClientContext;
using grpc::ClientReaderWriter;
using grpc::Status;
using real_repos::GRPCInferenceService;
using real_repos::ModelInferRequest;
using real_repos::ModelStreamInferResponse;

Status RunTritonClientLifecycleBugs(GRPCInferenceService::Stub* stub) {
    ClientContext context;
    // StartStream() initializes grpc_stream_
    std::unique_ptr<ClientReaderWriter<ModelInferRequest, ModelStreamInferResponse>> stream =
        stub->ModelStreamInfer(&context);
    if (!stream) {
        return Status::CANCELLED;
    }

    // Valid AsyncStreamInfer() call while stream is active
    ModelInferRequest req1;
    req1.set_model_name("resnet50");
    stream->Write(req1);

    ModelStreamInferResponse resp;
    stream->Read(&resp);

    // StopStream() (grpc_client.cc:1402-1420): calls WritesDone() and Finish(),
    // transitioning stream to S2_FINISHED, but does NOT reset `stream`!
    stream->WritesDone();
    Status status = stream->Finish();

    // BUG (grpc_client.cc:1447): Calling AsyncStreamInfer() after StopStream()
    // invokes stream->Write(req2) in terminal state S2_FINISHED!
    ModelInferRequest req2;
    req2.set_model_name("resnet50_after_stop");
    stream->Write(req2);

    return status;
}
