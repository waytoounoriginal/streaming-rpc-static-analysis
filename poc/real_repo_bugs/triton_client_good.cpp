// Triton Inference Server C++ Client — Conforming Stream Lifecycle
// Calls StartStream (ModelStreamInfer), writes requests, reads responses,
// and cleanly terminates via StopStream (WritesDone + Finish) with no use-after-stop.

#include <memory>
#include <grpcpp/grpcpp.h>
#include "real_repos.grpc.pb.h"

using grpc::ClientContext;
using grpc::ClientReaderWriter;
using grpc::Status;
using real_repos::GRPCInferenceService;
using real_repos::ModelInferRequest;
using real_repos::ModelStreamInferResponse;

Status RunTritonClientConforming(GRPCInferenceService::Stub* stub) {
    ClientContext context;
    std::unique_ptr<ClientReaderWriter<ModelInferRequest, ModelStreamInferResponse>> stream =
        stub->ModelStreamInfer(&context);
    if (!stream) {
        return Status::CANCELLED;
    }

    ModelInferRequest req;
    req.set_model_name("resnet50");
    if (!stream->Write(req)) {
        return Status::CANCELLED;
    }

    ModelStreamInferResponse resp;
    while (stream->Read(&resp)) {
        // Process inference response
    }

    stream->WritesDone();
    Status status = stream->Finish();
    return status;
}
