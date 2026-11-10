// BuildBox Common — Fixed 2-Phase StageTree Lifecycle
// Properly completes Phase 2 cleanup by reading the server's cleanup acknowledgment
// (StageTreeResponse) and calling Finish() before destroying the stream.

#include <memory>
#include <grpcpp/grpcpp.h>
#include "real_repos.grpc.pb.h"

using grpc::ClientContext;
using grpc::ClientReaderWriter;
using grpc::Status;
using real_repos::LocalContentAddressableStorage;
using real_repos::StageTreeRequest;
using real_repos::StageTreeResponse;

Status RunBuildBoxStageTreeFixed(LocalContentAddressableStorage::Stub* stub) {
    ClientContext context;
    std::unique_ptr<ClientReaderWriter<StageTreeRequest, StageTreeResponse>> stream =
        stub->StageTree(&context);
    if (!stream) {
        return Status::CANCELLED;
    }

    // Phase 1: Stage request + response
    StageTreeRequest stage_req;
    stage_req.set_staging_path("/tmp/staged");
    if (!stream->Write(stage_req)) {
        return Status::CANCELLED;
    }

    StageTreeResponse stage_resp;
    stream->Read(&stage_resp);

    // Phase 2: Cleanup request + WritesDone + second Read(ack) + Finish()
    StageTreeRequest cleanup_req;
    stream->Write(cleanup_req);
    stream->WritesDone();

    StageTreeResponse cleanup_resp;
    stream->Read(&cleanup_resp);
    Status status = stream->Finish();
    return status;
}
