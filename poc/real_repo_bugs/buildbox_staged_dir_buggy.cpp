// BuildBox Common — Real Bug in GrpcCASClient::stage & ~GrpcStagedDirectory()
// Source: common/buildboxcommon_grpccasclient.cpp:1311-1370
// Two-phase bidirectional StageTree RPC:
//   Phase 1 (GrpcCASClient::stage): Write(stage_req) -> Read(&stage_resp)
//   Phase 2 (~GrpcStagedDirectory): Write(StageTreeRequest()) -> WritesDone()
// BUG (lines 1317-1323): ~GrpcStagedDirectory() exits after WritesDone() without calling
// the second Read(&cleanup_resp) (sent at casd/buildboxcasd_localcasinstance.cpp:634) or Finish()!

#include <memory>
#include <grpcpp/grpcpp.h>
#include "real_repos.grpc.pb.h"

using grpc::ClientContext;
using grpc::ClientReaderWriter;
using grpc::Status;
using real_repos::LocalContentAddressableStorage;
using real_repos::StageTreeRequest;
using real_repos::StageTreeResponse;

void RunBuildBoxStageTreeBuggy(LocalContentAddressableStorage::Stub* stub) {
    ClientContext context;
    std::unique_ptr<ClientReaderWriter<StageTreeRequest, StageTreeResponse>> stream =
        stub->StageTree(&context);
    if (!stream) {
        return;
    }

    // Phase 1: GrpcCASClient::stage (lines 1344-1356)
    StageTreeRequest stage_req;
    stage_req.set_staging_path("/tmp/staged");
    if (!stream->Write(stage_req)) {
        return;
    }

    StageTreeResponse stage_resp;
    stream->Read(&stage_resp);

    // Phase 2: GrpcStagedDirectory::~GrpcStagedDirectory() (lines 1317-1323)
    StageTreeRequest cleanup_req;
    stream->Write(cleanup_req);
    stream->WritesDone();
    // BUG: Destructor ends here in state S4_WAIT_CLEANUP_ACK!
    // Missing second stream->Read(&cleanup_resp) and missing stream->Finish()!
}
