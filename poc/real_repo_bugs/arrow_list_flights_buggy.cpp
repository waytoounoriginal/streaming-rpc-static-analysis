// Apache Arrow Flight — Real Bug in GrpcClientImpl::ListFlights (Bypassing GrpcResultStream)
// Source: cpp/src/arrow/flight/transport/grpc/grpc_client.cc:892-941
// ListFlights uses raw grpc::ClientReader<pb::FlightInfo> directly instead of GrpcResultStream,
// and executes RETURN_NOT_OK(...) inside while (stream->Read(&pb_info)), returning early
// without calling stream->Finish()!

#include <memory>
#include <grpcpp/grpcpp.h>
#include "real_repos.grpc.pb.h"

using grpc::ClientContext;
using grpc::ClientReader;
using grpc::Status;
using real_repos::Criteria;
using real_repos::FlightInfo;
using real_repos::FlightService;

#define RETURN_NOT_OK(s)              \
    do {                              \
        Status _s = (s);              \
        if (!_s.ok()) {               \
            return _s;                \
        }                             \
    } while (false)

Status MakeFlightInfo(const FlightInfo& pb_info) {
    if (pb_info.total_records() < 0) {
        return Status::CANCELLED;
    }
    return Status::OK;
}

Status RunArrowListFlightsBuggy(FlightService::Stub* stub) {
    ClientContext context;
    Criteria criteria;
    std::unique_ptr<ClientReader<FlightInfo>> stream = stub->ListFlights(&context, criteria);
    if (!stream) {
        return Status::CANCELLED;
    }

    FlightInfo pb_info;
    while (stream->Read(&pb_info)) {
        // BUG (grpc_client.cc:926): Early return via RETURN_NOT_OK inside read loop
        // destroys `stream` in state S0_READING without calling stream->Finish()!
        RETURN_NOT_OK(MakeFlightInfo(pb_info));
    }

    Status status = stream->Finish();
    return status;
}
