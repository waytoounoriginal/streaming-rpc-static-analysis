// Apache Arrow Flight — Intended Upper Abstraction (GrpcResultStream<FlightInfo>)
// Source: cpp/src/arrow/flight/transport/grpc/grpc_client.cc:501-565
// Even when deserialization fails inside the read loop, GrpcResultStream invokes
// Finish() (which calls context.TryCancel() and stream->Finish()) before returning.

#include <memory>
#include <grpcpp/grpcpp.h>
#include "real_repos.grpc.pb.h"

using grpc::ClientContext;
using grpc::ClientReader;
using grpc::Status;
using real_repos::Criteria;
using real_repos::FlightInfo;
using real_repos::FlightService;

bool DeserializeFlightInfo(const FlightInfo& pb_info) {
    return pb_info.total_records() >= 0;
}

Status RunArrowGrpcResultStreamConforming(FlightService::Stub* stub) {
    ClientContext context;
    Criteria criteria;
    std::unique_ptr<ClientReader<FlightInfo>> stream = stub->ListFlights(&context, criteria);
    if (!stream) {
        return Status::CANCELLED;
    }

    FlightInfo pb_info;
    while (stream->Read(&pb_info)) {
        bool ok = DeserializeFlightInfo(pb_info);
        if (!ok) {
            // GrpcResultStream::Finish() drains/cancels and ALWAYS calls stream->Finish()
            context.TryCancel();
            Status err_status = stream->Finish();
            return err_status;
        }
    }

    // Normal completion: GrpcResultStream calls stream->Finish()
    Status status = stream->Finish();
    return status;
}
