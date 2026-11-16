#include <atomic>
#include <chrono>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <grpcpp/grpcpp.h>
#include "handshake.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReaderWriter;
using grpc::Status;
using minimal::handshake::AckMsg;
using minimal::handshake::HandshakeMsg;
using minimal::handshake::HandshakeService;

struct HandshakeClientResult {
    bool ok = false;
    int grpc_code = -1;
    bool got_ack = false;
    bool got_summary = false;
    int summary_count = 0;
    std::string summary_msg;
};

HandshakeClientResult RunHandshakeClient(HandshakeService::Stub* stub,
                                         const std::vector<std::string>& items);
int RunBadSwapClient(HandshakeService::Stub* stub);
int RunBadLeakClient(HandshakeService::Stub* stub, bool abort_early);

Status RunHandshakeServer(ServerContext* context,
                          ServerReaderWriter<AckMsg, HandshakeMsg>* stream);
Status RunBadOrderServer(ServerContext* context,
                         ServerReaderWriter<AckMsg, HandshakeMsg>* stream);
Status RunBadMissingSummaryServer(ServerContext* context,
                                  ServerReaderWriter<AckMsg, HandshakeMsg>* stream);

enum class ServerMode { Good, BadOrder, BadMissingSummary };

class HandshakeServiceImpl final : public HandshakeService::Service {
public:
    std::atomic<ServerMode> mode{ServerMode::Good};
    std::atomic<bool> last_server_cancelled{false};

    Status Exchange(ServerContext* context,
                    ServerReaderWriter<AckMsg, HandshakeMsg>* stream) override {
        Status s;
        switch (mode.load()) {
            case ServerMode::Good:
                s = RunHandshakeServer(context, stream);
                break;
            case ServerMode::BadOrder:
                s = RunBadOrderServer(context, stream);
                break;
            case ServerMode::BadMissingSummary:
                s = RunBadMissingSummaryServer(context, stream);
                break;
        }
        last_server_cancelled.store(context->IsCancelled());
        return s;
    }
};

int main() {
    HandshakeServiceImpl service;
    int port = 0;
    ServerBuilder builder;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    if (!server || port == 0) {
        std::cerr << "Failed to start loopback server\n";
        return 1;
    }

    auto channel = grpc::CreateChannel("127.0.0.1:" + std::to_string(port),
                                       grpc::InsecureChannelCredentials());
    auto stub = HandshakeService::NewStub(channel);

    // 1. Conforming Client + Dual Server
    service.mode.store(ServerMode::Good);
    auto r1 = RunHandshakeClient(stub.get(), {"A", "B"});
    std::cout << "[Ex1_Good] ok=" << r1.ok << " code=" << r1.grpc_code
              << " got_ack=" << r1.got_ack << " got_summary=" << r1.got_summary
              << " count=" << r1.summary_count << "\n";

    // 2. Bad Swap Client (Read before Write -> 200ms DEADLINE_EXCEEDED=4)
    service.mode.store(ServerMode::Good);
    int swap_code = RunBadSwapClient(stub.get());
    std::cout << "[Ex1_BadSwapClient] grpc_code=" << swap_code
              << " (4=DEADLINE_EXCEEDED deadlock)\n";

    // 3. Bad Leak Client (early return without WritesDone/Finish -> server IsCancelled)
    service.mode.store(ServerMode::Good);
    service.last_server_cancelled.store(false);
    int leak_rc = RunBadLeakClient(stub.get(), true);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    std::cout << "[Ex1_BadLeakClient] ret=" << leak_rc
              << " server_saw_cancelled=" << service.last_server_cancelled.load() << "\n";

    // 4. Bad Server Missing Summary (Client receives got_summary=0)
    service.mode.store(ServerMode::BadMissingSummary);
    auto r4 = RunHandshakeClient(stub.get(), {"A"});
    std::cout << "[Ex1_BadServerMissingSummary] client_ok=" << r4.ok
              << " code=" << r4.grpc_code << " got_ack=" << r4.got_ack
              << " got_summary=" << r4.got_summary << "\n";

    server->Shutdown();
    server->Wait();

    bool pass = r1.ok && r1.summary_count == 2 && swap_code == grpc::StatusCode::DEADLINE_EXCEEDED
                && leak_rc == 2 && !r4.got_summary;
    return pass ? 0 : 2;
}
