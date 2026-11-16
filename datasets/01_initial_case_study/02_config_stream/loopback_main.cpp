#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include <grpcpp/grpcpp.h>
#include "config_stream.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReaderWriter;
using grpc::Status;
using minimal::config::ConfigStreamer;
using minimal::config::Frame;
using minimal::config::Summary;

struct ConfigUploadResult {
    bool ok = false;
    int grpc_code = -1;
    int chunks_received = 0;
    int total_bytes = 0;
    std::string note;
};

ConfigUploadResult RunConfigUploadClient(ConfigStreamer::Stub* stub,
                                         const std::vector<std::string>& chunks);
int RunBadOneofUploadClient(ConfigStreamer::Stub* stub, std::string* err_out);
bool RunBadPostCloseUploadClient(ConfigStreamer::Stub* stub, int* grpc_code_out);
Status RunConfigUploadServer(ServerContext* context,
                             ServerReaderWriter<Summary, Frame>* stream);

class ConfigStreamerImpl final : public ConfigStreamer::Service {
public:
    Status Upload(ServerContext* context,
                  ServerReaderWriter<Summary, Frame>* stream) override {
        return RunConfigUploadServer(context, stream);
    }
};

int main() {
    ConfigStreamerImpl service;
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
    auto stub = ConfigStreamer::NewStub(channel);

    // 1. Conforming Client + Dual Server
    auto r1 = RunConfigUploadClient(stub.get(), {"AUDIO_01", "AUDIO_02"});
    std::cout << "[Ex2_Good] ok=" << r1.ok << " code=" << r1.grpc_code
              << " chunks=" << r1.chunks_received << " bytes=" << r1.total_bytes
              << " note=\"" << r1.note << "\"\n";

    // 2. Bad Level-2 oneof Client (chunk before init -> INVALID_ARGUMENT=3)
    std::string err_msg;
    int bad_oneof_code = RunBadOneofUploadClient(stub.get(), &err_msg);
    std::cout << "[Ex2_BadOneofClient] grpc_code=" << bad_oneof_code
              << " msg=\"" << err_msg << "\"\n";

    // 3. Bad Post-WritesDone Write() Client (Write() returns false at runtime)
    int post_close_code = -1;
    bool write_after_done_ok = RunBadPostCloseUploadClient(stub.get(), &post_close_code);
    std::cout << "[Ex2_BadPostCloseClient] write_after_WritesDone_ok=" << write_after_done_ok
              << " grpc_code=" << post_close_code << "\n";

    server->Shutdown();
    server->Wait();

    bool pass = r1.ok && r1.chunks_received == 2 && r1.total_bytes == 16
                && bad_oneof_code == grpc::StatusCode::INVALID_ARGUMENT
                && !write_after_done_ok;
    return pass ? 0 : 2;
}
