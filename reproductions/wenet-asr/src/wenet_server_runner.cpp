#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include <grpcpp/grpcpp.h>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#endif

#include "decoder/asr_decoder.h"
#include "frontend/feature_pipeline.h"
#include "grpc/grpc_server.h"
#include "utils/log.h"

int main(int argc, char* argv[]) {
  int decode_delay_ms = 0;
  int chunk_samples = 800;
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--decode-delay-ms" && i + 1 < argc) {
      decode_delay_ms = std::stoi(argv[++i]);
    } else if (arg == "--chunk-samples" && i + 1 < argc) {
      chunk_samples = std::stoi(argv[++i]);
    }
  }

#ifdef _WIN32
  // Disable Windows modal crash popups so headless std::abort() terminates
  // cleanly with an abnormal exit code rather than blocking on a GUI dialog.
  SetErrorMode(SEM_FAILCRITICALERRORS | SEM_NOGPFAULTERRORBOX);
  _set_abort_behavior(0, _WRITE_ABORT_MSG | _CALL_REPORTFAULT);
#endif

  auto feature_config =
      std::make_shared<wenet::FeaturePipelineConfig>(80, 16000);
  auto decode_config = std::make_shared<wenet::DecodeOptions>();
  auto decode_resource = std::make_shared<wenet::DecodeResource>();
  decode_resource->chunk_samples = chunk_samples;
  decode_resource->decode_delay_ms = decode_delay_ms;

  wenet::GrpcServer service(feature_config, decode_config, decode_resource);

  grpc::ServerBuilder builder;
  int selected_port = 0;
  builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(),
                           &selected_port);
  builder.RegisterService(&service);

  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  if (!server || selected_port <= 0) {
    std::cerr << "Failed to start GrpcServer on 127.0.0.1:0" << std::endl;
    return 2;
  }

  std::cout << "LISTENING_PORT=" << selected_port << std::endl;
  std::cout.flush();

  // Wait for stdin EOF to trigger graceful server shutdown.
  std::thread shutdown_watcher([&server]() {
    std::string line;
    while (std::getline(std::cin, line)) {
      if (line == "SHUTDOWN") {
        break;
      }
    }
    server->Shutdown();
  });

  server->Wait();
  if (shutdown_watcher.joinable()) {
    shutdown_watcher.join();
  }
  return 0;
}
