#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <mutex>
#include <new>
#include <string>
#include <thread>
#include <vector>

#include <grpcpp/grpcpp.h>
#include "grpc_client.h"
#include "grpc_service.grpc.pb.h"

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#endif

namespace tc = triton::client;

// Global operator new hook used ONLY when g_poison_active / g_count_allocs are
// toggled around targeted probes. Allows deterministic observation of
// uninitialized member reads (0x00 vs 0xAA) without modifying upstream files.
static thread_local bool g_poison_active = false;
static thread_local uint8_t g_poison_byte = 0xAA;
static thread_local bool g_count_allocs = false;
static std::atomic<int> g_allocs_during_infer{0};

void* operator new(std::size_t sz) {
  void* p = std::malloc(sz);
  if (!p) throw std::bad_alloc();
  if (g_poison_active && sz >= sizeof(tc::InferenceServerGrpcClient)) {
    std::memset(p, g_poison_byte, sz);
  }
  if (g_count_allocs) {
    g_allocs_during_infer.fetch_add(1, std::memory_order_relaxed);
  }
  return p;
}

void operator delete(void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }

#ifdef _WIN32
static LONG WINAPI CrashFilter(EXCEPTION_POINTERS* ep) {
  const auto code = ep->ExceptionRecord->ExceptionCode;
  const uintptr_t addr =
      reinterpret_cast<uintptr_t>(ep->ExceptionRecord->ExceptionAddress);
  const uintptr_t mod_base =
      reinterpret_cast<uintptr_t>(GetModuleHandleA(nullptr));
  constexpr uintptr_t kPeImageBase = 0x140000000ULL;
  const uintptr_t addr_rva = (addr >= mod_base) ? (addr - mod_base + kPeImageBase) : addr;

  void* stack[16] = {};
  USHORT frames = RtlCaptureStackBackTrace(0, 16, stack, nullptr);

  std::cerr << "[SEH] ExceptionCode=0x" << std::hex << code
            << " FaultRVA=0x" << addr_rva
            << std::dec
            << " allocs_during_AsyncStreamInfer=" << g_allocs_during_infer.load()
            << "\n[SEH] StackRVAs:";
  for (USHORT i = 0; i < frames; ++i) {
    uintptr_t f = reinterpret_cast<uintptr_t>(stack[i]);
    if (f >= mod_base) {
      std::cerr << " 0x" << std::hex << (f - mod_base + kPeImageBase);
    }
  }
  std::cerr << std::dec << std::endl;
  std::cerr.flush();
  return EXCEPTION_EXECUTE_HANDLER;
}
#endif

class EchoTritonService final : public inference::GRPCInferenceService::Service {
 public:
  std::atomic<bool> hold_response{false};
  std::mutex hold_mu;
  std::condition_variable hold_cv;

  grpc::Status ModelStreamInfer(
      grpc::ServerContext* context,
      grpc::ServerReaderWriter<
          inference::ModelStreamInferResponse,
          inference::ModelInferRequest>* stream) override {
    inference::ModelInferRequest req;
    while (stream->Read(&req)) {
      if (hold_response.load()) {
        std::unique_lock<std::mutex> lk(hold_mu);
        hold_cv.wait_for(lk, std::chrono::milliseconds(400), [&] {
          return !hold_response.load() || context->IsCancelled();
        });
        if (context->IsCancelled()) {
          return grpc::Status::CANCELLED;
        }
      }
      inference::ModelStreamInferResponse resp;
      auto* infer_resp = resp.mutable_infer_response();
      infer_resp->set_model_name(req.model_name());
      infer_resp->set_model_version(req.model_version());
      infer_resp->set_id(req.id());
      (*infer_resp->mutable_parameters())["triton_final_response"].set_bool_param(true);
      if (!stream->Write(resp)) {
        break;
      }
    }
    return grpc::Status::OK;
  }
};

struct LoopbackServer {
  EchoTritonService service;
  std::unique_ptr<grpc::Server> server;
  int port = 0;

  void Start() {
    grpc::ServerBuilder builder;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
    builder.RegisterService(&service);
    server = builder.BuildAndStart();
  }

  std::string Addr() const { return "127.0.0.1:" + std::to_string(port); }

  ~LoopbackServer() {
    if (server) {
      server->Shutdown();
      server->Wait();
    }
  }
};

static tc::Error SendSimpleInfer(
    tc::InferenceServerGrpcClient* client, const std::string& id) {
  tc::InferOptions opts("echo_model");
  opts.request_id_ = id;
  tc::InferInput* raw_in = nullptr;
  tc::InferInput::Create(&raw_in, "INPUT0", {1}, "INT32");
  std::unique_ptr<tc::InferInput> in(raw_in);
  int32_t val = 42;
  in->AppendRaw(reinterpret_cast<const uint8_t*>(&val), sizeof(val));
  g_allocs_during_infer.store(0);
  g_count_allocs = true;
  tc::Error err = client->AsyncStreamInfer(opts, {in.get()});
  g_count_allocs = false;
  return err;
}

static int RunBaseline(const std::string& addr) {
  std::unique_ptr<tc::InferenceServerGrpcClient> client;
  auto err = tc::InferenceServerGrpcClient::Create(&client, addr);
  if (!err.IsOk()) return 1;

  std::mutex mu;
  std::condition_variable cv;
  std::vector<std::string> ids;
  auto cb = [&](tc::InferResult* res) {
    std::unique_ptr<tc::InferResult> own(res);
    std::string id;
    own->Id(&id);
    std::lock_guard<std::mutex> lk(mu);
    ids.push_back(id);
    cv.notify_all();
  };

  err = client->StartStream(cb, /*enable_stats=*/true);
  std::cout << "[0_baseline] StartStream ok=" << err.IsOk() << std::endl;
  err = SendSimpleInfer(client.get(), "base-1");
  std::cout << "[0_baseline] AsyncStreamInfer ok=" << err.IsOk() << std::endl;
  {
    std::unique_lock<std::mutex> lk(mu);
    cv.wait_for(lk, std::chrono::seconds(5), [&] { return ids.size() >= 1; });
  }
  err = client->StopStream(false);
  tc::InferStat st;
  client->ClientInferStat(&st);
  std::cout << "[0_baseline] StopStream ok=" << err.IsOk()
            << " completed=" << st.completed_request_count
            << " total_ms=" << (st.cumulative_total_request_time_ns / 1e6) << std::endl;
  return (err.IsOk() && ids.size() == 1 && st.completed_request_count == 1) ? 0 : 1;
}

static int RunGuards(const std::string& addr) {
  std::unique_ptr<tc::InferenceServerGrpcClient> client;
  tc::InferenceServerGrpcClient::Create(&client, addr);

  auto e_stop_before = client->StopStream(false);
  std::cout << "[1_guards] StopStream before StartStream: ok=" << e_stop_before.IsOk() << std::endl;

  auto e_null_cb = client->StartStream(nullptr, true);
  std::cout << "[1_guards] StartStream(nullptr): ok=" << e_null_cb.IsOk()
            << " msg='" << e_null_cb.Message() << "'" << std::endl;

  auto cb = [](tc::InferResult* res) { delete res; };
  auto e_s1 = client->StartStream(cb, true);
  auto e_s2 = client->StartStream(cb, true);
  std::cout << "[1_guards] StartStream(1): ok=" << e_s1.IsOk()
            << " | Double StartStream: ok=" << e_s2.IsOk()
            << " msg='" << e_s2.Message() << "'" << std::endl;

  auto e_stop1 = client->StopStream(false);
  auto e_stop2 = client->StopStream(false);
  std::cout << "[1_guards] StopStream(1): ok=" << e_stop1.IsOk()
            << " | Double StopStream: ok=" << e_stop2.IsOk() << std::endl;

  return (e_stop_before.IsOk() && !e_null_cb.IsOk() && e_s1.IsOk() && !e_s2.IsOk() &&
          e_stop1.IsOk() && e_stop2.IsOk())
             ? 0
             : 1;
}

static int Run2A(const std::string& addr, uint8_t poison_byte, const char* tag) {
  g_poison_byte = poison_byte;
  g_poison_active = true;
  std::unique_ptr<tc::InferenceServerGrpcClient> client;
  tc::InferenceServerGrpcClient::Create(
      &client, addr, false, false, tc::SslOptions(), tc::KeepAliveOptions(), false);
  g_poison_active = false;

  std::cout << "[" << tag << "] Calling AsyncStreamInfer BEFORE StartStream (heap fill=0x"
            << std::hex << static_cast<int>(poison_byte) << std::dec << ")..." << std::endl;
  std::cout.flush();
  tc::Error err = SendSimpleInfer(client.get(), "before-start");
  std::cout << "[" << tag << "] Returned cleanly: ok=" << err.IsOk()
            << " msg='" << err.Message() << "'"
            << " allocs_during_AsyncStreamInfer=" << g_allocs_during_infer.load()
            << std::endl;
  return err.IsOk() ? 1 : 0;
}

static int Run2BAfterStop(const std::string& addr, bool cancel_stop, const char* tag) {
  std::unique_ptr<tc::InferenceServerGrpcClient> client;
  tc::InferenceServerGrpcClient::Create(&client, addr);

  std::mutex mu;
  std::condition_variable cv;
  std::vector<std::string> ids;
  auto cb = [&](tc::InferResult* res) {
    std::unique_ptr<tc::InferResult> own(res);
    std::string id;
    own->Id(&id);
    std::lock_guard<std::mutex> lk(mu);
    ids.push_back(id);
    cv.notify_all();
  };

  client->StartStream(cb, /*enable_stats=*/true);
  SendSimpleInfer(client.get(), "s1-req1");
  {
    std::unique_lock<std::mutex> lk(mu);
    cv.wait_for(lk, std::chrono::seconds(5), [&] { return ids.size() >= 1; });
  }
  client->StopStream(cancel_stop);
  if (cancel_stop) {
    // Drain synthetic "Locally cancelled by application!" callback if emitted.
    std::unique_lock<std::mutex> lk(mu);
    cv.wait_for(lk, std::chrono::milliseconds(500), [&] { return ids.size() >= 2; });
  }

  tc::InferStat st1;
  client->ClientInferStat(&st1);
  std::cout << "[" << tag << "] After StopStream(" << (cancel_stop ? "true" : "false")
            << "): completed=" << st1.completed_request_count
            << " s1_ms=" << (st1.cumulative_total_request_time_ns / 1e6) << std::endl;

  tc::Error post_err = SendSimpleInfer(client.get(), "after-stop");
  std::cout << "[" << tag << "] AsyncStreamInfer AFTER StopStream: ok=" << post_err.IsOk()
            << " msg='" << post_err.Message() << "'"
            << " allocs_during_AsyncStreamInfer=" << g_allocs_during_infer.load()
            << std::endl;

  std::this_thread::sleep_for(std::chrono::milliseconds(80));
  ids.clear();
  auto s2_err = client->StartStream(cb, /*enable_stats=*/true);
  auto i2_err = SendSimpleInfer(client.get(), "s2-req1");
  {
    std::unique_lock<std::mutex> lk(mu);
    cv.wait_for(lk, std::chrono::seconds(5), [&] { return ids.size() >= 1; });
  }
  client->StopStream(false);

  tc::InferStat st2;
  client->ClientInferStat(&st2);
  uint64_t delta_ns = st2.cumulative_total_request_time_ns - st1.cumulative_total_request_time_ns;
  std::cout << "[" << tag << "] Session2 StartStream ok=" << s2_err.IsOk()
            << " AsyncStreamInfer ok=" << i2_err.IsOk()
            << " completed=" << st2.completed_request_count
            << " s2_delta_ms=" << (delta_ns / 1e6) << std::endl;
  return (!post_err.IsOk() && (delta_ns < 20000000ULL)) ? 0 : 1;
}

static int Run2DInflightCancel(EchoTritonService& svc, const std::string& addr) {
  std::unique_ptr<tc::InferenceServerGrpcClient> client;
  tc::InferenceServerGrpcClient::Create(&client, addr);

  std::mutex mu;
  std::condition_variable cv;
  std::vector<std::string> ids;
  auto cb = [&](tc::InferResult* res) {
    std::unique_ptr<tc::InferResult> own(res);
    std::string id;
    own->Id(&id);
    std::lock_guard<std::mutex> lk(mu);
    ids.push_back(id);
    cv.notify_all();
  };

  svc.hold_response.store(true);
  client->StartStream(cb, /*enable_stats=*/true);
  SendSimpleInfer(client.get(), "inflight-cancelled");
  std::this_thread::sleep_for(std::chrono::milliseconds(15));
  client->StopStream(/*cancel_requests=*/true);
  svc.hold_response.store(false);
  svc.hold_cv.notify_all();

  tc::InferStat st1;
  client->ClientInferStat(&st1);

  std::this_thread::sleep_for(std::chrono::milliseconds(80));
  ids.clear();
  client->StartStream(cb, /*enable_stats=*/true);
  SendSimpleInfer(client.get(), "s2-after-inflight-cancel");
  {
    std::unique_lock<std::mutex> lk(mu);
    cv.wait_for(lk, std::chrono::seconds(5), [&] { return ids.size() >= 1; });
  }
  client->StopStream(false);

  tc::InferStat st2;
  client->ClientInferStat(&st2);
  uint64_t delta_ns = st2.cumulative_total_request_time_ns - st1.cumulative_total_request_time_ns;
  std::cout << "[2d_inflight_cancel] Session2 completed=" << st2.completed_request_count
            << " s2_delta_ms=" << (delta_ns / 1e6) << std::endl;
  return (delta_ns < 20000000ULL) ? 0 : 1;
}

int main(int argc, char** argv) {
#ifdef _WIN32
  SetErrorMode(SEM_FAILCRITICALERRORS | SEM_NOGPFAULTERRORBOX);
  SetUnhandledExceptionFilter(CrashFilter);
#endif
  std::string mode = (argc > 1) ? argv[1] : "0_baseline";
  LoopbackServer srv;
  srv.Start();
  std::cout << "[server] listening on " << srv.Addr() << std::endl;

  if (mode == "0_baseline") return RunBaseline(srv.Addr());
  if (mode == "1_guards") return RunGuards(srv.Addr());
  if (mode == "2a_zero") return Run2A(srv.Addr(), 0x00, "2a_zero");
  if (mode == "2a_aa") return Run2A(srv.Addr(), 0xAA, "2a_aa");
  if (mode == "2b_after_stop") return Run2BAfterStop(srv.Addr(), false, "2b_after_stop");
  if (mode == "2b_cancel_after_stop") return Run2BAfterStop(srv.Addr(), true, "2b_cancel_after_stop");
  if (mode == "2d_inflight_cancel") return Run2DInflightCancel(srv.service, srv.Addr());
  std::cerr << "Unknown mode: " << mode << std::endl;
  return 2;
}
