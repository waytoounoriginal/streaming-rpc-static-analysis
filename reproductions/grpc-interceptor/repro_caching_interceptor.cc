#include <grpcpp/grpcpp.h>
#include <grpcpp/support/client_interceptor.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <iostream>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

// Directly include the UNMODIFIED upstream examples/cpp/interceptors/caching_interceptor.h
// (it uses std::cout/std::endl without including <iostream>, relying on caller include order).
#include "caching_interceptor.h"
#include "fixed_caching_interceptor.h"
#include "keyvaluestore.grpc.pb.h"

using grpc::CallbackServerContext;
using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReaderWriter;
using grpc::Server;
using grpc::ServerBidiReactor;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReaderWriter;
using grpc::Status;
using grpc::StatusCode;
using grpc::experimental::ClientInterceptorFactoryInterface;
using grpc::experimental::ClientRpcInfo;
using grpc::experimental::InterceptionHookPoints;
using grpc::experimental::Interceptor;
using grpc::experimental::InterceptorBatchMethods;
using keyvaluestore::KeyValueStore;
using keyvaluestore::Request;
using keyvaluestore::Response;

namespace {

std::string LookupValue(const std::string& key) {
  if (key == "key1") return "value1";
  if (key == "key2") return "value2";
  if (key == "key3") return "value3";
  return "";
}

struct ServerState {
  enum class Mode {
    CLEAN_OK,
    FAIL_AFTER_FIRST_KEY,
    FAIL_IMMEDIATELY_ON_READ,
    FAIL_POST_WRITES_DONE,
    DELAY_ON_WRITES_DONE,
  };

  Mode mode = Mode::CLEAN_OK;
  std::atomic<int> rpcs_started{0};
  std::atomic<int> read_requests{0};
  std::atomic<bool> last_was_cancelled{false};

  std::mutex mu;
  std::condition_variable cv;
  bool last_rpc_exited = false;
  std::vector<std::string> seen_keys;

  void Reset(Mode m) {
    mode = m;
    rpcs_started = 0;
    read_requests = 0;
    last_was_cancelled = false;
    std::lock_guard<std::mutex> lock(mu);
    last_rpc_exited = false;
    seen_keys.clear();
  }

  bool WaitForServerExit(int timeout_ms = 2000) {
    std::unique_lock<std::mutex> lock(mu);
    return cv.wait_for(lock, std::chrono::milliseconds(timeout_ms),
                       [this] { return last_rpc_exited; });
  }
};

class ConfigurableKeyValueService final : public KeyValueStore::Service {
 public:
  explicit ConfigurableKeyValueService(ServerState* state) : state_(state) {}

  Status GetValues(ServerContext* context,
                   ServerReaderWriter<Response, Request>* stream) override {
    state_->rpcs_started.fetch_add(1);
    Request req;
    int local_reads = 0;
    Status final_status = Status::OK;

    while (stream->Read(&req)) {
      ++local_reads;
      state_->read_requests.fetch_add(1);
      {
        std::lock_guard<std::mutex> lock(state_->mu);
        state_->seen_keys.push_back(req.key());
      }

      if (state_->mode == ServerState::Mode::FAIL_IMMEDIATELY_ON_READ) {
        final_status = Status(StatusCode::INTERNAL, "db failure");
        break;
      }
      if (state_->mode == ServerState::Mode::FAIL_AFTER_FIRST_KEY &&
          local_reads >= 2) {
        final_status = Status(StatusCode::INTERNAL, "db failure");
        break;
      }

      Response resp;
      resp.set_value(LookupValue(req.key()));
      if (!stream->Write(resp)) {
        break;
      }
    }

    if (final_status.ok()) {
      if (state_->mode == ServerState::Mode::FAIL_POST_WRITES_DONE) {
        final_status = Status(StatusCode::INTERNAL, "commit failed");
      } else if (state_->mode == ServerState::Mode::DELAY_ON_WRITES_DONE) {
        // Simulate server-side finalization/commit work after client WritesDone()
        std::this_thread::sleep_for(std::chrono::milliseconds(150));
        context->AddTrailingMetadata("x-server-commit", "committed");
      }
    }

    state_->last_was_cancelled.store(context->IsCancelled());
    {
      std::lock_guard<std::mutex> lock(state_->mu);
      state_->last_rpc_exited = true;
    }
    state_->cv.notify_all();
    return final_status;
  }

 private:
  ServerState* state_;
};

// Reproduces the exact reactor logic of upstream examples/cpp/interceptors/server.cc:90-120
// where OnReadDone(!ok) calls Finish(grpc::Status::CANCELLED).
class UpstreamExampleStyleService final : public KeyValueStore::CallbackService {
 public:
  ServerBidiReactor<Request, Response>* GetValues(
      CallbackServerContext* /*context*/) override {
    class Reactor : public ServerBidiReactor<Request, Response> {
     public:
      Reactor() { StartRead(&request_); }

      void OnReadDone(bool ok) override {
        if (!ok) {
          return Finish(grpc::Status::CANCELLED);
        }
        response_.set_value(LookupValue(request_.key()));
        StartWrite(&response_);
      }

      void OnWriteDone(bool ok) override {
        if (!ok) {
          return Finish(grpc::Status::CANCELLED);
        }
        StartRead(&request_);
      }

      void OnDone() override { delete this; }

     private:
      Request request_;
      Response response_;
    };
    return new Reactor();
  }
};

struct BackingStreamStats {
  std::atomic<int> pre_send_initial_metadata{0};
  std::atomic<int> post_recv_initial_metadata{0};
  std::atomic<int> pre_send_message{0};
  std::atomic<int> post_recv_message{0};
  std::atomic<int> pre_send_close{0};
  std::atomic<int> post_recv_status{0};
  std::atomic<int> pre_send_cancel{0};

  std::mutex mu;
  bool has_status = false;
  Status observed_status = Status::OK;
  std::string observed_trailer_commit;

  void Reset() {
    pre_send_initial_metadata = 0;
    post_recv_initial_metadata = 0;
    pre_send_message = 0;
    post_recv_message = 0;
    pre_send_close = 0;
    post_recv_status = 0;
    pre_send_cancel = 0;
    std::lock_guard<std::mutex> lock(mu);
    has_status = false;
    observed_status = Status::OK;
    observed_trailer_commit.clear();
  }
};

// Placed AFTER CachingInterceptor in the interceptor pipeline, so it ONLY
// observes the backing stream_ created via methods->GetInterceptedChannel().
class BackingObserverInterceptor : public Interceptor {
 public:
  explicit BackingObserverInterceptor(BackingStreamStats* stats)
      : stats_(stats) {}

  void Intercept(InterceptorBatchMethods* methods) override {
    if (methods->QueryInterceptionHookPoint(
            InterceptionHookPoints::PRE_SEND_INITIAL_METADATA)) {
      stats_->pre_send_initial_metadata.fetch_add(1);
    }
    if (methods->QueryInterceptionHookPoint(
            InterceptionHookPoints::POST_RECV_INITIAL_METADATA)) {
      stats_->post_recv_initial_metadata.fetch_add(1);
    }
    if (methods->QueryInterceptionHookPoint(
            InterceptionHookPoints::PRE_SEND_MESSAGE)) {
      stats_->pre_send_message.fetch_add(1);
    }
    if (methods->QueryInterceptionHookPoint(
            InterceptionHookPoints::POST_RECV_MESSAGE)) {
      stats_->post_recv_message.fetch_add(1);
    }
    if (methods->QueryInterceptionHookPoint(
            InterceptionHookPoints::PRE_SEND_CLOSE)) {
      stats_->pre_send_close.fetch_add(1);
    }
    if (methods->QueryInterceptionHookPoint(
            InterceptionHookPoints::POST_RECV_STATUS)) {
      stats_->post_recv_status.fetch_add(1);
      auto* s = methods->GetRecvStatus();
      auto* trailers = methods->GetRecvTrailingMetadata();
      std::lock_guard<std::mutex> lock(stats_->mu);
      stats_->has_status = true;
      if (s != nullptr) {
        stats_->observed_status = *s;
      }
      if (trailers != nullptr) {
        auto it = trailers->find("x-server-commit");
        if (it != trailers->end()) {
          stats_->observed_trailer_commit.assign(it->second.begin(),
                                                 it->second.end());
        }
      }
    }
    if (methods->QueryInterceptionHookPoint(
            InterceptionHookPoints::PRE_SEND_CANCEL)) {
      stats_->pre_send_cancel.fetch_add(1);
      return;
    }
    methods->Proceed();
  }

 private:
  BackingStreamStats* stats_;
};

class BackingObserverInterceptorFactory
    : public ClientInterceptorFactoryInterface {
 public:
  explicit BackingObserverInterceptorFactory(BackingStreamStats* stats)
      : stats_(stats) {}

  Interceptor* CreateClientInterceptor(ClientRpcInfo* /*info*/) override {
    return new BackingObserverInterceptor(stats_);
  }

 private:
  BackingStreamStats* stats_;
};

enum class InterceptorKind {
  NONE,
  UPSTREAM_BUGGY,
  FIXED,
};

std::shared_ptr<Channel> MakeChannel(const std::string& target,
                                     InterceptorKind kind,
                                     BackingStreamStats* observer_stats = nullptr) {
  grpc::ChannelArguments args;
  std::vector<std::unique_ptr<ClientInterceptorFactoryInterface>> creators;
  if (kind == InterceptorKind::UPSTREAM_BUGGY) {
    creators.push_back(std::make_unique<CachingInterceptorFactory>());
  } else if (kind == InterceptorKind::FIXED) {
    creators.push_back(std::make_unique<FixedCachingInterceptorFactory>());
  }
  if (observer_stats != nullptr) {
    creators.push_back(
        std::make_unique<BackingObserverInterceptorFactory>(observer_stats));
  }
  if (creators.empty()) {
    return grpc::CreateCustomChannel(target, grpc::InsecureChannelCredentials(),
                                     args);
  }
  return grpc::experimental::CreateCustomChannelWithInterceptors(
      target, grpc::InsecureChannelCredentials(), args, std::move(creators));
}

struct SyncRunResult {
  struct Step {
    std::string key;
    bool write_ok = false;
    bool read_ok = false;
    std::string value;
  };
  std::vector<Step> steps;
  bool writes_done_ok = false;
  Status finish_status = Status::OK;
  long long finish_elapsed_ms = 0;
};

SyncRunResult RunSyncClient(std::shared_ptr<Channel> channel,
                            const std::vector<std::string>& keys,
                            bool stop_on_read_failure = false) {
  SyncRunResult result;
  auto stub = KeyValueStore::NewStub(channel);
  ClientContext ctx;
  std::unique_ptr<ClientReaderWriter<Request, Response>> stream =
      stub->GetValues(&ctx);

  bool broke_early = false;
  for (const auto& k : keys) {
    SyncRunResult::Step step;
    step.key = k;
    Request req;
    req.set_key(k);
    step.write_ok = stream->Write(req);
    Response resp;
    step.read_ok = stream->Read(&resp);
    step.value = resp.value();
    result.steps.push_back(step);
    if (stop_on_read_failure && (!step.write_ok || !step.read_ok)) {
      broke_early = true;
      break;
    }
  }

  if (!broke_early) {
    result.writes_done_ok = stream->WritesDone();
  }
  auto t0 = std::chrono::steady_clock::now();
  result.finish_status = stream->Finish();
  auto t1 = std::chrono::steady_clock::now();
  result.finish_elapsed_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
  return result;
}

// Mirrors upstream examples/cpp/interceptors/client.cc (ClientBidiReactor)
class CallbackKVClient : public grpc::ClientBidiReactor<Request, Response> {
 public:
  CallbackKVClient(std::shared_ptr<Channel> channel,
                   std::vector<std::string> keys)
      : stub_(KeyValueStore::NewStub(channel)), keys_(std::move(keys)) {
    stub_->async()->GetValues(&context_, this);
    request_.set_key(keys_[0]);
    StartWrite(&request_);
    StartCall();
  }

  void OnWriteDone(bool ok) override {
    write_ok_log_.push_back(ok);
    if (ok) {
      StartRead(&response_);
    }
  }

  void OnReadDone(bool ok) override {
    read_log_.push_back({keys_[counter_], ok, ok ? response_.value() : ""});
    if (ok) {
      std::cout << "  [Callback] " << request_.key() << " : "
                << response_.value() << std::endl;
      if (++counter_ < keys_.size()) {
        request_.set_key(keys_[counter_]);
        StartWrite(&request_);
      } else {
        StartWritesDone();
      }
    }
  }

  void OnDone(const Status& s) override {
    std::lock_guard<std::mutex> lock(mu_);
    final_status_ = s;
    done_ = true;
    cv_.notify_all();
  }

  bool Await(int timeout_ms = 3000) {
    std::unique_lock<std::mutex> lock(mu_);
    return cv_.wait_for(lock, std::chrono::milliseconds(timeout_ms),
                        [this] { return done_; });
  }

  Status final_status() {
    std::lock_guard<std::mutex> lock(mu_);
    return final_status_;
  }

  struct ReadEntry {
    std::string key;
    bool ok;
    std::string value;
  };
  std::vector<bool> write_ok_log_;
  std::vector<ReadEntry> read_log_;

 private:
  std::unique_ptr<KeyValueStore::Stub> stub_;
  std::vector<std::string> keys_;
  size_t counter_ = 0;
  ClientContext context_;
  Request request_;
  Response response_;
  std::mutex mu_;
  std::condition_variable cv_;
  bool done_ = false;
  Status final_status_;
};

void PrintSyncResult(const std::string& label, const SyncRunResult& r,
                     const ServerState& srv) {
  std::cout << "  [" << label << "] steps=" << r.steps.size()
            << " server_reads=" << srv.read_requests.load()
            << " writes_done_ok=" << (r.writes_done_ok ? "true" : "false")
            << " finish_code=" << static_cast<int>(r.finish_status.error_code())
            << " finish_msg=\"" << r.finish_status.error_message() << "\""
            << " finish_ms=" << r.finish_elapsed_ms << std::endl;
  for (size_t i = 0; i < r.steps.size(); ++i) {
    const auto& s = r.steps[i];
    std::cout << "    step[" << i << "] key=" << s.key
              << " write_ok=" << (s.write_ok ? "true" : "false")
              << " read_ok=" << (s.read_ok ? "true" : "false")
              << " value=\"" << s.value << "\"" << std::endl;
  }
}

void PrintObserverStats(const std::string& label, const BackingStreamStats& st) {
  std::cout << "  [" << label << " BackingObserver] "
            << "pre_send_init=" << st.pre_send_initial_metadata.load()
            << " post_recv_init=" << st.post_recv_initial_metadata.load()
            << " pre_send_msg=" << st.pre_send_message.load()
            << " post_recv_msg=" << st.post_recv_message.load()
            << " pre_send_close=" << st.pre_send_close.load()
            << " post_recv_status=" << st.post_recv_status.load()
            << " pre_send_cancel=" << st.pre_send_cancel.load();
  if (st.has_status) {
    std::cout << " observed_code="
              << static_cast<int>(st.observed_status.error_code())
              << " observed_msg=\"" << st.observed_status.error_message()
              << "\" trailer_commit=\"" << st.observed_trailer_commit << "\"";
  } else {
    std::cout << " observed_status=<NONE>";
  }
  std::cout << std::endl;
}

class ScopedServer {
 public:
  explicit ScopedServer(grpc::Service* service) {
    ServerBuilder builder;
    int selected_port = 0;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(),
                             &selected_port);
    builder.RegisterService(service);
    server_ = builder.BuildAndStart();
    target_ = "127.0.0.1:" + std::to_string(selected_port);
  }

  ~ScopedServer() {
    if (server_) {
      server_->Shutdown();
      server_->Wait();
    }
  }

  const std::string& target() const { return target_; }

 private:
  std::unique_ptr<Server> server_;
  std::string target_;
};

int RunFullSuite(bool trace_only_mode) {
  ServerState state;
  ConfigurableKeyValueService service(&state);
  ScopedServer srv(&service);

  if (trace_only_mode) {
    std::cout << "=== TRACE RUN: Case A1 + Case B1 with Upstream CachingInterceptor ==="
              << std::endl;
    state.Reset(ServerState::Mode::FAIL_AFTER_FIRST_KEY);
    BackingStreamStats obs;
    auto ch = MakeChannel(srv.target(), InterceptorKind::UPSTREAM_BUGGY, &obs);
    auto res = RunSyncClient(ch, {"key1", "key2", "key2"});
    PrintSyncResult("TRACE_CASE_A1_UPSTREAM", res, state);
    PrintObserverStats("TRACE_CASE_A1_UPSTREAM", obs);

    state.Reset(ServerState::Mode::DELAY_ON_WRITES_DONE);
    obs.Reset();
    auto ch2 = MakeChannel(srv.target(), InterceptorKind::UPSTREAM_BUGGY, &obs);
    auto res2 = RunSyncClient(ch2, {"key1"});
    bool exited = state.WaitForServerExit(2000);
    std::cout << "  [TRACE_CASE_B1_UPSTREAM] server_exited=" << exited
              << " server_saw_cancelled="
              << (state.last_was_cancelled.load() ? "true" : "false")
              << std::endl;
    PrintSyncResult("TRACE_CASE_B1_UPSTREAM", res2, state);
    PrintObserverStats("TRACE_CASE_B1_UPSTREAM", obs);

    std::cout << "=== TRACE RUN: Case B1 with FixedCachingInterceptor ==="
              << std::endl;
    state.Reset(ServerState::Mode::DELAY_ON_WRITES_DONE);
    obs.Reset();
    auto ch3 = MakeChannel(srv.target(), InterceptorKind::FIXED, &obs);
    auto res3 = RunSyncClient(ch3, {"key1"});
    bool exited3 = state.WaitForServerExit(2000);
    std::cout << "  [TRACE_CASE_B1_FIXED] server_exited=" << exited3
              << " server_saw_cancelled="
              << (state.last_was_cancelled.load() ? "true" : "false")
              << std::endl;
    PrintSyncResult("TRACE_CASE_B1_FIXED", res3, state);
    PrintObserverStats("TRACE_CASE_B1_FIXED", obs);
    return 0;
  }

  std::cout << "=== T0a: Clean Server Happy Path (Upstream CachingInterceptor) ==="
            << std::endl;
  {
    state.Reset(ServerState::Mode::CLEAN_OK);
    auto ch = MakeChannel(srv.target(), InterceptorKind::UPSTREAM_BUGGY);
    auto r = RunSyncClient(ch, {"key1", "key2", "key1"});
    PrintSyncResult("T0a_UPSTREAM", r, state);
  }

  std::cout << "\n=== T0b: Upstream examples/cpp/interceptors/server.cc (Finish(CANCELLED) on EOF) ==="
            << std::endl;
  {
    UpstreamExampleStyleService upstream_srv;
    ScopedServer srv2(&upstream_srv);
    auto ch_direct = MakeChannel(srv2.target(), InterceptorKind::NONE);
    auto ch_buggy = MakeChannel(srv2.target(), InterceptorKind::UPSTREAM_BUGGY);
    auto ch_fixed = MakeChannel(srv2.target(), InterceptorKind::FIXED);

    ServerState dummy;
    auto r_direct = RunSyncClient(ch_direct, {"key1", "key1"});
    auto r_buggy = RunSyncClient(ch_buggy, {"key1", "key1"});
    auto r_fixed = RunSyncClient(ch_fixed, {"key1", "key1"});
    std::cout << "  [T0b_NO_INTERCEPTOR] finish_code="
              << static_cast<int>(r_direct.finish_status.error_code())
              << " (1=CANCELLED)" << std::endl;
    std::cout << "  [T0b_UPSTREAM_BUGGY] finish_code="
              << static_cast<int>(r_buggy.finish_status.error_code())
              << " (masked to OK!)" << std::endl;
    std::cout << "  [T0b_FIXED]          finish_code="
              << static_cast<int>(r_fixed.finish_status.error_code())
              << " (propagated CANCELLED)" << std::endl;
  }

  std::cout << "\n=== T1 (Case A1): Mid-Stream INTERNAL(\"db failure\") after 1st key + Retry ==="
            << std::endl;
  {
    state.Reset(ServerState::Mode::FAIL_AFTER_FIRST_KEY);
    auto r_buggy = RunSyncClient(
        MakeChannel(srv.target(), InterceptorKind::UPSTREAM_BUGGY),
        {"key1", "key2", "key2"});
    PrintSyncResult("T1_UPSTREAM_BUGGY", r_buggy, state);

    state.Reset(ServerState::Mode::FAIL_AFTER_FIRST_KEY);
    auto r_fixed =
        RunSyncClient(MakeChannel(srv.target(), InterceptorKind::FIXED),
                      {"key1", "key2", "key2"});
    PrintSyncResult("T1_FIXED", r_fixed, state);

    state.Reset(ServerState::Mode::FAIL_AFTER_FIRST_KEY);
    auto r_fixed_early =
        RunSyncClient(MakeChannel(srv.target(), InterceptorKind::FIXED),
                      {"key1", "key2", "key2"}, /*stop_on_read_failure=*/true);
    PrintSyncResult("T1b_FIXED_EARLY_BREAK", r_fixed_early, state);
  }

  std::cout << "\n=== T2 (Case A1b): ClientBidiReactor (client.cc pattern) on Mid-Stream Failure ==="
            << std::endl;
  {
    state.Reset(ServerState::Mode::FAIL_AFTER_FIRST_KEY);
    CallbackKVClient c_buggy(
        MakeChannel(srv.target(), InterceptorKind::UPSTREAM_BUGGY),
        {"key1", "key2", "key2"});
    bool ok_wait = c_buggy.Await(3000);
    std::cout << "  [T2_UPSTREAM_BUGGY] completed=" << ok_wait
              << " on_done_code="
              << static_cast<int>(c_buggy.final_status().error_code())
              << " on_done_msg=\"" << c_buggy.final_status().error_message()
              << "\" server_reads=" << state.read_requests.load()
              << " reads_count=" << c_buggy.read_log_.size() << std::endl;

    state.Reset(ServerState::Mode::FAIL_AFTER_FIRST_KEY);
    CallbackKVClient c_fixed(
        MakeChannel(srv.target(), InterceptorKind::FIXED),
        {"key1", "key2", "key2"});
    bool ok_wait_fixed = c_fixed.Await(3000);
    std::cout << "  [T2_FIXED] completed=" << ok_wait_fixed
              << " on_done_code="
              << static_cast<int>(c_fixed.final_status().error_code())
              << " on_done_msg=\"" << c_fixed.final_status().error_message()
              << "\" server_reads=" << state.read_requests.load()
              << " reads_count=" << c_fixed.read_log_.size();
    for (size_t i = 0; i < c_fixed.read_log_.size(); ++i) {
      std::cout << " | read[" << i << "](" << c_fixed.read_log_[i].key
                << ",ok=" << c_fixed.read_log_[i].ok << ",val=\""
                << c_fixed.read_log_[i].value << "\")";
    }
    std::cout << std::endl;
  }

  std::cout << "\n=== T3 (Case A2): Immediate Server Failure on 1st Key (0 responses) ==="
            << std::endl;
  {
    state.Reset(ServerState::Mode::FAIL_IMMEDIATELY_ON_READ);
    auto r_buggy = RunSyncClient(
        MakeChannel(srv.target(), InterceptorKind::UPSTREAM_BUGGY),
        {"key1", "key1"});
    PrintSyncResult("T3_UPSTREAM_BUGGY", r_buggy, state);

    state.Reset(ServerState::Mode::FAIL_IMMEDIATELY_ON_READ);
    auto r_fixed = RunSyncClient(
        MakeChannel(srv.target(), InterceptorKind::FIXED), {"key1", "key1"});
    PrintSyncResult("T3_FIXED", r_fixed, state);
  }

  std::cout << "\n=== T4 (Case A3): Post-WritesDone Failure (all Reads ok, server fails at Finish) ==="
            << std::endl;
  {
    state.Reset(ServerState::Mode::FAIL_POST_WRITES_DONE);
    auto r_buggy = RunSyncClient(
        MakeChannel(srv.target(), InterceptorKind::UPSTREAM_BUGGY), {"key1"});
    PrintSyncResult("T4_UPSTREAM_BUGGY", r_buggy, state);

    state.Reset(ServerState::Mode::FAIL_POST_WRITES_DONE);
    auto r_fixed = RunSyncClient(
        MakeChannel(srv.target(), InterceptorKind::FIXED), {"key1"});
    PrintSyncResult("T4_FIXED", r_fixed, state);
  }

  std::cout << "\n=== T5 (Case B1) & T6 (Case B2): Unfinalized Stream Teardown, Server Cancellation, Trailers, & Downstream POST_RECV_STATUS ==="
            << std::endl;
  {
    state.Reset(ServerState::Mode::DELAY_ON_WRITES_DONE);
    BackingStreamStats obs_buggy;
    auto ch_buggy = MakeChannel(srv.target(), InterceptorKind::UPSTREAM_BUGGY,
                                &obs_buggy);
    auto r_buggy = RunSyncClient(ch_buggy, {"key1"});
    bool srv_exited = state.WaitForServerExit(2000);
    PrintSyncResult("T5_UPSTREAM_BUGGY", r_buggy, state);
    std::cout << "  [T5_UPSTREAM_BUGGY] server_exited=" << srv_exited
              << " server_saw_cancelled="
              << (state.last_was_cancelled.load() ? "true" : "false")
              << std::endl;
    PrintObserverStats("T6_UPSTREAM_BUGGY", obs_buggy);

    state.Reset(ServerState::Mode::DELAY_ON_WRITES_DONE);
    BackingStreamStats obs_fixed;
    auto ch_fixed =
        MakeChannel(srv.target(), InterceptorKind::FIXED, &obs_fixed);
    auto r_fixed = RunSyncClient(ch_fixed, {"key1"});
    bool srv_exited_fixed = state.WaitForServerExit(2000);
    PrintSyncResult("T5_FIXED", r_fixed, state);
    std::cout << "  [T5_FIXED]          server_exited=" << srv_exited_fixed
              << " server_saw_cancelled="
              << (state.last_was_cancelled.load() ? "true" : "false")
              << std::endl;
    PrintObserverStats("T6_FIXED", obs_fixed);
  }

  return 0;
}

}  // namespace

int main(int argc, char** argv) {
  bool trace_only = false;
  for (int i = 1; i < argc; ++i) {
    if (std::strcmp(argv[i], "--trace-only") == 0) {
      trace_only = true;
    }
  }
  return RunFullSuite(trace_only);
}
