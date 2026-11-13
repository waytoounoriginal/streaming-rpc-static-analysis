#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <iostream>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <grpcpp/grpcpp.h>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#endif

#include "grpc/grpc_client.h"
#include "grpc/wenet.grpc.pb.h"

namespace {

std::string StatusToStr(wenet::Response::Status s) {
  switch (s) {
    case wenet::Response::ok:
      return "ok";
    case wenet::Response::failed:
      return "failed";
    default:
      return "unknown(" + std::to_string(static_cast<int>(s)) + ")";
  }
}

std::string TypeToStr(wenet::Response::Type t) {
  switch (t) {
    case wenet::Response::server_ready:
      return "server_ready";
    case wenet::Response::partial_result:
      return "partial_result";
    case wenet::Response::final_result:
      return "final_result";
    case wenet::Response::speech_end:
      return "speech_end";
    default:
      return "unknown(" + std::to_string(static_cast<int>(t)) + ")";
  }
}

std::string FormatResponse(const wenet::Response& resp) {
  std::ostringstream oss;
  oss << "Response{status=" << StatusToStr(resp.status())
      << ", type=" << TypeToStr(resp.type())
      << ", nbest_size=" << resp.nbest_size();
  if (resp.nbest_size() > 0) {
    oss << ", sentences=[";
    for (int i = 0; i < resp.nbest_size(); ++i) {
      if (i > 0) oss << ", ";
      oss << "\"" << resp.nbest(i).sentence() << "\"";
    }
    oss << "]";
  }
  oss << "}";
  return oss.str();
}

std::string MakePcmBytes(int num_samples, int16_t fill = 100) {
  std::vector<int16_t> samples(static_cast<size_t>(num_samples), fill);
  return std::string(reinterpret_cast<const char*>(samples.data()),
                     samples.size() * sizeof(int16_t));
}

class ServerSubprocess {
 public:
  ServerSubprocess() = default;
  ~ServerSubprocess() { StopAndCleanup(); }

  ServerSubprocess(const ServerSubprocess&) = delete;
  ServerSubprocess& operator=(const ServerSubprocess&) = delete;

  bool Start(const std::string& exe_path, const std::string& extra_args = "") {
    SECURITY_ATTRIBUTES sa{};
    sa.nLength = sizeof(sa);
    sa.bInheritHandle = TRUE;

    HANDLE out_read = nullptr, out_write = nullptr;
    HANDLE err_read = nullptr, err_write = nullptr;
    HANDLE in_read = nullptr, in_write = nullptr;

    if (!CreatePipe(&out_read, &out_write, &sa, 0) ||
        !CreatePipe(&err_read, &err_write, &sa, 0) ||
        !CreatePipe(&in_read, &in_write, &sa, 0)) {
      std::cerr << "CreatePipe failed" << std::endl;
      return false;
    }

    SetHandleInformation(out_read, HANDLE_FLAG_INHERIT, 0);
    SetHandleInformation(err_read, HANDLE_FLAG_INHERIT, 0);
    SetHandleInformation(in_write, HANDLE_FLAG_INHERIT, 0);

    STARTUPINFOA si{};
    si.cb = sizeof(si);
    si.dwFlags = STARTF_USESTDHANDLES;
    si.hStdOutput = out_write;
    si.hStdError = err_write;
    si.hStdInput = in_read;

    std::string cmd = "\"" + exe_path + "\"";
    if (!extra_args.empty()) {
      cmd += " " + extra_args;
    }
    std::vector<char> cmd_buf(cmd.begin(), cmd.end());
    cmd_buf.push_back('\0');

    PROCESS_INFORMATION pi{};
    BOOL ok = CreateProcessA(nullptr, cmd_buf.data(), nullptr, nullptr, TRUE,
                             CREATE_NO_WINDOW, nullptr, nullptr, &si, &pi);

    CloseHandle(out_write);
    CloseHandle(err_write);
    CloseHandle(in_read);

    if (!ok) {
      CloseHandle(out_read);
      CloseHandle(err_read);
      CloseHandle(in_write);
      std::cerr << "CreateProcessA failed for: " << cmd
                << " err=" << GetLastError() << std::endl;
      return false;
    }

    hProcess_ = pi.hProcess;
    CloseHandle(pi.hThread);
    in_write_ = in_write;

    out_thread_ = std::thread([this, out_read]() {
      ReadPipeLines(out_read, &stdout_lines_, /*parse_port=*/true);
      CloseHandle(out_read);
    });
    err_thread_ = std::thread([this, err_read]() {
      ReadPipeLines(err_read, &stderr_lines_, /*parse_port=*/false);
      CloseHandle(err_read);
    });

    std::unique_lock<std::mutex> lock(mu_);
    bool got_port = cv_.wait_for(lock, std::chrono::seconds(10),
                                 [&] { return port_ > 0 || stdout_eof_; });
    return got_port && port_ > 0;
  }

  int port() const { return port_; }

  bool WaitForExit(DWORD timeout_ms, DWORD* exit_code) {
    if (!hProcess_) return false;
    DWORD wait_res = WaitForSingleObject(hProcess_, timeout_ms);
    if (wait_res != WAIT_OBJECT_0) {
      return false;
    }
    if (exit_code) {
      GetExitCodeProcess(hProcess_, exit_code);
    }
    JoinReaders();
    return true;
  }

  DWORD StopAndCleanup() {
    if (in_write_) {
      const char* msg = "SHUTDOWN\n";
      DWORD written = 0;
      WriteFile(in_write_, msg, static_cast<DWORD>(strlen(msg)), &written,
                nullptr);
      CloseHandle(in_write_);
      in_write_ = nullptr;
    }
    DWORD exit_code = 0;
    if (hProcess_) {
      DWORD wait_res = WaitForSingleObject(hProcess_, 5000);
      if (wait_res != WAIT_OBJECT_0) {
        TerminateProcess(hProcess_, 99);
        WaitForSingleObject(hProcess_, 2000);
      }
      GetExitCodeProcess(hProcess_, &exit_code);
      CloseHandle(hProcess_);
      hProcess_ = nullptr;
    }
    JoinReaders();
    return exit_code;
  }

  std::vector<std::string> stderr_lines() {
    std::lock_guard<std::mutex> lock(mu_);
    return stderr_lines_;
  }

 private:
  void JoinReaders() {
    if (out_thread_.joinable()) out_thread_.join();
    if (err_thread_.joinable()) err_thread_.join();
  }

  void ReadPipeLines(HANDLE hPipe, std::vector<std::string>* target,
                     bool parse_port) {
    std::string buf;
    char chunk[1024];
    DWORD bytes_read = 0;
    while (ReadFile(hPipe, chunk, sizeof(chunk), &bytes_read, nullptr) &&
           bytes_read > 0) {
      buf.append(chunk, bytes_read);
      size_t pos = 0;
      while (true) {
        size_t nl = buf.find('\n', pos);
        if (nl == std::string::npos) break;
        std::string line = buf.substr(pos, nl - pos);
        if (!line.empty() && line.back() == '\r') line.pop_back();
        {
          std::lock_guard<std::mutex> lock(mu_);
          target->push_back(line);
          if (parse_port && line.rfind("LISTENING_PORT=", 0) == 0) {
            port_ = std::stoi(line.substr(15));
            cv_.notify_all();
          }
        }
        pos = nl + 1;
      }
      buf.erase(0, pos);
    }
    if (!buf.empty()) {
      std::lock_guard<std::mutex> lock(mu_);
      target->push_back(buf);
    }
    if (parse_port) {
      std::lock_guard<std::mutex> lock(mu_);
      stdout_eof_ = true;
      cv_.notify_all();
    }
  }

  HANDLE hProcess_ = nullptr;
  HANDLE in_write_ = nullptr;
  std::thread out_thread_;
  std::thread err_thread_;
  std::mutex mu_;
  std::condition_variable cv_;
  std::vector<std::string> stdout_lines_;
  std::vector<std::string> stderr_lines_;
  int port_ = 0;
  bool stdout_eof_ = false;
};

std::shared_ptr<grpc::Channel> MakeChannel(int port) {
  return grpc::CreateChannel("127.0.0.1:" + std::to_string(port),
                             grpc::InsecureChannelCredentials());
}

grpc::Status RunProbeRpc(int port, int nbest, int audio_chunks,
                         std::vector<wenet::Response>* out_responses) {
  auto stub = wenet::ASR::NewStub(MakeChannel(port));
  grpc::ClientContext ctx;
  ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
  auto stream = stub->Recognize(&ctx);

  wenet::Request cfg;
  cfg.mutable_decode_config()->set_nbest_config(nbest);
  cfg.mutable_decode_config()->set_continuous_decoding_config(false);
  stream->Write(cfg);

  for (int i = 0; i < audio_chunks; ++i) {
    wenet::Request audio;
    audio.set_audio_data(MakePcmBytes(800));
    if (!stream->Write(audio)) break;
  }
  stream->WritesDone();

  wenet::Response resp;
  while (stream->Read(&resp)) {
    if (out_responses) out_responses->push_back(resp);
  }
  return stream->Finish();
}

void PrintServerStderr(const std::vector<std::string>& lines) {
  std::cout << "  [Server stderr lines: " << lines.size() << "]" << std::endl;
  for (const auto& l : lines) {
    std::cout << "    " << l << std::endl;
  }
}

bool RunTestBaseline(const std::string& server_exe) {
  std::cout << "\n=== BASELINE CONTROL: Conforming Client (nbest=1 & nbest=2) ==="
            << std::endl;
  ServerSubprocess srv;
  if (!srv.Start(server_exe)) {
    std::cout << "  ERROR: failed to start server" << std::endl;
    return false;
  }
  std::vector<wenet::Response> r1, r2;
  grpc::Status s1 = RunProbeRpc(srv.port(), 1, 2, &r1);
  grpc::Status s2 = RunProbeRpc(srv.port(), 2, 2, &r2);
  DWORD ec = srv.StopAndCleanup();

  int final_nbest_1 = -1;
  int final_nbest_2 = -1;
  for (const auto& r : r1) {
    if (r.type() == wenet::Response::final_result) final_nbest_1 = r.nbest_size();
  }
  for (const auto& r : r2) {
    if (r.type() == wenet::Response::final_result) final_nbest_2 = r.nbest_size();
  }
  std::cout << "  nbest=1 rpc status=" << s1.error_code()
            << " responses=" << r1.size()
            << " final_nbest_size=" << final_nbest_1 << std::endl;
  std::cout << "  nbest=2 rpc status=" << s2.error_code()
            << " responses=" << r2.size()
            << " final_nbest_size=" << final_nbest_2 << std::endl;
  std::cout << "  server exit_code=" << ec << std::endl;
  PrintServerStderr(srv.stderr_lines());
  return s1.ok() && s2.ok() && final_nbest_1 == 1 && final_nbest_2 == 2 &&
         ec == 0;
}

bool RunTest3A(const std::string& server_exe, bool expect_patched) {
  std::cout << "\n=== TEST 3A: Empty Stream (0 messages + WritesDone) ["
            << (expect_patched ? "PATCHED" : "UNPATCHED") << "] ==="
            << std::endl;
  ServerSubprocess srv;
  if (!srv.Start(server_exe)) {
    std::cout << "  ERROR: failed to start server" << std::endl;
    return false;
  }

  auto stub = wenet::ASR::NewStub(MakeChannel(srv.port()));
  grpc::ClientContext ctx;
  ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
  auto stream = stub->Recognize(&ctx);
  stream->WritesDone();

  wenet::Response resp;
  int resp_count = 0;
  while (stream->Read(&resp)) {
    resp_count++;
    std::cout << "  Received: " << FormatResponse(resp) << std::endl;
  }
  grpc::Status st = stream->Finish();
  std::cout << "  Empty-stream Finish(): ok=" << (st.ok() ? "true" : "false")
            << " code=" << st.error_code()
            << " msg=\"" << st.error_message() << "\""
            << " responses=" << resp_count << std::endl;

  DWORD crash_code = 0;
  bool exited_early = srv.WaitForExit(1500, &crash_code);
  std::cout << "  Server exited spontaneously after empty stream: "
            << (exited_early ? "YES" : "NO");
  if (exited_early) {
    std::cout << " (exit_code=" << crash_code << " / 0x" << std::hex
              << crash_code << std::dec << ")";
  }
  std::cout << std::endl;

  // Probe server liveness with a second RPC
  std::vector<wenet::Response> probe_resps;
  grpc::Status probe_st = RunProbeRpc(srv.port(), 1, 1, &probe_resps);
  std::cout << "  Subsequent health-probe RPC: ok="
            << (probe_st.ok() ? "true" : "false")
            << " code=" << probe_st.error_code()
            << " msg=\"" << probe_st.error_message() << "\""
            << " responses=" << probe_resps.size() << std::endl;

  if (!exited_early) {
    DWORD ec = srv.StopAndCleanup();
    std::cout << "  Graceful shutdown exit_code=" << ec << std::endl;
  }
  PrintServerStderr(srv.stderr_lines());

  if (!expect_patched) {
    return exited_early && crash_code != 0 && !probe_st.ok();
  } else {
    return !exited_early && st.ok() && probe_st.ok();
  }
}

bool RunTest3AControl(const std::string& server_exe) {
  std::cout << "\n=== TEST 3A-CONTROL: DecodeConfig Only (0 audio chunks) ==="
            << std::endl;
  ServerSubprocess srv;
  if (!srv.Start(server_exe)) {
    std::cout << "  ERROR: failed to start server" << std::endl;
    return false;
  }
  std::vector<wenet::Response> resps;
  grpc::Status st = RunProbeRpc(srv.port(), 1, 0, &resps);
  DWORD ec = srv.StopAndCleanup();

  std::cout << "  Finish(): ok=" << (st.ok() ? "true" : "false")
            << " code=" << st.error_code()
            << " responses=" << resps.size() << std::endl;
  for (const auto& r : resps) {
    std::cout << "    " << FormatResponse(r) << std::endl;
  }
  std::cout << "  server exit_code=" << ec << std::endl;
  return st.ok() && ec == 0;
}

bool RunTest3B(const std::string& server_exe, bool expect_patched) {
  std::cout << "\n=== TEST 3B: oneof Handshake Inversion (audio_data before decode_config) ["
            << (expect_patched ? "PATCHED" : "UNPATCHED") << "] ==="
            << std::endl;
  ServerSubprocess srv;
  if (!srv.Start(server_exe)) {
    std::cout << "  ERROR: failed to start server" << std::endl;
    return false;
  }

  auto run_subcase = [&](const std::string& label, bool send_second_config,
                         int trailing_audio_chunks) {
    std::cout << "\n  -- " << label << " --" << std::endl;
    auto stub = wenet::ASR::NewStub(MakeChannel(srv.port()));
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() +
                     std::chrono::seconds(5));
    auto stream = stub->Recognize(&ctx);

    wenet::Request m1;
    m1.set_audio_data(MakePcmBytes(1600, 100));
    bool w1 = stream->Write(m1);

    bool w2 = true;
    if (send_second_config) {
      wenet::Request m2;
      m2.mutable_decode_config()->set_nbest_config(2);
      m2.mutable_decode_config()->set_continuous_decoding_config(true);
      w2 = stream->Write(m2);
    }

    int written_trailing = 0;
    for (int i = 0; i < trailing_audio_chunks; ++i) {
      wenet::Request ma;
      ma.set_audio_data(MakePcmBytes(1600, 200));
      if (!stream->Write(ma)) break;
      written_trailing++;
    }
    stream->WritesDone();

    std::vector<wenet::Response> resps;
    wenet::Response resp;
    while (stream->Read(&resp)) {
      resps.push_back(resp);
      std::cout << "    " << FormatResponse(resp) << std::endl;
    }
    grpc::Status st = stream->Finish();
    std::cout << "    w1=" << w1 << " w2=" << w2
              << " trailing_written=" << written_trailing
              << " Finish(): ok=" << (st.ok() ? "true" : "false")
              << " code=" << st.error_code()
              << " total_responses=" << resps.size() << std::endl;
    return resps;
  };

  // 3B-1: Msg 1 = audio_data (1600 samples), Msg 2 = audio_data (1600 samples)
  auto r_3b1 = run_subcase("3B-1: Msg1=audio_data(1600), Msg2=audio_data(1600)",
                           false, 1);
  // 3B-2: Msg 1 = audio_data (1600 samples), no trailing audio
  auto r_3b2 = run_subcase("3B-2: Msg1=audio_data(1600) ONLY", false, 0);
  // 3B-3: Msg 1 = audio_data, Msg 2 = decode_config(nbest=2), Msg 3 = audio_data
  auto r_3b3 = run_subcase(
      "3B-3: Msg1=audio_data(1600), Msg2=decode_config(nbest=2), Msg3=audio_data(1600)",
      true, 1);

  // Verify server still alive and serves a valid request afterward
  std::vector<wenet::Response> r_after;
  grpc::Status s_after = RunProbeRpc(srv.port(), 1, 1, &r_after);
  DWORD ec = srv.StopAndCleanup();

  std::cout << "\n  Post-3B valid probe ok=" << (s_after.ok() ? "true" : "false")
            << " server exit_code=" << ec << std::endl;
  PrintServerStderr(srv.stderr_lines());

  if (!expect_patched) {
    // Expect in 3B-1: final_result received, and check nbest_size empirically
    bool saw_final_3b1 = false;
    int nbest_3b1 = -1;
    for (const auto& r : r_3b1) {
      if (r.type() == wenet::Response::final_result) {
        saw_final_3b1 = true;
        nbest_3b1 = r.nbest_size();
      }
    }
    std::cout << "  [EMPIRICAL VERDICT 3B-1 unpatched] final_result seen="
              << (saw_final_3b1 ? "true" : "false")
              << " nbest_size=" << nbest_3b1 << std::endl;
    return saw_final_3b1 && s_after.ok() && ec == 0;
  } else {
    auto has_failed_status = [](const std::vector<wenet::Response>& vec) {
      for (const auto& r : vec) {
        if (r.status() == wenet::Response::failed) return true;
      }
      return false;
    };
    return has_failed_status(r_3b1) && has_failed_status(r_3b2) &&
           has_failed_status(r_3b3) && s_after.ok() && ec == 0;
  }
}

bool RunTest3C(const std::string& server_exe, const std::string& label) {
  std::cout << "\n=== TEST 3C: Real Unmodified wenet::GrpcClient Concurrency & Lifecycle ["
            << label << "] ===" << std::endl;
  ServerSubprocess srv;
  // Use decode_delay_ms=5 to force overlapping Read() and Write() on both sides
  if (!srv.Start(server_exe, "--chunk-samples 800 --decode-delay-ms 5")) {
    std::cout << "  ERROR: failed to start server" << std::endl;
    return false;
  }

  // 3C-1: 25 sequential sessions using REAL wenet::GrpcClient
  const int kSeqRuns = 25;
  int seq_ok = 0;
  for (int run = 0; run < kSeqRuns; ++run) {
    wenet::GrpcClient client("127.0.0.1", srv.port(), 2, false);
    std::vector<int16_t> pcm(800, static_cast<int16_t>(run + 1));
    for (int chunk = 0; chunk < 6; ++chunk) {
      client.SendBinaryData(pcm.data(), pcm.size() * sizeof(int16_t));
      std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    client.Join();
    if (client.done()) {
      seq_ok++;
    }
  }
  std::cout << "  3C-1 (25 sequential GrpcClient sessions): done_count="
            << seq_ok << "/" << kSeqRuns << std::endl;

  // 3C-2: 10 concurrent GrpcClient instances in parallel threads
  const int kThreads = 10;
  std::atomic<int> par_ok{0};
  std::vector<std::thread> workers;
  workers.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    workers.emplace_back([&, t]() {
      wenet::GrpcClient client("127.0.0.1", srv.port(), 2, false);
      std::vector<int16_t> pcm(800, static_cast<int16_t>(t + 10));
      for (int chunk = 0; chunk < 8; ++chunk) {
        client.SendBinaryData(pcm.data(), pcm.size() * sizeof(int16_t));
        std::this_thread::sleep_for(std::chrono::milliseconds(3));
      }
      client.Join();
      if (client.done()) {
        par_ok++;
      }
    });
  }
  for (auto& w : workers) w.join();
  std::cout << "  3C-2 (10 parallel GrpcClient threads): done_count="
            << par_ok.load() << "/" << kThreads << std::endl;

  DWORD ec = srv.StopAndCleanup();
  std::cout << "  server exit_code=" << ec << std::endl;
  auto errs = srv.stderr_lines();
  std::cout << "  [Server stderr lines count: " << errs.size() << "]"
            << std::endl;
  // Print first 8 and last 4 lines so log remains readable
  for (size_t i = 0; i < errs.size(); ++i) {
    if (i < 6 || i + 4 >= errs.size()) {
      std::cout << "    " << errs[i] << std::endl;
    } else if (i == 6) {
      std::cout << "    ... (" << (errs.size() - 10) << " lines omitted) ..."
                << std::endl;
    }
  }
  return seq_ok == kSeqRuns && par_ok.load() == kThreads && ec == 0;
}

}  // namespace

int main(int argc, char* argv[]) {
  std::string server_exe;
  std::string mode = "unpatched";
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a == "--server-exe" && i + 1 < argc) {
      server_exe = argv[++i];
    } else if (a == "--mode" && i + 1 < argc) {
      mode = argv[++i];
    }
  }
  if (server_exe.empty()) {
    std::cerr << "Usage: wenet_test_client --server-exe <path> --mode <unpatched|patched>"
              << std::endl;
    return 2;
  }

  bool expect_patched = (mode == "patched");
  bool ok = true;

  if (!expect_patched) {
    ok &= RunTestBaseline(server_exe);
    ok &= RunTest3AControl(server_exe);
    ok &= RunTest3A(server_exe, false);
    ok &= RunTest3B(server_exe, false);
    ok &= RunTest3C(server_exe, "UNPATCHED");
  } else {
    ok &= RunTestBaseline(server_exe);
    ok &= RunTest3AControl(server_exe);
    ok &= RunTest3A(server_exe, true);
    ok &= RunTest3B(server_exe, true);
    ok &= RunTest3C(server_exe, "PATCHED");
  }

  std::cout << "\n=== SUMMARY [" << mode << "]: " << (ok ? "PASS" : "FAIL")
            << " ===" << std::endl;
  return ok ? 0 : 1;
}
