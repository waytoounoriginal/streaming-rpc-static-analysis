# WeNet ASR (`GrpcConnectionHandler` & `GrpcClient`) Verification Test Plan (`CD-12`, `CD-13`)

> **Provenance & Naming Note:** Tests Category D candidates **`CD-12`** and **`CD-13`**, plus concurrency control **`CTL-04`** (see [`reproductions/README.md`](../README.md) and [`docs/04_Candidate_Bugs_and_Disclosure_Drafts.md`](../../docs/04_Candidate_Bugs_and_Disclosure_Drafts.md)). Legacy labels `Test 3A..3D` match `TEST_OUTPUT.txt` harness markers, **not** provenance Categories A–D.

## 1. Objective & Scope

This test plan verifies candidate protocol-conformance and concurrency claims for `wenet-e2e/wenet`'s synchronous C++ gRPC streaming implementation (`runtime/core/grpc/grpc_server.cc`, `runtime/core/grpc/grpc_client.cc`, `runtime/core/grpc/wenet.proto`) against **real `libgrpc++ 1.72.0`**, **real `protoc 31.1` + `grpc_cpp_plugin`**, and the **unmodified upstream C++ server/client sources** (with lightweight stubs replacing only `decoder/asr_decoder.h`, `frontend/feature_pipeline.h`, and `utils/log.h`).

We seek ground-truth empirical results—explicitly checking whether any part of a prior static-analysis claim is a **true positive**, **partially inaccurate / nuanced**, or a **false positive**.

## 2. System Under Test (SUT) & Harness Architecture

- **Upstream target (`wenet-e2e/wenet`):** pinned base commit `d170596` (auto-fetched into `_build/upstream-wenet` unless `-UpstreamWenetDir` is supplied)
- **Files compiled directly from upstream without modification (baseline):**
  - `runtime/core/grpc/wenet.proto` -> compiled with `protoc.exe` + `grpc_cpp_plugin.exe` to `wenet.pb.{h,cc}` and `wenet.grpc.pb.{h,cc}`
  - `runtime/core/grpc/grpc_server.h`, `runtime/core/grpc/grpc_server.cc`
  - `runtime/core/grpc/grpc_client.h`, `runtime/core/grpc/grpc_client.cc`
  - `runtime/core/utils/utils.h`
- **Lightweight stub headers (`harness_stubs/`):**
  - `utils/log.h`: Google `glog`-compatible `LOG(INFO|WARNING|ERROR|FATAL)`, `VLOG(level)`, `CHECK(cond)` (prints `FATAL` and invokes `std::abort()` on failure, mirroring `glog`), and `DISALLOW_COPY_AND_ASSIGN`.
  - `frontend/feature_pipeline.h`: Minimal thread-safe `FeaturePipeline` preserving `AcceptWaveform(const int16_t*, int)`, `set_input_finished()`, and blocking chunk consumption with mutex/condition variable; tracks `accept_calls()` and `total_samples()` for test observability.
  - `decoder/asr_decoder.h`: Minimal `AsrDecoder`, `DecodeOptions`, `DecodeResource`, `DecodeResult`, `WordPiece`, and `DecodeState` matching upstream `grpc_server.cc` signatures. Emits deterministic partial/final hypotheses (`K = 3` beam candidates when `total_samples > 0`, `0` candidates when `total_samples == 0`) so `SerializeResult()` and `nbest_` truncation behavior can be observed directly on the wire.
- **Transport:** Real gRPC C++ `ServerBuilder` over loopback TCP (`127.0.0.1:0`, OS-assigned ephemeral port).
- **Subprocess isolation:** The gRPC server runs in a dedicated child process (`--server`) so `std::abort()` / `SIGABRT` crashes in Test 3A can be verified cleanly via child exit code, stderr diagnostics, and subsequent connection failure.

## 3. Test Matrix

| ID | Target | Procedure | Hypotheses & Verification Criteria |
| :--- | :--- | :--- | :--- |
| **Test 3A** | Empty Stream Server Crash (`grpc_server.cc:52, 155`) | Real `wenet::ASR::Stub` calls `Recognize(&ctx)`, immediately calls `stream->WritesDone()`, then `stream->Finish()` (0 messages sent). Followed by a health-probe RPC to confirm server availability/death. | **Hypothesis:** `stream_->Read()` returns `false` on iteration 0; `OnSpeechEnd()` runs with `feature_pipeline_ == nullptr`; `CHECK(feature_pipeline_ != nullptr)` calls `std::abort()`, crashing the entire server process and breaking subsequent RPCs. Also run **3A-control** (`decode_config` sent, 0 audio messages, `WritesDone()`), expecting `Status::OK` and no crash. |
| **Test 3B** | `oneof` Handshake Inversion (`grpc_server.cc:146-153, 89-106`) | **3B-1:** Client sends `Request` #1 with `set_audio_data(...)` (1600 samples, omitting `decode_config`), then `Request` #2 with `set_audio_data(...)` (1600 samples), then `WritesDone()`.<br>**3B-2:** Client sends only 1 `Request` with `set_audio_data(...)` (1600 samples), then `WritesDone()`.<br>**3B-3:** Client sends Msg #1 `audio_data`, Msg #2 `decode_config{nbest=2, continuous=true}`, Msg #3 `audio_data`, then `WritesDone()`. | **Hypotheses to test skeptically:**<br>1. Does `grpc_server.cc` omit `has_decode_config()` and overwrite `nbest_` from `1` to `0` (`DecodeConfig::default_instance()`)?<br>2. Is Msg #1's audio silently dropped (`total_samples` excludes chunk 1)?<br>3. Does `nbest_ == 0` cause `SerializeResult()` to return `0` hypotheses (`nbest_size() == 0`) despite running decode, **OR** does `add_nbest()` before `if (response_->nbest_size() == nbest_) break;` cause `SerializeResult()` to bypass the `nbest_` cap and emit **all `K` hypotheses** (`1 == 0` is false)? |
| **Test 3C** | False-Positive & Concurrency Check (`grpc_server.cc` & `grpc_client.cc`) | **3C-1:** Instantiate unmodified `wenet::GrpcClient`, stream 10 audio chunks via `SendBinaryData()` concurrently with `ReadLoopFunc()` while server emits `server_ready` + multiple `partial_result` + `final_result` + `speech_end`, then call `client.Join()`. Repeat across 25 sequential sessions.<br>**3C-2:** Run 10 parallel `wenet::GrpcClient` instances concurrently against one `GrpcServer` instance. | **Hypothesis:** Verify whether earlier assessment holds: `OnSpeechStart()` writes `server_ready` *before* spawning `decode_thread_`; after spawn, `operator()()` only calls `Read()` while `DecodeThreadFunc()` only calls `Write()`; `GrpcClient::Connect()` writes `decode_config` *before* spawning `t_`; `GrpcClient::Join()` calls `WritesDone()`, joins `t_`, and only then calls `Finish()`. Also check non-atomic `GrpcClient::done_` and `ReadLoopFunc` early break before `Read()==false` for subtle defects. |
| **Test 3D** | Proposed Defensive Fix (`wenet_server_fix.patch`) | Apply `wenet_server_fix.patch` (`if (!request_->has_decode_config())` rejecting with `Response::failed`, plus `if (got_start_tag_ && feature_pipeline_ != nullptr) OnSpeechEnd();`) to upstream `runtime/core/grpc/grpc_server.cc`, rebuild server directly from the patched upstream file, and re-run **3A**, **3B-1/2/3**, and **3C-1/2**. | **Criteria:**<br>- Empty stream (3A) does NOT abort/crash the server; subsequent health-probe RPC succeeds.<br>- `audio_data` before `decode_config` (3B-1/2/3) returns `Response{status = Response::failed}` cleanly without overwriting `nbest_`, without dropping audio into decode, and without crashing.<br>- Conforming `GrpcClient` traffic (3C) continues to succeed end-to-end. |

## 4. Acceptance & Reporting Standards

- Verbatim stdout/stderr outputs, exact Windows process exit codes, and observed wire `Response` counts/fields will be saved to `TEST_OUTPUT.txt` and analyzed in `FINDINGS.md`.
- Every hypothesis must be judged strictly by observed runtime evidence.
