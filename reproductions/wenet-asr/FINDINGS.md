# WeNet ASR (`GrpcConnectionHandler` & `GrpcClient`) — Real `libgrpc++ 1.72.0` Runtime Verification Findings

## 1. Summary Verdicts

All runtime tests compiled the **real upstream `wenet-e2e/wenet`** sources (`runtime/core/grpc/wenet.proto`, `grpc_server.cc`, `grpc_client.cc`) against **real `libgrpc++ 1.72.0`** and **`libprotoc 31.1`** over loopback TCP (`127.0.0.1:0`), replacing only `decoder/asr_decoder.h`, `frontend/feature_pipeline.h`, and `utils/log.h` with lightweight stubs preserving `glog` `CHECK(...) -> std::abort()` semantics.

| Test | Claim Evaluated | Verdict | Key Empirical Observation |
| :--- | :--- | :--- | :--- |
| **Test 3A** | Empty stream (`WritesDone()` with 0 messages) crashes WeNet server via `CHECK(feature_pipeline_ != nullptr)` | **TRUE POSITIVE (Confirmed DoS)** | Server aborted immediately (`exit_code = 3`, `Check failed: feature_pipeline_ != nullptr` at `grpc_server.cc:52`); active RPC returned `UNAVAILABLE` (`code=14`, WinError `10054`), and subsequent probe RPC failed with `Connection refused` (`10061`). |
| **Test 3B** | `oneof` handshake inversion (`audio_data` before `decode_config`): (a) missing `has_decode_config()` silently drops audio and overwrites `nbest_` to `0`; (b) `nbest_ == 0` causes `SerializeResult()` to return `0` hypotheses despite decoding | **PARTIAL TRUE POSITIVE + EMPIRICAL CORRECTION** | **(a) Confirmed:** Msg #1's `1600` audio samples were silently dropped (`accept_calls=1 total_samples=1600` in 3B-1; `0` in 3B-2), `nbest_` was overwritten from `1` to `0` (`DecodeConfig::default_instance()`), and trailing `decode_config` in 3B-3 was misparsed as 0-byte audio.<br>**(b) Refuted / Inverse Bug:** When decode runs (`result_count=3`), `nbest_ == 0` did **not** yield `nbest_size() == 0`; instead `SerializeResult()` returned **all `3` hypotheses** (`nbest_size=3`) because `response_->add_nbest()` increments `nbest_size()` to `1..K` *before* checking `if (response_->nbest_size() == nbest_) break;` (`1 == 0` is never true). `nbest_size() == 0` only occurs in 3B-2 because Msg #1 was the sole audio message and was discarded. |
| **Test 3C** | Concurrent `Write()` vs `Read()` in `GrpcConnectionHandler` and lifecycle/threading in `GrpcClient` (false-positive check) | **CONFIRMED NON-ISSUE FOR gRPC STREAM OPS** (with minor `done_` atomicity note) | Across 25 sequential sessions + 10 parallel client threads using unmodified `wenet::GrpcClient` (`25/25`, `10/10`, `exit_code=0`), `OnSpeechStart()` writes `server_ready` *before* spawning `decode_thread_`, `GrpcClient::Connect()` writes `decode_config` *before* spawning `t_`, and `GrpcClient::Join()` executes `WritesDone()` -> `t_->join()` -> `Finish()`. One non-gRPC nuance observed by inspection: `GrpcClient::done_` (`grpc_client.h:62`) is a plain `bool` (non-atomic) when polled concurrently (`grpc_client_main.cc:43`). |
| **Test 3D** | Proposed fix (`if (!request_->has_decode_config())` + `if (got_start_tag_ && feature_pipeline_ != nullptr) OnSpeechEnd();`) on `repro/grpc-server-checks` (`0b25ae8`) | **CONFIRMED EFFECTIVE** (with 2 schema/edge-case notes) | Empty stream (3A) completed with `Status::OK` and left server healthy (`probe ok=true`). All `audio_data`-first cases (3B-1/2/3) returned `Response{status=failed}` cleanly without crashing or dropping into decode, and 3C passed (`25/25`, `10/10`). Note: `docs/04_Candidate_Bugs_and_Disclosure_Drafts.md:261` used `Response::FAILED` (does not compile; enum in `wenet.proto:53` is lowercase `Response::failed`). |

---

## 2. Detailed Empirical Evidence

### Baseline & Control Behavior (`d170596`)
- **Conforming Client (`Baseline Control`):**
  - `nbest_config = 1` with 2 audio chunks (`1600` samples): `rpc status=0 responses=5 final_nbest_size=1` (`"hello world"`).
  - `nbest_config = 2` with 2 audio chunks (`1600` samples): `rpc status=0 responses=5 final_nbest_size=2` (`"hello world"`, `"hello ward"`).
- **Control `3A-Control` (`decode_config` sent, 0 audio chunks, `WritesDone()`):**
  - Completed cleanly with `Finish(): ok=true code=0 responses=3` (`server_ready`, `final_result nbest_size=0`, `speech_end`) and `server exit_code=0`, confirming the crash is strictly caused by skipping `OnSpeechStart()` on an empty stream.

### Test 3A — Empty Client Stream Process Abort (`d170596`)
- Client opened `stub->Recognize(&ctx)` and immediately invoked `stream->WritesDone()` then `stream->Finish()` (0 `Request` messages).
- Server output (`TEST_OUTPUT.txt` lines 54–57):
  ```text
  [I ...grpc_server.cc:167] Get Recognize request
  [I ...grpc_server.cc:51] Received speech end signal
  [F ...grpc_server.cc:52] Check failed: feature_pipeline_ != nullptr
  ```
- Client `Finish()` failed with `code=14` (`UNAVAILABLE`, WinError `10054: Connection reset`), server subprocess exited spontaneously with `exit_code=3 / 0x3` (UCRT `std::abort()`), and a follow-up probe RPC failed with `code=14` (`WinError 10061: Connection refused`).

### Test 3B — `oneof` Handshake Inversion (`d170596`)
- **Subcase 3B-1 (`Msg1 = audio_data(1600)`, `Msg2 = audio_data(1600)`):**
  - Decoder telemetry logged: `Rescoring accept_calls=1 total_samples=1600 chunks_decoded=2 result_count=3`.
  - Msg #1's 1600 samples were **silently dropped** because `operator()` (`grpc_server.cc:146–150`) treats the first message unconditionally as `decode_config`.
  - Because `Msg1` had `audio_data` active in `oneof RequestPayload`, `request_->decode_config()` returned `DecodeConfig::default_instance()` (`nbest_config() == 0`, `continuous_decoding_config() == false`), overwriting `GrpcConnectionHandler::nbest_` from `1` to `0`.
  - On the wire, `partial_result` and `final_result` both carried **`nbest_size=3`** (`["hello world", "hello ward", "halo world"]`) instead of `0` or `1`! Inspection of `SerializeResult()` (`grpc_server.cc:90–104`) confirms why:
    ```cpp
    Response_OneBest* one_best_ = response_->add_nbest();
    ...
    if (response_->nbest_size() == nbest_) {
      break;
    }
    ```
    Because `response_->add_nbest()` increments `nbest_size()` to `1` before the comparison `response_->nbest_size() == nbest_`, `nbest_ == 0` disables the upper bound entirely and emits every hypothesis in `decoder_->result()`.
- **Subcase 3B-2 (`Msg1 = audio_data(1600)` ONLY):**
  - Decoder telemetry logged: `Rescoring accept_calls=0 total_samples=0 chunks_decoded=0 result_count=0`.
  - Returned `final_result` with `nbest_size=0` strictly due to 100% silent drop of the sole audio message.
- **Subcase 3B-3 (`Msg1 = audio_data(1600)`, `Msg2 = decode_config(nbest=2, continuous=true)`, `Msg3 = audio_data(1600)`):**
  - Decoder telemetry logged: `accept_calls=2 total_samples=1600 chunks_decoded=2 result_count=3`.
  - Msg #1 was dropped (`nbest_` set to `0`); Msg #2 entered `OnSpeechData()` (`grpc_server.cc:78–87`), where `request_->audio_data()` returned empty `""` (`0` samples) and the requested `nbest=2` / `continuous_decoding=true` settings were completely ignored; Msg #3 triggered decode returning all `3` hypotheses (`nbest_size=3`).

### Test 3C — Concurrency & Client Lifecycle Verification
- **Server (`GrpcConnectionHandler`):**
  - `OnSpeechStart()` (`grpc_server.cc:36–48`) mutates `response_` and calls `stream_->Write(*response_)` (`server_ready`) **before** constructing `decode_thread_`.
  - After `decode_thread_` starts, `operator()()` only calls `stream_->Read(...)` and `feature_pipeline_->AcceptWaveform(...)` / `set_input_finished()`, whereas `DecodeThreadFunc()` only mutates `response_` and calls `stream_->Write(...)`. One concurrent `Read()` and one concurrent `Write()` satisfy `grpc::ServerReaderWriter` thread-safety rules.
- **Client (`GrpcClient`):**
  - Unmodified `grpc_client.cc` passed `25/25` sequential sessions (`3C-1`) and `10/10` parallel multi-threaded client sessions (`3C-2`) with `decode_delay_ms=5` forcing concurrent `Read()`/`Write()` overlap, exiting cleanly with `server exit_code=0`.
  - Breaking out of `GrpcClient::ReadLoopFunc()` upon `Response_Type_speech_end` (prior to `stream_->Read()` returning `false`) did not interfere with `stream_->Finish()` in `GrpcClient::Join()` under `libgrpc++ 1.72.0`.
  - **Minor non-gRPC nuance (by source inspection):** `GrpcClient::done_` (`grpc_client.h:62`) is declared as `bool done_ = false;` rather than `std::atomic<bool>`. In `grpc_client_main.cc:43`, the main thread polls `if (client.done())` while `ReadLoopFunc()` sets `done_ = true;` (`grpc_client.cc:68`), which is a benign-in-practice but formally UB C++ data race on `done_` (not a gRPC typestate or `Finish()`-vs-`Read()` race). Also, destroying `GrpcClient` without calling `Join()` leaves `t_` joinable (`std::terminate()`).

### Test 3D — Patched Verification (`repro/grpc-server-checks`, commit `0b25ae8`)
- Patch applied to `runtime/core/grpc/grpc_server.cc`:
  1. Check `if (!request_->has_decode_config())` on first message -> log `WARNING`, `response_->set_status(Response::failed);`, `stream_->Write(*response_);`, `return;`.
  2. Guard `if (got_start_tag_ && feature_pipeline_ != nullptr) { OnSpeechEnd(); }`.
- **Verified Outcomes:**
  - `Test 3A` (empty stream): `Empty-stream Finish(): ok=true code=0`, `Server exited spontaneously: NO`, `Subsequent health-probe RPC: ok=true`, `Graceful shutdown exit_code=0`.
  - `Test 3B-1/2/3`: Every audio-first request received `Response{status=failed, type=server_ready, nbest_size=0}`, logged `[W] First message on Recognize stream must contain decode_config`, performed zero decoding, and kept the server healthy (`Post-3B valid probe ok=true server exit_code=0`).
  - `Test 3C`: `25/25` sequential and `10/10` parallel `GrpcClient` sessions succeeded (`done_count=25/25`, `10/10`, `server exit_code=0`).
- **Additional Disclosure/Patch Notes for Upstream PR:**
  1. **Disclosure typo fixed:** `docs/04_Candidate_Bugs_and_Disclosure_Drafts.md:261` specified `Response::FAILED`, which fails compilation against `wenet.proto:53` (`enum Status { ok = 0; failed = 1; }`). The working symbol is lowercase `Response::failed`.
  2. **`Response::Type` on failure:** Because `wenet.proto` defines `enum Type { server_ready = 0; ... }` with no `unspecified`/`error` value, sending `Response` with only `set_status(Response::failed)` defaults `type` to `0` (`server_ready`). `GrpcClient::ReadLoopFunc()` (`grpc_client.cc:64`) still handles this properly because it checks `response_->status() != Response_Status_ok` before inspecting `type()`.
  3. **Zero `nbest_config` edge case:** Even with `has_decode_config()` validated, a client sending `DecodeConfig{}` with `nbest_config = 0` (proto3 default) still causes `SerializeResult()` to bypass truncation due to post-increment comparison `response_->nbest_size() == nbest_`; clamping `if (nbest_ <= 0) nbest_ = 1;` (or rejecting `nbest_config <= 0`) is recommended alongside the guard.
