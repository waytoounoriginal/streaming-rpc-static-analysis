# Skeptical Runtime Verification Findings — `triton-inference-server/client` (`InferenceServerGrpcClient`)

**Target Upstream Repository:** `triton-inference-server/client` (branch `repro/stream-lifecycle`, commit `13095d2`)  
**Runtime Environment:** Windows MSYS2 UCRT64 (`g++ 15.2.0`, `libprotoc 31.1`, `libgrpc++ 1.72.0`, real loopback `inference::GRPCInferenceService::ModelStreamInfer`)  
**Compilation Scope:** Direct compilation of unmodified upstream `src/c++/library/grpc_client.cc` and `src/c++/library/common.cc` (zero abstraction or mocking of client internals).

---

## 1. Executive Summary & Per-Claim Verdicts

| Claim / Check | Verdict | Summary of Empirical Evidence |
|---|---|---|
| **Bug 2A-1: Uninitialized `enable_stream_stats_` (`grpc_client.h:674`, `grpc_client.cc:1737–1745`)** | **CONFIRMED (Real UB / Indeterminate Memory Read)** | `bool enable_stream_stats_;` has no default member initializer (`grpc_client.h:674`) and is omitted from `InferenceServerGrpcClient::InferenceServerGrpcClient(...)` (`grpc_client.cc:1737–1745`). Compiling with `-Weffc++` warns explicitly at `grpc_client.cc:1737:1`. Poisoning heap allocations before `Create()` with `0x00` vs `0xAA` flips `AsyncStreamInfer()` control flow (`allocs_during_AsyncStreamInfer=5` vs `7`) as `if (enable_stream_stats_)` allocates `RequestTimers` and pushes to `ongoing_stream_request_timers_` under `0xAA`. |
| **Bug 2A-2: Null `grpc_stream_` Dereference before `StartStream()` (`grpc_client.cc:1447`)** | **CONFIRMED (`0xC0000005` / `SIGSEGV`)** | `AsyncStreamInfer()` (`grpc_client.cc:1422–1461`) performs **no** check on `grpc_stream_` or `stream_worker_.joinable()`. Calling `AsyncStreamInfer()` before `StartStream()` unconditionally calls `grpc_stream_->Write(infer_request_)` on a default-constructed `nullptr` `std::shared_ptr`, crashing immediately with `0xC0000005` (`STATUS_ACCESS_VIOLATION`) at `grpc_client.cc:1447` inside `WriterInterface::Write`. |
| **Bug 2B: Post-`StopStream()` Stream Reuse (`grpc_client.cc:1402–1461`)** | **CONFIRMED — With Crucial Runtime Refinement** | `StopStream()` (`grpc_client.cc:1402–1420`) calls `WritesDone()` / `TryCancel()`, joins `stream_worker_` (which calls `grpc_stream_->Finish()` at line `1734`), and **does not** call `grpc_stream_.reset()`, clear `enable_stream_stats_`, or drain `ongoing_stream_request_timers_`. **Important nuance:** On real `libgrpc++ 1.72.0`, calling `grpc_stream_->Write()` after `WritesDone()` + `Finish()` does **not** crash (`ExitCode` does not fault); `cq_.Pluck(&ops)` returns `false`, so `AsyncStreamInfer()` returns `Error("Stream has been closed.")`. **However**, because `enable_stream_stats_` remains `true`, `AsyncStreamInfer()` pushes a **phantom `RequestTimers` entry** onto `ongoing_stream_request_timers_` at line `1445` *before* `Write()` fails and **never pops it**. A subsequent `StartStream()` session inherits that stale timer, corrupting Session 2's latency statistics from ~0.6 ms to **88.459 ms** (graceful stop) / **89.4037 ms** (cancelled stop) and causing a permanent off-by-one queue shift. |
| **Check: Double `StartStream()` / Premature or Double `StopStream()`** | **NOT A BUG (Already Guarded)** | Verified as properly guarded upstream: `StartStream()` checks `if (stream_worker_.joinable())` (`grpc_client.cc:1360`), `StartStream(nullptr)` checks `if (callback == nullptr)` (`grpc_client.cc:1367`), and `StopStream()` checks `if (stream_worker_.joinable())` (`grpc_client.cc:1404`). `StartStream()` after `StopStream()` also reconstructs `grpc_context_` in-place (`grpc_client.cc:1375–1376`). |
| **Test 2C: Proposed 3-Hunk Fix** | **PASSES ALL 2A / 2B / 2C CHECKS** | Applying `enable_stream_stats_{false}`, `grpc_stream_.reset()` + `enable_stream_stats_ = false` in `StopStream()`, and `if (!grpc_stream_ \|\| !stream_worker_.joinable())` in `AsyncStreamInfer()` fixes `2a_zero`, `2a_aa`, `2b_after_stop` (`s2_delta_ms = 0.284 ms`), and `2b_cancel_after_stop` (`s2_delta_ms = 0.2875 ms`). |
| **Subtle Residual Edge Case (`2d_inflight_cancel`)** | **FOUND & VERIFIED** | Even with the 3-hunk patch, if a request is in-flight when `StopStream(/*cancel_requests=*/true)` cancels the RPC before the server responds, its `RequestTimers` entry remains in `ongoing_stream_request_timers_` and corrupts the next `StartStream()` session (`s2_delta_ms = 117.188 ms`). Draining `ongoing_stream_request_timers_` under `stream_mutex_` in `StopStream()` (Part C) eliminates this (`s2_delta_ms = 0.3312 ms`). |

---

## 2. Exact Upstream Code Audit

### 2.1 `src/c++/library/grpc_client.h`
- **Line 671:** `std::shared_ptr<grpc::ClientReaderWriter<inference::ModelInferRequest, inference::ModelStreamInferResponse>> grpc_stream_;` (default-initializes to `nullptr`).
- **Line 674:** `bool enable_stream_stats_;` — **no default member initializer**.
- **Line 675:** `std::queue<std::unique_ptr<RequestTimers>> ongoing_stream_request_timers_;`
- **Line 677:** `std::atomic<bool> stream_cancel_requested_{false};` (explicitly initialized, unlike `enable_stream_stats_`).

### 2.2 `src/c++/library/grpc_client.cc`
- **Lines 1355–1399 (`StartStream`):**
  - Line `1360`: Guards active stream reuse via `if (stream_worker_.joinable())`.
  - Line `1367`: Guards null callback via `if (callback == nullptr)`.
  - Lines `1375–1376`: Reconstructs `grpc_context_` in-place so `StartStream()` can be called again after `StopStream(true)`.
  - Line `1379`: `enable_stream_stats_ = enable_stats;`
  - Line `1390`: `grpc_stream_ = stub_->ModelStreamInfer(&grpc_context_);`
  - Does **not** clear `ongoing_stream_request_timers_`.
- **Lines 1401–1420 (`StopStream`):**
  - Line `1404`: `if (stream_worker_.joinable())`
  - Lines `1405–1410`: Either `TryCancel()` or `grpc_stream_->WritesDone()`
  - Line `1412`: `stream_worker_.join();` (`AsyncStreamTransfer()` calls `grpc_stream_->Finish()` at line `1734`)
  - Missing: `grpc_stream_.reset()`, `enable_stream_stats_ = false`, and draining `ongoing_stream_request_timers_`.
- **Lines 1422–1461 (`AsyncStreamInfer`):**
  - **Zero** validation of `grpc_stream_` or `stream_worker_.joinable()`.
  - Lines `1428, 1439, 1443`: Branches on `enable_stream_stats_`.
  - Line `1445`: `ongoing_stream_request_timers_.push(std::move(timer));` **before** calling `Write()`.
  - Line `1447`: `bool ok = grpc_stream_->Write(infer_request_);`
  - Lines `1458–1460`: On `!ok`, returns `Error("Stream has been closed.")` **without** removing the timer pushed at line `1445`.

---

## 3. Verbatim Key Runtime Observations (from `TEST_OUTPUT.txt`)

### 3.1 Bug 2A (`AsyncStreamInfer` before `StartStream`, Unpatched)
- **`2a_zero` (`0x00` heap fill):**
  - `ExitCode: -1073741819 (0xC0000005)`
  - `[SEH] ExceptionCode=0xc0000005 FaultRVA=0x14008d707 allocs_during_AsyncStreamInfer=5`
  - `addr2line(FaultRVA): grpc::internal::WriterInterface<inference::ModelInferRequest>::Write(...) at .../grpcpp/support/sync_stream.h:125`
  - `addr2line(frame 0x14000f2d4): triton::client::InferenceServerGrpcClient::AsyncStreamInfer(...) at .../src/c++/library/grpc_client.cc:1447`
- **`2a_aa` (`0xAA` heap fill):**
  - `ExitCode: -1073741819 (0xC0000005)`
  - `[SEH] ExceptionCode=0xc0000005 FaultRVA=0x14008d707 allocs_during_AsyncStreamInfer=7`
  - The jump from `5` to `7` heap allocations proves `enable_stream_stats_` retained `0xAA` (`true`) and executed `new RequestTimers()` + `std::vector<uint64_t>` allocation before dereferencing null `grpc_stream_` at line `1447`.

### 3.2 Bug 2B (`AsyncStreamInfer` after `StopStream`, Unpatched)
- **`2b_after_stop`:**
  - `[2b_after_stop] After StopStream(false): completed=1 s1_ms=0.6314`
  - `[2b_after_stop] AsyncStreamInfer AFTER StopStream: ok=0 msg='Stream has been closed.' allocs_during_AsyncStreamInfer=7`
  - `[2b_after_stop] Session2 StartStream ok=1 AsyncStreamInfer ok=1 completed=2 s2_delta_ms=88.459`
- **`2b_cancel_after_stop`:**
  - `[2b_cancel_after_stop] After StopStream(true): completed=1 s1_ms=0.6187`
  - `[2b_cancel_after_stop] AsyncStreamInfer AFTER StopStream: ok=0 msg='Stream has been closed.' allocs_during_AsyncStreamInfer=7`
  - `[2b_cancel_after_stop] Session2 StartStream ok=1 AsyncStreamInfer ok=1 completed=2 s2_delta_ms=89.4037`

### 3.3 False-Positive Check on `libgrpc++ 1.72.0` Behavior & Documentation Precision
- In `docs/04_Candidate_Bugs_and_Disclosure_Drafts.md`, Bug 2B accurately flags post-`StopStream` `Write()` on a finished stream (`S2_FINISHED`), and the proposed patch fixes it. However, one subtle empirical fact should be reflected in any upstream bug report or thesis prose:
  - Calling `grpc::ClientReaderWriter::Write()` after `WritesDone()` + `Finish()` on `libgrpc++ 1.72.0` does **not** crash or trigger `ABSL_CHECK`—`cq_.Pluck(&ops)` returns `false` and `AsyncStreamInfer()` returns `Error("Stream has been closed.")`.
  - The **concrete runtime impact** of omitting `grpc_stream_.reset()`, `enable_stream_stats_ = false`, and the `AsyncStreamInfer()` active-stream guard is **silent statistics queue corruption** across stream sessions (`~88–90 ms` fabricated latency + permanent off-by-one `RequestTimers` misalignment), alongside violating gRPC's `ClientReaderWriter` post-`Finish()` API contract.

### 3.4 Verification of Fixes (Parts B & C)
- **Part B (Exact 3-Hunk Fix in `triton_client_fix.patch`):**
  - `0_baseline`: `ExitCode: 0`, `completed=1 total_ms=0.5489`
  - `1_guards`: `ExitCode: 0`
  - `2a_zero`: `ExitCode: 0`, `ok=0 msg='Stream is not active; call StartStream() before AsyncStreamInfer().' allocs_during_AsyncStreamInfer=3`
  - `2a_aa`: `ExitCode: 0`, `ok=0 msg='Stream is not active; call StartStream() before AsyncStreamInfer().' allocs_during_AsyncStreamInfer=3`
  - `2b_after_stop`: `ExitCode: 0`, `s2_delta_ms=0.284`
  - `2b_cancel_after_stop`: `ExitCode: 0`, `s2_delta_ms=0.2875`
  - `2d_inflight_cancel`: `ExitCode: 1`, `s2_delta_ms=117.188` (residual orphaned timer from cancelled in-flight request)
- **Part C (Hardened Fix: 3-Hunk Patch + Draining `ongoing_stream_request_timers_` in `StopStream()`):**
  - All 7 modes (`0_baseline`, `1_guards`, `2a_zero`, `2a_aa`, `2b_after_stop`, `2b_cancel_after_stop`, **and `2d_inflight_cancel`** with `s2_delta_ms=0.3312`) exit with `0`.
