# Triton Inference Server C++ Client (`InferenceServerGrpcClient`) — Runtime Test Plan (`CD-10`, `CD-11`)

> **Provenance & Naming Note:** Tests Category D candidates **`CD-10`** and **`CD-11`**, guard control **`CTL-03`**, and incidental C++ findings **`INC-02`** / **`INC-03`** (see [`reproductions/README.md`](../README.md) and [`docs/04_Candidate_Bugs_and_Disclosure_Drafts.md`](../../docs/04_Candidate_Bugs_and_Disclosure_Drafts.md)). Mode tags (`1_guards`, `2a_*`, `2b_*`, `2c_*`, `2d_*`) are Triton CLI harness labels in `TEST_OUTPUT.txt`, **not** Categories A–D.

## 1. Target Upstream Revision & Source Locations
- **Repository:** `triton-inference-server/client` (upstream worktree branch `repro/stream-lifecycle`, commit `13095d2`).
- **Inspected Files:**
  - `src/c++/library/grpc_client.h`
    - Constructor declaration: lines 651–654
    - Stream state members: lines 667–677 (`stream_callback_`, `stream_worker_`, `grpc_stream_`, `grpc_context_`, `enable_stream_stats_` at line 674, `ongoing_stream_request_timers_` at line 675, `stream_mutex_`, `stream_cancel_requested_{false}`)
  - `src/c++/library/grpc_client.cc`
    - `StartStream()`: lines 1355–1399 (checks `stream_worker_.joinable()` at line 1360, `callback == nullptr` at line 1367, resets `grpc_context_` in-place at lines 1375–1376, sets `enable_stream_stats_ = enable_stats` at line 1379, calls `stub_->ModelStreamInfer(&grpc_context_)` at line 1390, spawns `stream_worker_` at line 1391)
    - `StopStream(bool cancel_requests)`: lines 1401–1420 (if `stream_worker_.joinable()`, cancels or calls `grpc_stream_->WritesDone()` at line 1409, joins `stream_worker_` at line 1412, does **not** reset `grpc_stream_`, `enable_stream_stats_`, or `ongoing_stream_request_timers_`)
    - `AsyncStreamInfer(...)`: lines 1422–1461 (reads `enable_stream_stats_` at lines 1428, 1439, 1443; pushes `RequestTimers` onto `ongoing_stream_request_timers_` at line 1445; dereferences `grpc_stream_->Write(infer_request_)` at line 1447 with **no** `grpc_stream_` or `stream_worker_.joinable()` guard)
    - `AsyncStreamTransfer()`: lines 1680–1735 (reads responses, pops `ongoing_stream_request_timers_` at lines 1695–1697 when `enable_stream_stats_` is true, updates `InferStat` at line 1713, calls `grpc_stream_->Finish()` at line 1734)
    - `InferenceServerGrpcClient::InferenceServerGrpcClient(...)`: lines 1737–1745 (omits `enable_stream_stats_` from member initializer list; `grpc_client.h:674` lacks a default member initializer)
    - `~InferenceServerGrpcClient()`: lines 1747–1771 (calls `StopStream()` at line 1770)

## 2. Build Strategy (Zero-Abstraction Real Upstream Code)
Rather than re-implementing or mocking `InferenceServerGrpcClient`, this reproduction compiles the **actual upstream** `src/c++/library/grpc_client.cc` and `src/c++/library/common.cc` directly with `g++ -std=c++17` against:
- Official `grpc_service.proto` and `model_config.proto` (from `triton-inference-server/common`) compiled via `protoc.exe` and `grpc_cpp_plugin.exe`
- Real `libgrpc++ 1.72.0` and `libprotobuf 31.1.0` (MSYS2 UCRT64)
- A real in-process loopback `grpc::Server` implementing `inference::GRPCInferenceService::ModelStreamInfer`

## 3. Skeptical Test Matrix

| Test ID | Scenario | Skeptical Question / Hypothesis Being Tested |
|---|---|---|
| `0_baseline` | Normal `StartStream` -> `AsyncStreamInfer` -> callback -> `StopStream` | Verify real `InferenceServerGrpcClient` + loopback `GRPCInferenceService` succeeds and records ~1 ms latency. |
| `1_guards` | `StopStream` before `StartStream`, `StartStream(nullptr)`, double `StartStream` while active, double `StopStream` | Check for false positives: confirm whether `StartStream()`/`StopStream()` already guard duplicate start/stop cleanly. |
| `2a_zero` & `2a_aa` | `AsyncStreamInfer` called **before** `StartStream` (with heap pre-filled `0x00` vs `0xAA` via `operator new` hook) | Verify empirically: (1) `enable_stream_stats_` holds indeterminate value (`0x00` vs `0xAA` alters `AsyncStreamInfer` branch/alloc count), and (2) `grpc_stream_ == nullptr` dereference at `grpc_client.cc:1447` causes `0xC0000005` (`SIGSEGV`). |
| `2b_after_stop` | `StartStream(true)` -> `AsyncStreamInfer("req-1")` -> `StopStream(false)` -> `AsyncStreamInfer("req-after-stop")` -> sleep 80 ms -> `StartStream(true)` -> `AsyncStreamInfer("req-session2")` -> `StopStream(false)` | Determine what real `libgrpc++ 1.72.0` `ClientReaderWriter::Write()` actually does after `WritesDone()` + `Finish()`: crash/abort vs returning `false` (`Error("Stream has been closed.")`) while silently pushing a phantom `RequestTimers` entry that corrupts Session 2's latency statistics (`delta_ns >= 80 ms`). |
| `2b_cancel_after_stop` | Same as `2b_after_stop`, using `StopStream(cancel_requests=true)` | Observe behavior of `Write()` after `TryCancel()` + `Finish()` and check whether phantom timer still corrupts Session 2. |
| `2d_inflight_cancel` | `StartStream(true)` -> `AsyncStreamInfer("inflight")` while server delays response -> `StopStream(true)` -> sleep 80 ms -> Session 2 `StartStream(true)` -> `AsyncStreamInfer` -> `StopStream(false)` | Probe residual edge case: does `StopStream(true)` leave unpopped timers in `ongoing_stream_request_timers_` even if `AsyncStreamInfer` is NOT called post-stop? |
| `2c_fixed` | Re-run `0_baseline`, `1_guards`, `2a_zero`, `2a_aa`, `2b_after_stop`, `2b_cancel_after_stop` against the 3-hunk patched copy (`enable_stream_stats_{false}`, `grpc_stream_.reset()` + `enable_stream_stats_ = false` in `StopStream()`, `if (!grpc_stream_ || !stream_worker_.joinable())` guard in `AsyncStreamInfer()`), plus check `2d_inflight_cancel` | Verify that all 2A/2B/2C tests pass cleanly without crashes, uninitialized branching, post-stop `Write()`, or post-stop stats corruption, and observe whether `2d_inflight_cancel` still leaks timers without draining `ongoing_stream_request_timers_`. |
| `2c_hardened` (Part C) | Re-run all tests including `2d_inflight_cancel` with `ongoing_stream_request_timers_` drained under `stream_mutex_` in `StopStream()` | Confirm complete elimination of cross-session timer leakage across cancelled in-flight streams as well. |
