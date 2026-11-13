# Candidate Static-Analysis Findings & Upstream Disclosure Drafts

This document tracks protocol-conformance defects and lifecycle issues identified across open-source C++ gRPC repositories during our static analysis evaluation.

For academic and reporting integrity, we separate **evidence provenance categories** from **canonical bug/check IDs**:

### Evidence Provenance Categories (A–D)
- **Category A — Confirmed Historical Defects:** Upstream issues/PRs already acknowledged or patched in repository version history (e.g., `nvidia-riva/cpp-clients` PR #56/#57, WeNet WebSocket Issue #813 / PR #911).
- **Category B — Production Schema Invariants:** Ordering/`oneof` contracts documented in active `.proto` schemas (`storage.proto`, `cloud_speech.proto`, `local_cas.proto`, `wenet.proto`).
- **Category C — Synthetic Fault Injection:** Controlled mutations for checker unit evaluation (`poc/client_bad_*.cpp`).
- **Category D — Candidate Upstream Findings (`[CANDIDATE / UNCONFIRMED UPSTREAM]`):** Unconfirmed protocol-conformance defects in current upstream C++ revisions. Local `libgrpc++ 1.72.0` runtime verification confirms reproducibility (`Reproduced locally`), **not** upstream maintainer confirmation.

### Canonical ID Naming Convention
To avoid any ambiguity between Categories **A–D** and numbered test items (legacy harnesses previously used repo-prefixed labels like `1A`, `2B`, `3D`, `4A`), every finding or control in `docs/`, `reproductions/`, and `latex/` uses non-colliding canonical IDs:
- **`CD-01`..`CD-13`** — **Category D** gRPC streaming protocol/typestate candidate bugs (`CD` = *Category D*).
- **`CTL-01`..`CTL-04`** — **Conforming / Guard Controls** (True Negatives verified not to be bugs).
- **`FP-01`..`FP-02`** — **False Positives / Retracted Claims** (dead code or misattributed earlier claims).
- **`INC-01`..`INC-03`** — **Incidental Non-Protocol C++ Bugs** noticed during auditing/testing (outside session-type scope).

---

## 1. Master Registry of Category D Candidate Bugs & Related Checks

### 1.1 Category D Candidate Protocol Bugs (`CD-01`..`CD-13` — All `[UNCONFIRMED UPSTREAM]`)

| Canonical ID | Repository & Location | Level | Analyzer Detected? | Local Runtime Verdict | Summary & Legacy Harness Tag |
| :--- | :--- | :---: | :---: | :--- | :--- |
| **`CD-01`** | **`grpc/grpc`**: `examples/cpp/interceptors/caching_interceptor.h:39–105` | L1 | Yes (`39:1`) | **Reproduced** | `CachingInterceptor::Intercept` omits `stream_->Finish()`, masks status to `OK` (`PRE_RECV_STATUS`), caches empty reads, fires `RST_STREAM(8)`, skips `POST_RECV_STATUS` *(repro tags: `T0a`, `T0b`, `T1`–`T7`)*. |
| **`CD-02`** | **`apache/arrow`**: `grpc_client.cc:907` (`ListFlights`) | L1 | Yes (`907:9`) | **Reproduced** | `RETURN_NOT_OK(internal::FromProto(...))` skips `Finish()`, dropping `POST_RECV_STATUS` *(legacy: `Bug 1A` / `T1b`, `T1c`)*. |
| **`CD-03`** | **`apache/arrow`**: `grpc_client.cc:910–911` (`ListFlights`) | L1 | Yes (`911:9`) | **Reproduced** | `TryCancel(); RETURN_NOT_OK(stop_token.Poll())` skips `Finish()` (`POST_RECV_STATUS=0`) *(legacy: `Bug 1B` / `T2a`, `T2b`)*. |
| **`CD-04`** | **`apache/arrow`**: `grpc_client.cc:938–939` (`ListActions`) | L1 | Yes (`939:9`) | **Reproduced** | `TryCancel(); RETURN_NOT_OK(stop_token.Poll())` skips `Finish()` (`POST_RECV_STATUS=0`) *(legacy: `Bug 1D` / `T3b`)*. |
| **`CD-05`** | **`apache/arrow`**: `grpc_client.cc:1074` (`AuthenticateInternal`) | L1 | Yes (`1074:9`) | **Reproduced** | `RETURN_NOT_OK(auth_handler_->Authenticate(...))` skips `Finish()` *(legacy: `Bug 1E-R1` / `T4b`)*. |
| **`CD-06`** | **`apache/arrow`**: `grpc_client.cc:1077–1081` (`AuthenticateInternal`) | L1 | Yes (`1079:9`) | **Partial** *(unreached on clean HTTP/2 close)* | `if (!finished_writes)` omits `Finish()` syntactically (`WritesDone()` returned `true` in `libgrpc++ 1.72.0` loopback) *(legacy: `Bug 1E-R2` / `T4c`)*. |
| **`CD-07`** | **`BuildGrid/buildbox`**: `buildboxcommon_grpccasclient.cpp:1317–1323` | L1 | Yes (`1317:1`) | **Reproduced** *(async `casd` nuance)* | `~GrpcStagedDirectory()` skips cleanup `Read()` + `Finish()`, causing `RST_STREAM(8)`, dropped ack/`INTERNAL`, `POST_RECV_STATUS=0`, and restage races (`casd` deletes async) *(legacy: `Bug 4A` / `Suite 1`)*. |
| **`CD-08`** | **`BuildGrid/buildbox`**: `buildboxcommon_grpccasclient.cpp:534, 546` | L1 | Yes (`534, 546`) | **Reproduced** | Uncaught `std::system_error` / `runtime_error` in `uploadFdRegion()` unwinds `Write()` stream without `Finish()` *(legacy: `Bug 4B` / `Suite 2A`)*. |
| **`CD-09`** | **`BuildGrid/buildbox`**: `buildboxcommon_grpccasclient.cpp:381` | L1 | Yes (`381:9`) | **Reproduced** | Uncaught `std::system_error` in `download()` unwinds `Read()` stream without `Finish()` *(legacy: `Bug 4B` / `Suite 2B`)*. |
| **`CD-10`** | **`triton-client`**: `grpc_client.cc:1447` (`AsyncStreamInfer` pre-`StartStream`) | L1 | Audited *(modeled as `S_UNINITIALIZED`)* | **Reproduced** (`0xC0000005`) | Calling `AsyncStreamInfer()` before `StartStream()` invokes `grpc_stream_->Write()` on null pointer *(legacy: `Bug 2A-2` / `2a_zero`, `2a_aa`)*. |
| **`CD-11`** | **`triton-client`**: `grpc_client.cc:1447` (`AsyncStreamInfer` post-`StopStream`) | L1 | Yes (`1447:9`) | **Reproduced** *(returns closed + timer skew)* | `StopStream()` leaves `grpc_stream_` non-null; post-`Finish()` `Write()` returns `"Stream has been closed."` and leaks a phantom `RequestTimers` entry at line `1445` *(legacy: `Bug 2B` / `2b_after_stop`)*. |
| **`CD-12`** | **`wenet-e2e/wenet`**: `grpc_server.cc:155 -> 52` (`operator()`) | L1 | Yes (`155:9`) | **Reproduced** (`exit_code=3`) | Empty stream (`WritesDone()` with 0 msgs) calls `OnSpeechEnd()` with `feature_pipeline_ == nullptr` (`CHECK` abort); Cat. A WebSocket twin in PR #911 *(legacy: `Test 3A`)*. |
| **`CD-13`** | **`wenet-e2e/wenet`**: `grpc_server.cc:146–150` (`operator()`) | L2 | Yes (`S0_WAIT_CONFIG` spec) | **Reproduced** *(inverse `nbest_=0` leak)* | Missing `has_decode_config()` check drops 1st `audio_data` chunk and resets `nbest_=0`, causing `SerializeResult()` to emit all `K=3` candidates *(legacy: `Test 3B`)*. |

### 1.2 Non-D Checks Tested Alongside Category D (Controls, False Positives & Incidental C++ Bugs)

| ID | Category / Role | Repository & Location | Status & Mapping Note |
| :--- | :--- | :--- | :--- |
| **`CTL-01`** | Conforming Control (`TN`) | **`apache/arrow`**: `GrpcResultStream` (`504–559`), `FinishableDataStream::DoFinish` (`271`) | Verified conforming (`T0a`, `T0b`). |
| **`CTL-02`** | Conforming Control (`TN`) | **`BuildGrid/buildbox`**: `GrpcCASClient::upload(string)` (`433–505`) | Verified conforming (`Suite 2C`). |
| **`CTL-03`** | Guard Non-Bug Control | **`triton-client`**: `StartStream`/`StopStream` guards (`1360, 1367, 1404`) | Properly guarded upstream (`1_guards`). |
| **`CTL-04`** | Non-Issue Control (`TN`) | **`wenet-e2e/wenet`**: `GrpcClient` concurrent `Read`/`Write` | Passes `25/25` sequential + `10/10` parallel (`Test 3C`; non-atomic `bool done_` noted). |
| **`FP-01`** | Unreachable AST False Positive | **`apache/arrow`**: `ListActions` (`grpc_client.cc:935`) | Syntactically flagged by AST checker, unreachable because `FromProto(ActionType)` always returns `Status::OK()` *(legacy: `Bug 1C` / `T3a`)*. |
| **`FP-02`** | Retracted Earlier Claim | **`apache/arrow`**: `AuthenticateInternal` (`592–624` wrong lines) | Retracted as fabricated (`592–624` is `GrpcGarbageBin`; real line `1089` calls `Finish()`) *(legacy: `Bug 1E` / `T4a`)*. |
| **`INC-01`** | Incidental Non-Protocol Bug | **`BuildGrid/buildbox`**: `CasInstance::processWriteRequest` (`casd_casinstance.cpp:196–200`) | Missing `return` on temporary `grpc::Status(OUT_OF_RANGE, ...)` (`Suite 3A`–`3C`). |
| **`INC-02`** | Incidental C++ UB | **`triton-client`**: `enable_stream_stats_` (`grpc_client.h:674`, `1737–1745`) | Uninitialized `bool` member (`-Weffc++`) *(legacy: `Bug 2A-1`)*. |
| **`INC-03`** | Incidental Queue Cleanup Bug | **`triton-client`**: `ongoing_stream_request_timers_` (`grpc_client.cc:1402–1420`) | Stale timer retained on `StopStream(true)` mid-flight cancel (`2d_inflight_cancel`). |

### 1.3 Other Evaluated Items Outside the 5 `reproductions/` Harnesses
- **Category A (Confirmed Historical):** `nvidia-riva/cpp-clients` (`streaming_s2s_client.cc`, PR #56/#57 concurrent `Finish()`/`Read()` race); `wenet` WebSocket PR #911.
- **Conforming Controls Only (`google-cloud-cpp`):** `GrpcStub::InsertObjectMedia` (`stub.cc:325`) & `UploadChunk` (`stub.cc:620`) passed static checks with zero false alarms (`TN`).
- **Locally Archived / Excluded:** `P4 PI` (`SimpleRouterMgr` joinable thread) and `Stratum` (`AdminService::SetPackage` laxity) are excluded from the thesis reproduction set (`repro/p4-pi-stratum` remains local/archived).

---

## 2. Detailed Technical Analysis & Reproducers

### 1. Apache Arrow Flight (`apache/arrow`) — [`CD-02`..`CD-06`, `FP-01`, `FP-02`]
* **Source:** `cpp/src/arrow/flight/transport/grpc/grpc_client.cc`
* **Root Cause:**
  Arrow Flight includes higher-level RAII abstractions (`GrpcResultStream` and `FinishableDataStream`, control **`CTL-01`**) that reliably call `rpc_.context.TryCancel()` and `stream_->Finish()` upon destruction.
  
  However, `GrpcClientImpl::ListFlights` (lines 892–914) and `GrpcClientImpl::ListActions` (lines 923–941) bypass these helpers and manage raw `std::unique_ptr<grpc::ClientReader<...>>` handles directly (while `AuthenticateInternal` at lines 1067–1091 manages a raw `ClientReaderWriter`):

  ```cpp
  std::unique_ptr<::grpc::ClientReader<pb::FlightInfo>> stream(
      stub_->ListFlights(&rpc.context, pb_criteria));

  std::vector<FlightInfo> flights;
  pb::FlightInfo pb_info;
  while (!options.stop_token.IsStopRequested() && stream->Read(&pb_info)) {
    FlightInfo::Data info_data;
    RETURN_NOT_OK(internal::FromProto(pb_info, &info_data)); // Early return 1 (line 907)
    flights.emplace_back(std::move(info_data));
  }
  if (options.stop_token.IsStopRequested()) rpc.context.TryCancel();
  RETURN_NOT_OK(options.stop_token.Poll());                  // Early return 2 (line 911)
  listing->reset(new SimpleFlightListing(std::move(flights)));
  return FromGrpcStatus(stream->Finish(), &rpc.context);     // Skipped on early return!
  ```

* **Impact & Reproducer (Verified on `libgrpc++ 1.72.0`):**
  1. *Deserialization Failure (`ListFlights:907` vs. `ListActions:935` FP):* In `ListFlights`, `internal::FromProto(pb::FlightInfo, ...)` fails when the server returns `FlightDescriptor::UNKNOWN` or an invalid `Location` URI, exiting immediately without `Finish()`. By contrast, `ListActions`'s statically flagged `FromProto(pb::ActionType, ...)` check at line 935 unconditionally returns `Status::OK()` (`serialization_internal.cc:124-128`) and is therefore an unreachable path-insensitive false positive.
  2. *StopToken Cancellation (`ListFlights:910-911`, `ListActions:938-939`):* Both call `rpc.context.TryCancel()` followed by `RETURN_NOT_OK(options.stop_token.Poll())`, exiting without `stream->Finish()`. Runtime testing confirms `TryCancel()` alone does not fire `POST_RECV_STATUS`.
  3. *AuthenticateInternal (`1067-1091`):* Unlike earlier misattributed notes citing lines `592-624` (`GrpcGarbageBin`), real `AuthenticateInternal` DOES call `stream->Finish()` on its main path (line 1089). However, line `1074` (`RETURN_NOT_OK(auth_handler_->Authenticate(...))`) omits `Finish()` when authentication fails after a clean read (`1E-R1`, confirmed), and lines `1078-1081` (`if (!finished_writes)`) syntactically omit `Finish()` (`1E-R2`, partial).
  4. *Consequence:* While `~ClientContext()` cancels and reclaims the C-core HTTP/2 call (`RST_STREAM`), skipping `stream->Finish()` prevents gRPC core from starting `RECV_STATUS_ON_CLIENT`, so `InterceptionHookPoints::POST_RECV_STATUS` and registered `ClientMiddleware::CallCompleted(status)` callbacks never execute and trailing server status/metadata are discarded.

---

### 2. Triton Inference Server C++ Client (`triton-inference-server/client`) — [`CD-10`, `CD-11`, `INC-02`, `INC-03`]
* **Source:** `src/c++/library/grpc_client.cc` (lines 1355–1461, 1680–1771)
* **Root Cause:**
  `InferenceServerGrpcClient` manages an asynchronous worker thread and a bidirectional stream pointer `grpc_stream_`. Two lifecycle management bugs exist in the client implementation:
  
  ```cpp
  Error InferenceServerGrpcClient::StopStream() {
    if (!stream_worker_.joinable()) {
      return Error::Success;
    }
    grpc_stream_->WritesDone();
    stream_worker_.join(); // Worker thread calls grpc_stream_->Finish() at line 1734
    // Issue: grpc_stream_.reset() is never called!
    return Error::Success;
  }

  Error InferenceServerGrpcClient::AsyncStreamInfer(...) {
    // Issue 1: enable_stream_stats_ is uninitialized if StartStream() was never called
    if (enable_stream_stats_) { ... }
    
    // Issue 2: Missing nullptr check for grpc_stream_
    bool ok = grpc_stream_->Write(infer_request_);
  }
  ```

* **Impact & Reproducer (Verified on `libgrpc++ 1.72.0`):**
  1. *Premature Infer:* Calling `AsyncStreamInfer()` prior to `StartStream()` branches on uninitialized `enable_stream_stats_` (`grpc_client.h:674`) and dereferences `grpc_stream_ == nullptr` at line 1447, triggering an immediate segmentation fault (`0xC0000005`).
  2. *Use-After-Stop:* Calling `AsyncStreamInfer()` after `StopStream()` invokes `grpc_stream_->Write()` on a finished stream (`S2_FINISHED`). On `libgrpc++ 1.72.0`, `Write()` returns `false` (`Error("Stream has been closed.")`) rather than crashing, **but** because `StopStream()` neither resets `grpc_stream_` nor clears `enable_stream_stats_`, `AsyncStreamInfer()` pushes a phantom `RequestTimers` entry at line 1445 before `Write()` fails and never pops it—corrupting latency statistics (`~88–90 ms` skew) in subsequent `StartStream()` sessions.

---

### 3. WeNet ASR Server (`wenet-e2e/wenet`) — [`CD-12`, `CD-13`, `CTL-04`]
* **Source:** `runtime/core/grpc/grpc_server.cc` (lines 50–55, 143–163)
* **Root Cause:**
  In the gRPC connection handler, the server reads incoming audio chunks from `stream_`:

  ```cpp
  void GrpcConnectionHandler::OnSpeechEnd() {
    CHECK(feature_pipeline_ != nullptr); // Crashes server with SIGABRT if null
    feature_pipeline_->set_input_finished();
  }

  void GrpcConnectionHandler::operator()() {
    try {
      while (stream_->Read(request_.get())) {
        if (!got_start_tag_) {
          // Never checks if request has decode_config!
          nbest_ = request_->decode_config().nbest_config();
          continuous_decoding_ = request_->decode_config().continuous_decoding_config();
          OnSpeechStart(); // Initializes feature_pipeline_
        } else {
          OnSpeechData();
        }
      }
      OnSpeechEnd(); // Unconditionally called even when read loop ran 0 times
  ```

* **Impact & Reproducer (Verified on `libgrpc++ 1.72.0`):**
  1. *Empty Stream Crash:* If a client opens a connection and disconnects or closes the stream immediately (`WritesDone()`) without sending any messages, `stream_->Read()` returns `false` on iteration 0. `OnSpeechEnd()` runs with `feature_pipeline_ == nullptr`, triggering Google Log's `CHECK` macro and aborting the server process (`SIGABRT`).
  2. *Prior Precedent:* WeNet previously resolved an identical bug in their WebSocket server implementation (Issue #813, fixed in PR #911), but the gRPC server handler was left unguarded.
  3. *`oneof` Handshake Inversion (`nbest_ == 0` beam bypass):* If the client sends `audio_data` before `decode_config`, message 1's audio is silently dropped and `nbest_` is overwritten to `0` (`DecodeConfig::default_instance()`). Because `SerializeResult()` increments `response_->nbest_size()` before checking `response_->nbest_size() == nbest_`, subsequent chunks bypass truncation and emit all internal beam hypotheses.

---

### 4. BuildGrid BuildBox (`BuildGrid/buildbox`) — [`CD-07`..`CD-09`, `CTL-02`, `INC-01`]
* **Source:** `common/buildboxcommon_grpccasclient.cpp`
* **Root Cause:**
  1. *`~GrpcStagedDirectory` Lifecycle:*
     In `buildboxcommon_grpccasclient.cpp` (lines 1317–1323), the RAII staged directory cleanup destructor notifies the CAS server:
     ```cpp
     GrpcCASClient::GrpcStagedDirectory::~GrpcStagedDirectory() {
         d_reader_writer->Write(StageTreeRequest());
         d_reader_writer->WritesDone();
         // Exits without reading server cleanup response or calling Finish()!
     }
     ```
     `local_cas.proto` specifies a two-phase handshake where the server sends a confirmation `StageTreeResponse` after cleanup is complete. Direct inspection of `buildboxcasd` (`casd/buildboxcasd_localcasinstance.cpp:589–637`) confirms `casd` intentionally ignores `IsCancelled()` so directory deletion still finishes asynchronously; however, omitting `Read(&response)` and `Finish()` violates the proto contract, aborts the server's cleanup ack `Write()` via `RST_STREAM`, silently drops server cleanup errors (`INTERNAL`), bypasses `ClientInterceptor::POST_RECV_STATUS`, and causes immediate local filesystem / re-stage race collisions (`local_cas.proto:376–378`).
  2. *Mid-Stream Exception Leaks in `uploadFdRegion` and `download`:*
     In `uploadFdRegion` (lines 506–584) and `download` (lines 358–431), local I/O errors (`pread < 0`, unexpected EOF, or `write(fd) < 0`) throw exceptions before `WritesDone()` / `Finish()` are reached, cancelling the HTTP/2 call (`RST_STREAM`) and bypassing `POST_RECV_STATUS`.

---

### 5. Official gRPC Example (`CachingInterceptor`) — [`CD-01`]
* **Source:** `examples/cpp/interceptors/caching_interceptor.h` (lines 39–108, `keyvaluestore::KeyValueStore::GetValues`)
* **Root Cause:**
  On each cache miss inside `PRE_SEND_MESSAGE`, `CachingInterceptor` issues a synchronous `stream_->Write(req);` and `stream_->Read(&resp);` without checking return values, stores `cached_map_.insert({requested_key, response_})`, and calls `stream_->WritesDone();` in `PRE_SEND_CLOSE` without ever calling `stream_->Finish()`:
  ```cpp
  // PRE_SEND_MESSAGE (caching_interceptor.h:83-89, 96-100):
  stream_->Write(req);
  keyvaluestore::Response resp;
  stream_->Read(&resp);
  response_ = resp.value();
  cached_map_.insert({requested_key, response_}); // Caches "" on server failure!

  // PRE_RECV_STATUS (caching_interceptor.h:101-105):
  auto* status = methods->GetRecvStatus();
  *status = grpc::Status::OK; // Hardcodes OK without calling stream_->Finish()
  ```
* **Impact (Verified on `libgrpc++ 1.72.0`):**
  1. If the upstream server fails (e.g., `INTERNAL` or `CANCELLED`—note upstream `examples/cpp/interceptors/server.cc:99` itself returns `CANCELLED` on normal client EOF `!ok`), `stream_->Read(&resp)` returns `false` leaving `resp.value() == ""`. `CachingInterceptor` serves an empty response (`read_ok=true`, `value=""`), poisons `cached_map_` with `""` for future retries, and masks the failure to `Status::OK`.
  2. Skipping `stream_->Finish()` abandons delayed server completion/trailers via `RST_STREAM(8)` and skips downstream `POST_RECV_STATUS` interceptors. Additionally, because upstream `client.cc` (`ClientBidiReactor`) posts `PRE_RECV_STATUS` eagerly at `StartCall()`, a sound fix must defer `stream_->Finish()` until `PRE_SEND_CLOSE` or stream error.
* **Runtime Reproduction Matrix (`reproductions/grpc-interceptor/`):**
  - **`T0a` (Clean Happy Path):** Sync `ClientReaderWriter` requests `key1`, `key2`, `key1`; misses on `key1`/`key2`, hits cache on second `key1`, finishes with `Status::OK`.
  - **`T0b` (`examples/cpp/interceptors/server.cc:99` Quirk):** Server calls `Finish(CANCELLED)` on normal client `WritesDone()`. Direct (`T0b_NO_INTERCEPTOR`) and fixed (`T0b_FIXED`) clients observe `finish_code=1` (`CANCELLED`); buggy upstream `CachingInterceptor` (`T0b_UPSTREAM_BUGGY`) masks it to `finish_code=0` (`OK`).
  - **`T1` (Case A1 — Mid-Stream Failure, Sync Client):** Server fails `key2` with `INTERNAL("db failure")` after `key1`, then client retries `key2`. Buggy returns phantom `read_ok=true value=""`, poisons `cached_map_["key2"] = ""`, hits cache on retry (`server_reads=2`), and returns `finish_code=0`; fixed returns `read_ok=false`, fails retry (`write_ok=false`), and returns `finish_code=13` (`INTERNAL`).
  - **`T2` (Case A1b — Mid-Stream Failure, `ClientBidiReactor`):** Same `INTERNAL("db failure")` test using `client.cc`'s callback `ClientBidiReactor` pattern. Buggy prints empty `key2` and delivers `on_done_code=0` (`OK`); fixed stops cleanly (`read[1](key2,ok=0,val="")`) and delivers `on_done_code=13` (`INTERNAL`).
  - **`T3` (Case A2 — Immediate 1st-Key Failure):** Server fails immediately on `key1` (0 responses), then client retries `key1`. Buggy caches `{"key1", ""}` and returns `finish_code=0`; fixed avoids caching and returns `finish_code=13`.
  - **`T4` (Case A3 — Post-`WritesDone` Failure):** All `Write`/`Read` calls succeed, then server returns `INTERNAL("commit failed")` after `WritesDone()`. Buggy returns `finish_code=0`; fixed calls `stream_->Finish()` and returns `finish_code=13`.
  - **`T5` (Case B1 — Delayed Server Completion & Trailing Metadata):** Server sleeps `150 ms` after `WritesDone()`, attaches `x-server-commit: committed`, and calls `Finish(OK)`. Buggy returns `OK` immediately (`finish_ms=0`), cancels the server call (`server_saw_cancelled=true`), and drops trailers; fixed awaits completion (`finish_ms=151` / `164`), `server_saw_cancelled=false`, and preserves trailers.
  - **`T6` (Case B2 — Downstream Interceptor Hook Audit):** `BackingObserverInterceptor` downstream via `GetInterceptedChannel()` sees `pre_send_close=1`, `post_recv_status=0` (`observed_status=<NONE>`) under buggy vs. `post_recv_status=1` (`observed_code=0`, `trailer_commit="committed"`) under fixed.
  - **`T7` (Case B3 — `libgrpc++` Diagnostic Traces):** Runs teardown under `GRPC_VERBOSITY=DEBUG GRPC_TRACE=api,call_error`, confirming `grpc_call_unref` triggers `CancelWithError CLI CANCELLED:CANCELLED` and `Received RST_STREAM with error code 8` on the server under buggy vs. clean `set_final_status SVR OK` / `CLI OK` under fixed.

---

## 3. Upstream Disclosure Reports & Patches

Below are the drafted reports and corresponding patch diffs ready for submission to upstream maintainers according to each project's contribution guidelines.

---

### Disclosure 1: Apache Arrow — [`CD-02`..`CD-06`]
* **Target:** [github.com/apache/arrow/issues](https://github.com/apache/arrow/issues)
* **Title:** `[C++][FlightRPC] GrpcClientImpl::ListFlights and ListActions skip stream->Finish() on early return, bypassing ClientMiddleware::CallCompleted`
* **Labels:** `Component: FlightRPC`, `Component: C++`
* **Report Body:**
  > In `cpp/src/arrow/flight/transport/grpc/grpc_client.cc`, `GrpcClientImpl::ListFlights` (lines 892–914) and `ListActions` (lines 923–941) interact directly with `grpc::ClientReader` rather than using the `GrpcResultStream` wrapper used elsewhere in the transport layer (`AuthenticateInternal` at lines 1067–1091 similarly omits `Finish()` on its `auth_handler_->Authenticate()` error exit at line 1074 and `!finished_writes` branch at lines 1078–1081).
  >
  > If `internal::FromProto` fails during `ListFlights` iteration (line 907, e.g., `FlightDescriptor::UNKNOWN` or invalid `Location` URI) or if `options.stop_token` triggers cancellation (lines 911 / 939), the functions return early via `RETURN_NOT_OK`. When this happens, `stream->Finish()` is never called. While `~ClientContext()` still reclaims the C-core HTTP/2 call, skipping `Finish()` prevents `RECV_STATUS_ON_CLIENT` / `InterceptionHookPoints::POST_RECV_STATUS` from firing, silently bypassing registered `ClientMiddleware::CallCompleted()` callbacks and discarding trailing status. (Note: `FromProto(pb::ActionType)` in `ListActions:935` currently always returns `Status::OK()`.)
* **Proposed Patch:**
  ```diff
  --- a/cpp/src/arrow/flight/transport/grpc/grpc_client.cc
  +++ b/cpp/src/arrow/flight/transport/grpc/grpc_client.cc
  @@ -904,11 +904,18 @@ class GrpcClientImpl : public internal::ClientTransport {
        while (!options.stop_token.IsStopRequested() && stream->Read(&pb_info)) {
          FlightInfo::Data info_data;
  -      RETURN_NOT_OK(internal::FromProto(pb_info, &info_data));
  +      auto status = internal::FromProto(pb_info, &info_data);
  +      if (!status.ok()) {
  +        rpc.context.TryCancel();
  +        ARROW_UNUSED(FromGrpcStatus(stream->Finish(), &rpc.context));
  +        return status;
  +      }
          flights.emplace_back(std::move(info_data));
        }
  -    if (options.stop_token.IsStopRequested()) rpc.context.TryCancel();
  -    RETURN_NOT_OK(options.stop_token.Poll());
  +    if (options.stop_token.IsStopRequested()) {
  +      rpc.context.TryCancel();
  +      ARROW_UNUSED(FromGrpcStatus(stream->Finish(), &rpc.context));
  +      return options.stop_token.Poll();
  +    }
        listing->reset(new SimpleFlightListing(std::move(flights)));
        return FromGrpcStatus(stream->Finish(), &rpc.context);
      }
  ```

---

### Disclosure 2: Triton Inference Server C++ Client — [`CD-10`, `CD-11`, `INC-02`]
* **Target:** [github.com/triton-inference-server/client/issues](https://github.com/triton-inference-server/client/issues)
* **Title:** `[Bug]: C++ InferenceServerGrpcClient::AsyncStreamInfer dereferences null grpc_stream_ before StartStream() and StopStream() fails to reset grpc_stream_`
* **Report Body:**
  > In `src/c++/library/grpc_client.cc`, `InferenceServerGrpcClient::AsyncStreamInfer` assumes `StartStream()` was already called and dereferences `grpc_stream_` without validation, causing an immediate crash if called out of order. Additionally, `StopStream()` joins the stream worker thread but does not reset `grpc_stream_`, leaving the client in an invalid state where subsequent `AsyncStreamInfer` calls invoke `Write()` on a finished stream.
* **Proposed Patch:**
  ```diff
  --- a/src/c++/library/grpc_client.h
  +++ b/src/c++/library/grpc_client.h
  @@ -673,3 +673,3 @@ class InferenceServerGrpcClient : public InferenceServerClient {
      grpc::ClientContext grpc_context_;
  -  bool enable_stream_stats_;
  +  bool enable_stream_stats_{false};
    };
  --- a/src/c++/library/grpc_client.cc
  +++ b/src/c++/library/grpc_client.cc
  @@ -1409,5 +1409,6 @@ InferenceServerGrpcClient::StopStream()
      grpc_stream_->WritesDone();
      stream_worker_.join();
  +  grpc_stream_.reset();
      return Error::Success;
    }
  @@ -1425,4 +1426,7 @@ InferenceServerGrpcClient::AsyncStreamInfer(
    {
  +  if (!grpc_stream_ || !stream_worker_.joinable()) {
  +    return Error("Stream is not active; call StartStream() before AsyncStreamInfer().");
  +  }
  ```

---

### Disclosure 3: WeNet ASR Server — [`CD-12`, `CD-13`]
* **Target:** [github.com/wenet-e2e/wenet/issues](https://github.com/wenet-e2e/wenet/issues)
* **Title:** `[gRPC Runtime] Guard OnSpeechEnd() on empty client stream and validate has_decode_config() on initial Recognize request`
* **Report Body:**
  > In `runtime/core/grpc/grpc_server.cc`, `GrpcConnectionHandler::operator()` executes `OnSpeechEnd()` immediately when `stream_->Read()` returns false. If a client connects and disconnects without sending audio or configuration data (0 messages read), `feature_pipeline_` is never initialized, and `OnSpeechEnd()` triggers `CHECK(feature_pipeline_ != nullptr)`, causing the server process to abort.
  >
  > A similar issue was previously resolved for the WebSocket server in #911. We recommend applying the same guard to the gRPC connection handler.
* **Proposed Patch:**
  ```diff
  --- a/runtime/core/grpc/grpc_server.cc
  +++ b/runtime/core/grpc/grpc_server.cc
  @@ -144,6 +144,13 @@ void GrpcConnectionHandler::operator()() {
      try {
        while (stream_->Read(request_.get())) {
          if (!got_start_tag_) {
  +        if (!request_->has_decode_config()) {
  +          LOG(WARNING) << "First message on Recognize stream must contain decode_config";
  +          response_->set_status(Response::failed);
  +          stream_->Write(*response_);
  +          return;
  +        }
            nbest_ = request_->decode_config().nbest_config();
            continuous_decoding_ =
                request_->decode_config().continuous_decoding_config();
  @@ -152,7 +159,9 @@ void GrpcConnectionHandler::operator()() {
            OnSpeechData();
          }
        }
  -    OnSpeechEnd();
  +    if (got_start_tag_ && feature_pipeline_ != nullptr) {
  +      OnSpeechEnd();
  +    }
  ```

---

### Disclosure 4: BuildGrid BuildBox — [`CD-07`]
* **Target:** [gitlab.com/BuildGrid/buildbox/buildbox/-/issues](https://gitlab.com/BuildGrid/buildbox/buildbox/-/issues)
* **Title:** `[common] GrpcCASClient::GrpcStagedDirectory destructor exits without completing StageTree cleanup handshake`
* **Report Body:**
  > In `common/buildboxcommon_grpccasclient.cpp`, `GrpcStagedDirectory::~GrpcStagedDirectory()` signals cleanup by writing an empty `StageTreeRequest` and calling `WritesDone()`. However, it does not read the server's acknowledgment `StageTreeResponse` nor does it call `Finish()`. While `buildboxcasd` completes directory deletion asynchronously without checking `IsCancelled()`, returning early violates `local_cas.proto`, aborts the server's ack `Write()` via `RST_STREAM`, silently drops cleanup errors, bypasses `POST_RECV_STATUS`, and causes immediate filesystem re-stage collisions on the same path.
* **Proposed Patch:**
  ```diff
  --- a/common/buildboxcommon_grpccasclient.cpp
  +++ b/common/buildboxcommon_grpccasclient.cpp
  @@ -1317,6 +1317,11 @@ GrpcCASClient::GrpcStagedDirectory::~GrpcStagedDirectory()
   {
       // Tell CAS server to clean up:
  -    d_reader_writer->Write(StageTreeRequest());
  -    d_reader_writer->WritesDone();
  +    StageTreeRequest cleanup_request;
  +    if (d_reader_writer->Write(cleanup_request)) {
  +        d_reader_writer->WritesDone();
  +        StageTreeResponse cleanup_response;
  +        d_reader_writer->Read(&cleanup_response);
  +    }
  +    d_reader_writer->Finish();
   }
  ```

---

### Disclosure 5: gRPC Examples (`CachingInterceptor`) — [`CD-01`]
* **Target:** [github.com/grpc/grpc/issues](https://github.com/grpc/grpc/issues)
* **Title:** `[C++ Example] CachingInterceptor in examples/cpp/interceptors/caching_interceptor.h never calls stream_->Finish() and masks upstream RPC failures`
* **Report Body:**
  > In `examples/cpp/interceptors/caching_interceptor.h` (`keyvaluestore::KeyValueStore::GetValues`), `CachingInterceptor` calls `stream_->Write(req);` and `stream_->Read(&resp);` inside `PRE_SEND_MESSAGE` without checking return values, caches the result in `cached_map_`, and calls `stream_->WritesDone();` inside `PRE_SEND_CLOSE`. However, it never invokes `stream_->Finish()` and unconditionally sets `*status = grpc::Status::OK` in `PRE_RECV_STATUS`. If the upstream server fails mid-stream, an empty string `""` is cached for future retries, failure statuses are masked as `OK`, delayed server trailers are aborted via `RST_STREAM`, and downstream `POST_RECV_STATUS` interceptors never fire. (Because `ClientBidiReactor` callers post `PRE_RECV_STATUS` eagerly at `StartCall()`, any fix calling `stream_->Finish()` must defer execution until `PRE_SEND_CLOSE` or stream error.)
* **Proposed Fix Summary:**
  - Check `bool w_ok = stream_->Write(req); bool r_ok = w_ok && stream_->Read(&resp);` before inserting into `cached_map_`; fail the hijacked message on error rather than serving an empty `Response`.
  - Defer calling `stream_->Finish()` until `PRE_SEND_CLOSE` or stream failure so blocking/async reactor callers do not deadlock, and propagate the real returned `grpc::Status` instead of hardcoding `grpc::Status::OK`.
