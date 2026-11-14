# Runtime Verification Findings: Apache Arrow Flight C++ (`apache/arrow`) — `CD-02`..`CD-06`

**Provenance & Naming Convention:** Protocol bugs here belong to **Category D (`[CANDIDATE / UNCONFIRMED UPSTREAM]`)** — *"Reproduced locally"* means verified in `libgrpc++ 1.72.0` loopback runs, **not** accepted upstream. Canonical IDs map to [`docs/04_Candidate_Bugs_and_Disclosure_Drafts.md`](../../docs/04_Candidate_Bugs_and_Disclosure_Drafts.md) and [`reproductions/README.md`](../README.md) (`CD-*` = Category D bug, `CTL-*` = control, `FP-*` = false positive/retracted claim; legacy `1A..1E` and `T0..T5` are repo-local tags only).  
**Upstream Commit Audited:** `fb767a7cae8a52bd52166628b73353d3d85ca88f`  
**Runtime Stack:** Windows 11, MSYS2 UCRT64, `g++.exe (Rev8, Built by MSYS2 project) 15.2.0`, `libprotoc 31.1`, real **`libgrpc++ 1.72.0`** (`-DPROTOBUF_USE_DLLS`)  
**Loopback Telemetry Evidence:** [`TEST_OUTPUT.txt`](./TEST_OUTPUT.txt) (normal execution + `GRPC_TRACE=api,call_error GRPC_VERBOSITY=INFO`)

---

## 1. Executive Summary & Per-Bug Verdicts

Using a real `libgrpc++ 1.72.0` loopback `arrow.flight.protocol.FlightService::Service` compiled from verbatim [`proto/Flight.proto`](./proto/Flight.proto) and instrumented with a `grpc::experimental::ClientInterceptorFactoryInterface` mirroring `GrpcClientInterceptorAdapter` (`grpc_client.cc:112-162`), we empirically settled every candidate claim for Apache Arrow Flight C++:

| Canonical ID *(Legacy Tag)* | Method & Real Upstream Location | Original Claim in Prior Thesis Docs | Local Verdict *(Upstream: Unconfirmed)* | Real `libgrpc++ 1.72.0` Evidence (`TEST_OUTPUT.txt`) |
| :--- | :--- | :--- | :--- | :--- |
| **`CTL-01`** *(Baseline)* | `GrpcResultStream` (`DoAction`, `grpc_client.cc:496-559`) | Conforming RAII wrapper calling `TryCancel()` + `Finish()` on teardown | **VERIFIED CONFORMING (`TN`)** | `T0a`: `POST_RECV_STATUS=1`, `CallCompleted=1` (`OK`).<br/>`T0b` (early exit): `PRE_SEND_CANCEL=2`, `POST_RECV_STATUS=1`, `CallCompleted=1` (`CANCELLED`). |
| **`CD-02`** *(`Bug 1A`)* | `ListFlights` (`grpc_client.cc:907`) | `RETURN_NOT_OK(internal::FromProto(...))` skips `Finish()` on malformed `pb::FlightInfo` | **REPRODUCED (Cat. D)** | `T1b`, `T1b_stream` (`FlightDescriptor::UNKNOWN`) and `T1c` (invalid `Location` URI `"://not-a-valid-uri"`): `PRE_SEND_CANCEL=0`, **`POST_RECV_STATUS=0`**, **`CallCompleted=0`**. Server status `DATA_LOSS` & trailers lost. |
| **`CD-03`** *(`Bug 1B`)* | `ListFlights` (`grpc_client.cc:910-911`) | `rpc.context.TryCancel(); RETURN_NOT_OK(options.stop_token.Poll());` skips `Finish()` | **REPRODUCED (Cat. D)** | `T2a` (pre-cancelled) & `T2b` (mid-stream): `PRE_SEND_CANCEL=1`, `server_saw_cancelled=1`, yet **`POST_RECV_STATUS=0`**, **`CallCompleted=0`**. |
| **`FP-01`** *(`Bug 1C`)* | `ListActions` (`grpc_client.cc:935`, *NOT* `1067-1091`) | `RETURN_NOT_OK(internal::FromProto(pb_type, &type))` skips `Finish()` on invalid `pb::ActionType` | **FALSE POSITIVE** *(dead error path)* | `serialization_internal.cc:124-128` (`FromProto(pb::ActionType, ActionType*)`) performs zero validation and unconditionally returns `Status::OK()`. `T3a` ran to `Finish()` with `POST_RECV_STATUS=1`, `CallCompleted=1`. *(Syntactically present in AST, dynamically unreachable; invalid UTF-8 in proto3 fails earlier inside gRPC/Protobuf deserialization during `Read()` before `FromProto()` is called.)* |
| **`CD-04`** *(`Bug 1D`)* | `ListActions` (`grpc_client.cc:938-939`, *NOT* `1067-1091`) | `rpc.context.TryCancel(); RETURN_NOT_OK(options.stop_token.Poll());` skips `Finish()` | **REPRODUCED (Cat. D)** | `T3b` (mid-stream cancel): `Status=Cancelled`, `PRE_SEND_CANCEL=1`, **`POST_RECV_STATUS=0`**, **`CallCompleted=0`**. |
| **`FP-02`** *(`Bug 1E orig`)* | `AuthenticateInternal` *(cited at wrong lines `592-624`; real lines `1067-1091`)* | Normal path calls `WritesDone()` then returns `Status::OK()` without `Finish()`, masking `UNAUTHENTICATED` as `OK` | **RETRACTED / FALSE POSITIVE** *(fabricated code & wrong lines)* | Upstream lines `588-626` are `GrpcGarbageBin`. Real `AuthenticateInternal` (`1067-1091`) **DOES call `FromGrpcStatus(stream->Finish(), &rpc.context)` at line 1089**. `T4a_basic_unauth` returned `Unauthenticated: Invalid credentials` (`POST_RECV_STATUS=1`, `CallCompleted=1`), NOT `OK`. |
| **`CD-05`** *(`Bug 1E-R1`)* | `AuthenticateInternal` (`grpc_client.cc:1074`) | `RETURN_NOT_OK(auth_handler_->Authenticate(...))` skips `TryCancel()`/`Finish()` when `Authenticate()` fails after clean `Read()`/`Write()` | **REPRODUCED (Cat. D)** | `T4b_handler_reject`: handler reads server challenge (`Read()` succeeds so `GrpcClientAuthReader` does not call `Finish()`), returns `Status::Invalid`. Observed: `PRE_SEND_CLOSE=0`, `PRE_SEND_CANCEL=0`, **`POST_RECV_STATUS=0`**, **`CallCompleted=0`**. |
| **`CD-06`** *(`Bug 1E-R2`)* | `AuthenticateInternal` (`grpc_client.cc:1077-1081`) | `if (!finished_writes) return MakeFlightError(FlightStatusCode::Internal, ...)` omits `Finish()` | **PARTIAL (Cat. D)** *(syntactic gap; unreached on clean HTTP/2 server close)* | `T4c_immediate_nodelay` (`UNIMPLEMENTED`) & `T4c_delayed_writesdone` (40ms delay after immediate server `UNAUTHENTICATED`): `stream->WritesDone()` still returned **`true`** in `libgrpc++ 1.72.0` over HTTP/2, proceeding to line 1089 `Finish()` (`POST_RECV_STATUS=1`, `Unimplemented`/`Unauthenticated` preserved). Branch `1078-1081` triggers only if local send-half teardown fails (`WritesDone() == false`). |
| **Fixes (`T5*`)** | `ListFlights_Fixed`, `ListActions_Fixed`, `AuthenticateInternal_Fixed` | Explicit `TryCancel()` + `Finish()` on every early exit (and `Finish()` when `!finished_writes`) | **FIX VERIFIED LOCALLY** | `T5a_bad_desc`, `T5a_stop`, `T5b_stop`, `T5c_auth_reject`, `T5c_delayed_wd`: all achieved **`POST_RECV_STATUS=1`**, **`CallCompleted=1`** with zero hangs. |

---

## 2. Empirical `libgrpc++ 1.72.0` Mechanics: What Actually Happens When `Finish()` Is Skipped

Combining the interceptor counters in Section 1 and the `GRPC_TRACE=api,call_error` logs in Section 3 of [`TEST_OUTPUT.txt`](./TEST_OUTPUT.txt) settles how real `libgrpc++ 1.72.0` behaves on every teardown variant:

1. **`~ClientContext()` / `grpc_call_unref` DOES cancel C-core HTTP/2 calls (`T1b`, `T1b_stream`, `T1c`, `T4b_handler_reject`):**
   - Destroying `ClientReader` + `ClientContext` without `TryCancel()` or `Finish()` triggers `filter_stack_call.cc: CancelWithError CLI CANCELLED:CANCELLED` inside gRPC C core (`RST_STREAM` error code 8 on the wire; `T1b_stream` observed `server_saw_cancelled=1`).
   - Thus, unlike raw C sockets left open indefinitely, **transport buffers and HTTP/2 streams are reclaimed once `ClientRpc` goes out of scope**. We must **not** claim an infinite OS file-descriptor or socket hang when `ClientRpc` drops locally.
2. **Skipping `stream->Finish()` STILL breaks the `libgrpc++` C++ lifecycle contract and suppresses `POST_RECV_STATUS` (`CallCompleted`):**
   - Look at Section 3 of `TEST_OUTPUT.txt`: `ops[0]: RECV_STATUS_ON_CLIENT` is **only** submitted when `Finish()` runs (`T0a`, `T0b`, `T1a`, `T3a`, `T4a_*`, `T4c_*`, `T5*`).
   - Whenever `Finish()` is skipped — **even when `rpc.context.TryCancel()` WAS called (`T2a`, `T2b`, `T3b`: `PRE_SEND_CANCEL=1`)** — `RECV_STATUS_ON_CLIENT` is never started, so:
     - `InterceptionHookPoints::POST_RECV_STATUS` **never fires** (`POST_RECV_STATUS=0`).
     - Arrow's `GrpcClientInterceptorAdapter::Intercept` (`grpc_client.cc:135-143`) **never invokes `ReceivedHeaders(*methods->GetRecvTrailingMetadata())` or `middleware->CallCompleted(status)`** (`CallCompleted=0`).
     - Any registered `ClientMiddleware` tracking RPC latency, active span counts, OpenTelemetry spans, or auth-header rotation permanently leaks/drops completion for that call.
     - Without `Finish()`, trailing metadata and any actual server status (e.g., `DATA_LOSS` in `T1b_stream`, `UNAUTHENTICATED` in `T4b_handler_reject`) are discarded client-side.
   - Note also a subtle distinction shown in `T1b_stream` vs. `T2b`: skipping `TryCancel()` before early destruction (`T1b_stream`, `T4b_handler_reject`) also bypasses `ClientContext::TryCancel()`'s `PRE_SEND_CANCEL` hook (`PRE_SEND_CANCEL=0` despite C-core sending `RST_STREAM`).
3. **`stream->WritesDone()` on immediate server half-close (`T4c_immediate_nodelay`, `T4c_delayed_writesdone`):**
   - Even when the server returned `grpc::StatusCode::UNIMPLEMENTED` or `UNAUTHENTICATED` immediately (and in `T4c_delayed_writesdone`, the client waited `40ms` before calling `WritesDone()`), `libgrpc++ 1.72.0` returned `WritesDone() == true`, drained empty `Read()`, and called line 1089 `stream->Finish()` (`POST_RECV_STATUS=1`, server error preserved).
4. **Proto3 UTF-8 `string` wire guard before `FromProto(ActionType)` (`T3a` finding during development):**
   - While constructing `T3a`, sending raw invalid UTF-8 bytes (`0xff`) in `ActionType.type` failed inside Protobuf C++ `wire_format_lite.cc:578` during gRPC's `stream->Read(&pb_type)` deserialization, causing `Read()` to return `false` and exit the loop directly to line 940 `stream->Finish()` — never reaching `internal::FromProto()` at line 935. Meanwhile, for any valid UTF-8 `ActionType` (including empty strings or unrecognized control bytes), `FromProto(const pb::ActionType&, ActionType*)` (`serialization_internal.cc:124-128`) unconditionally returns `Status::OK()`. Hence line 935 (`RETURN_NOT_OK(internal::FromProto(pb_type, &type))`) is unreachable dead error handling unless `FromProto(ActionType)` adds validation in a future version of Arrow.

---

## 3. Comprehensive Audit of Discrepancies in Prior Thesis & Disclosure Files

Our verified audit against upstream commit `fb767a7` identified multiple concrete inaccuracies in earlier reports, LaTeX sections, and synthetic PoCs that **must be corrected** before thesis submission or any upstream disclosure:

1. **`docs/04_Candidate_Bugs_and_Disclosure_Drafts.md`:**
   - **Line 17:** Summary table still marks `AuthenticateInternal` as `"Missing stream->Finish() on handshake | Candidate"`, which was based on the fabricated `592-624` version (Claimed Bug 1E is a **FALSE POSITIVE**; only the narrower `auth_handler_->Authenticate()` error exit at line `1074` [`1E-R1`] and `!finished_writes` branch at lines `1078-1081` [`1E-R2`] omit `Finish()`).
   - **Line 41:** Misattributes `GrpcClientImpl::ListActions` to `lines 1067-1091` (which is `AuthenticateInternal`). `ListActions` is actually at `lines 923-941`.

2. **`forensic_bug_verification_by_repo_design.md`:**
   - **Lines 59, 63, 85:** Repeats the `1067-1091` misattribution for `ListActions` (citing nonexistent `ListActions` lines `1082` and `1086`; actual `ListActions` lines are `935` and `939`) and cites `AuthenticateInternal` at `lines 592-624` (actual `588-626` is `GrpcGarbageBin`).
   - **Lines 88-93 (Bug 1A / 1C triggers):**
     - Falsely states invalid `app_metadata` fails `FromProto(pb::FlightInfo, ...)`; in `serialization_internal.cc:327`, `info->app_metadata = pb_info.app_metadata();` copies raw bytes unconditionally. The actual reachable failures in `FromProto(FlightInfo)` are `FlightDescriptor::UNKNOWN` (`serialization_internal.cc:282-297`) and invalid endpoint URIs via `Location::Parse` (`serialization_internal.cc:193-195, 321`).
     - Falsely claims streaming an invalid `pb::ActionType` in `ListActions` triggers Bug 1C; `FromProto(const pb::ActionType&, ActionType*)` (`serialization_internal.cc:124-128`) always returns `Status::OK()`, making Bug 1C a semantic **FALSE POSITIVE**.
   - **Lines 140-171 (Bug 1E — `AuthenticateInternal`):**
     - Cites wrong line numbers (`592-624`) and fabricates non-existent code (`while (stream->Read(&response)) { ... } stream->WritesDone(); return Status::OK();`) that never calls `stream->Finish()`, claiming `UNAUTHENTICATED` is silently masked as `Status::OK()`.
     - In upstream `grpc_client.cc:1067-1091`, line `1089` explicitly executes `RETURN_NOT_OK(FromGrpcStatus(stream->Finish(), &rpc.context))`, and `GrpcClientAuthSender::Write` (`231`) / `GrpcClientAuthReader::Read` (`253`) also call `stream_->Finish()` on read/write failure.

3. **`github_bug_disclosure_and_reports.md`:**
   - **Lines 32-70 (Report 1):** Lists `Bugs 1A, 1B, 1C, 1D, 1E` in the section heading, repeats `ListActions (lines 1067-1091)` and fabricated snippet lines `1080-1089` (even misquoting variable names `action_type`/`actions` vs upstream `type`/`types` at `932-936`), and presents `ListActions` `FromProto` (`1C`) as an active runtime early-return bug instead of unreachable dead code. (Must be corrected before submitting any `[C++][FlightRPC]` issue!)

4. **`latex/candidate_bugs.tex` & `latex/empirical_audit.tex`:**
   - `latex/empirical_audit.tex:81` correctly fixes `ListActions` to `lines 923--941`, but **still cites `AuthenticateInternal (lines 592--624)`** (which is `GrpcGarbageBin`).
   - `latex/candidate_bugs.tex:222` reports `GrpcClientImpl::AuthenticateInternal | Missing Finish() on Token | Violation | Caught (2 diags)` against the fabricated `592-624` behavior instead of the narrower `line 1074` (`1E-R1`) / `line 1078` (`1E-R2`) behavior in `lines 1067-1091`.
   - `latex/candidate_bugs.tex:230` (`ListActions` `"Early Return Leak | Caught (2 diags)"`) counts Bug `1C` (`line 935` `FromProto`) alongside Bug `1D` (`line 939` `StopToken::Poll()`); the text should explicitly classify `1C` as a static/syntactic unreachable branch (path-insensitive false positive) while retaining `1D` as a dynamically reachable defect.

5. **`poc/real_repo_bugs/arrow_handshake_buggy.cpp` & `poc/real_repo_bugs/arrow_list_flights_buggy.cpp`:**
   - `arrow_handshake_buggy.cpp` (entire file, `lines 1-40`) models fabricated lines `592-624` omitting `stream->Finish()` at line 38-39 and must be replaced or retired as a **FALSE POSITIVE** artifact. (A true residual PoC for `AuthenticateInternal` should trigger line `1074` `auth_handler_->Authenticate()` failure after a clean `Read()`, i.e. `1E-R1`.)
   - `arrow_list_flights_buggy.cpp:43` cites `grpc_client.cc:926`, whereas `FromProto` in `ListFlights` is at **line `907`** (`926` is inside `ListActions`).

---

## 4. Recommended Cleanup for Thesis Evaluation Integrity

1. Reclassify Arrow Flight findings in thesis tables (`latex/candidate_bugs.tex`, `docs/04`, `forensic_bug_verification_by_repo_design.md`) cleanly:
   - **Real, dynamically confirmed Category D candidates:** **Bug 1A** (`ListFlights:907`), **Bug 1B** (`ListFlights:910-911`), **Bug 1D** (`ListActions:938-939`), and **Residual Bug 1E-R1** (`AuthenticateInternal:1074`).
   - **Reachable-in-principle / partial:** **Residual Bug 1E-R2** (`AuthenticateInternal:1077-1081`, syntactically omits `Finish()`, unreached in normal HTTP/2 server half-close in `libgrpc++ 1.72.0`).
   - **Statically flagged / dynamically unreachable (honest precision reporting):** **Bug 1C** (`ListActions:935`, unreachable because `FromProto(ActionType)` always returns `OK`). Reporting this honestly strengthens the thesis evaluation by demonstrating flow- vs. value-sensitivity limits rather than claiming "100% precision with zero false alarms" on unreachable branches.
   - **Retract / remove as False Positive:** **Original Bug 1E claim** (`AuthenticateInternal` lines `592-624` returning `OK` without `Finish()`).
2. Fix all line-number citations to `grpc_client.cc` (`fb767a7`): `ListFlights` = `892-914`, `ListActions` = `923-941`, `AuthenticateInternal` = `1067-1091` (with `GrpcGarbageBin` at `588-626`).
3. If preparing an upstream `[C++][FlightRPC]` issue/PR, scope it accurately to `ListFlights` (`907`, `910-911`), `ListActions` (`938-939` `StopToken` cancellation — and optionally `935` defensively), and `AuthenticateInternal` (`1074` `auth_handler_->Authenticate()` failure + `1078` `!finished_writes`), using real `ClientMiddleware::CallCompleted` / `POST_RECV_STATUS` evidence as demonstrated in [`repro_arrow_flight.cpp`](./repro_arrow_flight.cpp).


---

## Methodology & Limitations

- **Verification Tier 2 — Extracted Standalone Loopback Harness (Not Full Upstream Build):** Full Apache Arrow C++ (`libarrow_flight`, Parquet/IPC dependencies, CMake toolchain) was not compiled natively on Windows MSYS2. Instead, `repro_arrow_flight.cpp` faithfully extracts the audited `ListFlights`, `ListActions`, `AuthenticateInternal`, `GrpcResultStream`, and `GrpcClientInterceptorAdapter` control-flow paths and runs them against verbatim `proto/Flight.proto` stubs (`protoc 31.1`) and real `libgrpc++ 1.72.0` over HTTP/2 loopback.
- **Simulated Triggers & Threats to Validity:**
  - Malformed `FlightInfo` payloads (`FlightDescriptor::UNKNOWN`, invalid endpoint URI), `StopToken` cancellations, `ClientAuthHandler` challenge errors (`1E-R1`), and immediate `UNIMPLEMENTED`/`UNAUTHENTICATED` rejections (`1E-R2`) are generated by the loopback harness rather than an external Arrow Flight server or real Parquet/IPC stream.
  - Whether upstream call sites trigger those error exits relies on direct inspection of `apache/arrow` (`grpc_client.cc` and `serialization_internal.cc` at `fb767a7`), which also established that `FromProto(ActionType)` unconditionally returns `Status::OK()` (`1C` dynamically unreachable).
  - All runs executed on Windows 11 MSYS2 UCRT64 (`g++ 15.2.0`, `libgrpc++ 1.72.0`); local `WritesDone()` behavior (`1E-R2`) and teardown timing may vary across gRPC versions or Linux event-loop scheduling.
