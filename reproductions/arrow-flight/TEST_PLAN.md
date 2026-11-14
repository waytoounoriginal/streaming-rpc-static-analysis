# Test Plan: Runtime Verification of Candidate Protocol Bugs in Apache Arrow Flight (`apache/arrow` — `CD-02`..`CD-06`)

> **Provenance & Naming Note:** Evaluates Category D candidates **`CD-02`..`CD-06`**, control **`CTL-01`**, and false-positive checks **`FP-01`..`FP-02`** (see [`reproductions/README.md`](../README.md) and [`docs/04_Candidate_Bugs_and_Disclosure_Drafts.md`](../../docs/04_Candidate_Bugs_and_Disclosure_Drafts.md)). Legacy labels `Bug 1A`..`1E` and `T0`..`T5` in this plan/output are local Arrow harness identifiers, **not** provenance Categories A–D.

**Upstream Commit Audited:** `fb767a7cae8a52bd52166628b73353d3d85ca88f`  
**Audited Source Files:**
- `format/Flight.proto` (copied verbatim to `proto/Flight.proto` for standalone `protoc` compilation)
- `cpp/src/arrow/flight/transport/grpc/grpc_client.cc`
- `cpp/src/arrow/flight/transport/grpc/grpc_server.cc`
- `cpp/src/arrow/flight/transport/grpc/util_internal.cc`
- `cpp/src/arrow/flight/serialization_internal.cc`
- `cpp/src/arrow/util/cancel.h`, `cpp/src/arrow/util/cancel.cc`

**Target Runtime Environment:**
- Windows 11, MSYS2 UCRT64 (`g++ 15.2.0`, `libprotoc 31.1`, `grpc_cpp_plugin`, real `libgrpc++ 1.72.0`)
- Real loopback `grpc::Server` (`arrow::flight::protocol::FlightService::Service`)
- Real `grpc::experimental::ClientInterceptorFactoryInterface` mirroring `GrpcClientInterceptorAdapter` (`grpc_client.cc:112-215`)

---

## 1. Hypotheses and Skeptical Audit Questions

Prior thesis documents (`docs/04_Candidate_Bugs_and_Disclosure_Drafts.md`, `forensic_bug_verification_by_repo_design.md`, and `poc/real_repo_bugs/arrow_handshake_buggy.cpp`) claimed five defects (`1A`–`1E`) across `ListFlights`, `ListActions`, and `AuthenticateInternal`. A line-by-line inspection of upstream `grpc_client.cc` reveals major discrepancies that require runtime verification rather than mock assumptions:

1. **Does skipping `stream->Finish()` after `RETURN_NOT_OK` actually suppress `POST_RECV_STATUS` in real `libgrpc++ 1.72.0`?**
   - Specifically:
     - When `Finish()` is omitted *without* `TryCancel()` (e.g., `ListFlights` `FromProto` error at line 907), does `~ClientReader()` / `~ClientContext()` ever fire `POST_RECV_STATUS`, or is `CallCompleted()` permanently skipped?
     - When `rpc.context.TryCancel()` *is* called prior to early return *without* `Finish()` (e.g., `ListFlights` / `ListActions` `StopToken::Poll()` at lines 910–911 and 938–939), does `TryCancel()` cause `POST_RECV_STATUS` to fire on interceptor teardown, or does only `PRE_SEND_CANCEL` fire while `POST_RECV_STATUS` stays silent?
   - Also inspect `GRPC_TRACE=api,call_error` output on both teardown paths.
2. **Is Bug 1C (`ListActions` `FromProto` early return at line 935) actually reachable?**
   - Upstream `internal::FromProto(const pb::ActionType&, ActionType*)` (`serialization_internal.cc:124-128`) performs zero validation and unconditionally returns `Status::OK()`. Test whether any wire `pb::ActionType` payload can cause `FromProto` to fail.
3. **Is Bug 1E (`AuthenticateInternal` returning `Status::OK()` without `stream->Finish()`) real or a false positive?**
   - Earlier thesis docs cited `lines 592-624` (actually `GrpcGarbageBin`) and claimed `AuthenticateInternal` omits `stream->Finish()` on the normal path, masking `UNAUTHENTICATED`.
   - In actual `grpc_client.cc:1067-1091`, line 1089 **explicitly calls `RETURN_NOT_OK(FromGrpcStatus(stream->Finish(), &rpc.context))`** after `WritesDone()` and draining `Read()`.
   - However, two *different* early-return paths exist in `AuthenticateInternal`:
     - Line 1074 (`RETURN_NOT_OK(auth_handler_->Authenticate(&outgoing, &incoming))`) when `Authenticate` fails locally after a successful `Read`/`Write`.
     - Lines 1077–1081 (`if (!finished_writes) return MakeFlightError(...)`) if `stream->WritesDone()` returns `false`. Test empirically whether immediate server `UNAUTHENTICATED` / `UNIMPLEMENTED` rejection causes `WritesDone()` to return `false` in real `libgrpc++ 1.72.0`.

---

## 2. Test Matrix

| ID | Target Method & Lines | Scenario / Trigger | Expected Behavior to Verify in Real `libgrpc++ 1.72.0` |
| :--- | :--- | :--- | :--- |
| **T0a** | `GrpcResultStream` (`DoAction`, `496-559`) | Happy path: consume all `pb::Result` items to EOF. | `Next()` calls `stream_->Finish()` at line 547; `POST_RECV_STATUS` fires with `OK`. |
| **T0b** | `GrpcResultStream` (`DoAction`, `504-514, 538-545`) | Early exit: consumer reads 1 item (or hits `StopToken` cancel at line 545) and drops `GrpcResultStream` before EOF. | `~GrpcResultStream()` executes `TryCancel()` + `stream_->Finish()`; `POST_RECV_STATUS` **still fires** (with `CANCELLED`), proving Arrow's RAII baseline works in real `libgrpc++`. |
| **T1a** | `ListFlights` (`892-914`) | Happy path: 2 valid `pb::FlightInfo` messages + EOF. | `stream->Finish()` runs at line 913; `POST_RECV_STATUS` fires with `OK`; middleware `CallCompleted(OK)` runs. |
| **T1b** | `ListFlights` (`907`, Bug 1A) | Server sends valid `FlightInfo` #1, then malformed `FlightInfo` #2 (`FlightDescriptor::UNKNOWN`), then server closes with `OK` or continues streaming. | `FromProto` fails (`Invalid`), `RETURN_NOT_OK` exits at line 907 without `TryCancel()` or `Finish()`. Verify whether `POST_RECV_STATUS` fires (`false` expected) and observe `GRPC_TRACE` / server cancellation. |
| **T1c** | `ListFlights` (`907`, Bug 1A variant) | Server sends malformed `FlightInfo` (`PATH` descriptor + invalid endpoint URI `"://invalid"`). | Same `FromProto` failure via `Location::Parse`; verify `POST_RECV_STATUS` skipped. |
| **T2a** | `ListFlights` (`910-911`, Bug 1B) | Pre-cancelled `StopToken` (`RequestStop()` before `ListFlights`). | Loop runs 0 times; line 910 calls `TryCancel()`; line 911 returns `Cancelled` without `Finish()`. Empirically test whether `TryCancel()` alone causes `POST_RECV_STATUS` to fire (`PRE_SEND_CANCEL` vs `POST_RECV_STATUS`). |
| **T2b** | `ListFlights` (`910-911`, Bug 1B) | Mid-stream `StopToken` cancellation after item #1 while server streams multiple items with delay. | Loop exits after item #1; line 910 calls `TryCancel()`; line 911 returns `Cancelled` without `Finish()`. Check `POST_RECV_STATUS`, `PRE_SEND_CANCEL`, and server `IsCancelled()`. |
| **T3a** | `ListActions` (`935`, Bug 1C check) | Server sends empty/unusual `pb::ActionType` (`type=""`, `description=""`, binary bytes). | Upstream `FromProto(pb::ActionType, ActionType*)` (`serialization_internal.cc:124-128`) always returns `Status::OK()`. Line 935 early return is **unreachable** (semantic false positive for "malformed ActionType"). |
| **T3b** | `ListActions` (`938-939`, Bug 1D) | Mid-stream `StopToken` cancellation while server streams multiple `ActionType`s. | Line 938 calls `TryCancel()`; line 939 returns `Cancelled` before line 940 `Finish()`. Check `POST_RECV_STATUS` is skipped. |
| **T4a** | `AuthenticateInternal` (`1067-1091`, Bug 1E false-positive check) | `auth_handler_ == nullptr` (as in `AuthenticateBasicToken`) or conforming `ClientAuthHandler`; server returns `grpc::Status(UNAUTHENTICATED, "bad token")` after client `WritesDone()`. | Line 1089 **DOES** call `stream->Finish()`. Verify `AuthenticateInternal` returns `Unauthenticated` (NOT `Status::OK()`!) and `POST_RECV_STATUS` **DOES** fire with `UNAUTHENTICATED`. |
| **T4b** | `AuthenticateInternal` (`1074`, residual Bug 1E-R1) | Server sends challenge `HandshakeResponse`; custom `ClientAuthHandler::Authenticate` reads it (`Read` succeeds) and returns `Status::Invalid("bad challenge")` directly. | `RETURN_NOT_OK` at line 1074 exits without `WritesDone()`, `TryCancel()`, or `Finish()`. Verify `POST_RECV_STATUS` is skipped. |
| **T4c** | `AuthenticateInternal` (`1077-1081`, residual Bug 1E-R2) | Server immediately rejects `Handshake` (`UNIMPLEMENTED` or `UNAUTHENTICATED`) before reading client messages. | Measure empirically in `libgrpc++ 1.72.0` whether `stream->WritesDone()` returns `false` (triggering line 1079 `FlightStatusCode::Internal` and skipping `Finish()`) or `true` (race/timing dependent). |
| **T5a** | `ListFlights_Fixed` | Same triggers as **T1b** (malformed `FlightInfo`) and **T2b** (`StopToken` cancel), calling `rpc.context.TryCancel(); (void)stream->Finish();` on early exits. | Both early exits trigger `POST_RECV_STATUS`, invoke `CallCompleted()`, do not hang even if server has unsent messages, and return the expected `Invalid` / `Cancelled` status. |
| **T5b** | `ListActions_Fixed` | Same trigger as **T3b** (`StopToken` cancel), calling `TryCancel()` + `Finish()`. | `POST_RECV_STATUS` fires and `CallCompleted()` runs cleanly. |
| **T5c** | `AuthenticateInternal_Fixed` | Same triggers as **T4b** (`Authenticate` returning error) and **T4c** (`!finished_writes`), calling `TryCancel()` + `Finish()` / `Finish()` on `!finished_writes`. | `POST_RECV_STATUS` fires on all paths and server `UNAUTHENTICATED`/`UNIMPLEMENTED` status is preserved via `Finish()`. |

---

## 3. Pass / Failure Criteria & Artifacts

- **Pass Criterion for Reproduction Harness:** Every test case executes end-to-end against real `libgrpc++ 1.72.0` over a real loopback gRPC channel without hanging or crashing, logs exact interceptor hook points (`StartCall`, `PRE_SEND_INITIAL_METADATA`, `POST_RECV_INITIAL_METADATA`, `PRE_SEND_CLOSE`, `PRE_SEND_CANCEL`, `POST_RECV_STATUS`), and cleanly settles each bug's verdict (`CONFIRMED`, `PARTIAL / REVISED`, or `FALSE POSITIVE`).
- **Artifacts Generated in `reproductions/arrow-flight/`:**
  - `TEST_PLAN.md` (this file)
  - `proto/Flight.proto` (verbatim copy from upstream commit `fb767a7`)
  - `repro_arrow_flight.cpp` (real `libgrpc++ 1.72.0` loopback harness)
  - `build_and_run.ps1` (builds from `Flight.proto` using MSYS2 UCRT64 `protoc`, `grpc_cpp_plugin`, `g++`, runs normal + `GRPC_TRACE=api,call_error` diagnostics)
  - `TEST_OUTPUT.txt` (verbatim captured execution output + trace excerpt)
  - `FINDINGS.md` (honest per-bug verdicts, real `libgrpc++ 1.72.0` `POST_RECV_STATUS` empirical results, and exact discrepancies vs. `docs/04` and `forensic_bug_verification_by_repo_design.md`)
