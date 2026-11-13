# Runtime Reproductions — Candidate Bug Registry, Methodology & Limitations

Standalone `libgrpc++ 1.72.0` (`libprotoc 31.1`, HTTP/2 loopback) harnesses in this directory verify our **Category D (`[CANDIDATE / UNCONFIRMED UPSTREAM]`)** C++ gRPC streaming protocol bugs alongside non-D controls, false-positive checks, and incidental findings. *(Historical Category A bugs such as NVIDIA Riva PR #56/#57 and baseline Category B/conforming controls such as `google-cloud-cpp` are documented in [`latex/empirical_audit.tex`](../latex/empirical_audit.tex).)*

### ID Naming Convention (Why We Use `CD-*`)
To avoid collision between **Categories A–D** (evidence provenance) and test labels, every finding maps to the master registry in [`docs/04_Candidate_Bugs_and_Disclosure_Drafts.md`](../docs/04_Candidate_Bugs_and_Disclosure_Drafts.md):
- **`CD-01`..`CD-13`:** **Category D** gRPC streaming protocol/typestate candidate bugs (`CD` = *Category D*; *all unconfirmed upstream* — `"Reproduced"` means verified locally on `libgrpc++ 1.72.0`).
- **`CTL-01`..`CTL-04`:** Conforming RAII/concurrency/guard **controls** (`TN`).
- **`FP-01`..`FP-02`:** Unreachable AST **false positive** / retracted claim.
- **`INC-01`..`INC-03`:** **Incidental** non-protocol C++ bugs discovered during auditing/testing (outside session-type scope).
- Legacy labels in `TEST_OUTPUT.txt` logs (`T1b`, `2b_after_stop`, `3A`, `Suite 1`, etc.) were repo-local harness tags (`<Repo#><Subcase>`), **not** Categories A–D; each `FINDINGS.md` maps those log tags to canonical `CD-*` / `CTL-*` / `FP-*` / `INC-*` IDs.

---

## 1. Canonical Category D Bugs & Non-D Checks Mapped to Harnesses

| Harness Directory | Category D Protocol Bugs (`[UNCONFIRMED UPSTREAM]`) | Controls / FPs / Incidental Findings Checked Alongside |
| :--- | :--- | :--- |
| **[`grpc-interceptor/`](./grpc-interceptor/FINDINGS.md)** (`grpc/grpc` @ `30d65f9e86`) | **`CD-01`**: `CachingInterceptor::Intercept()` (`39–105`) omits `stream_->Finish()`, masks status to `OK`, caches empty failed reads, causes `RST_STREAM(8)`, skips `POST_RECV_STATUS` *(fixed in `fixed_caching_interceptor.h` with `ClientBidiReactor` deferral)*. | `T0a` clean baseline; `T0b` notes upstream `server.cc:99` finishes EOF with `CANCELLED`. |
| **[`arrow-flight/`](./arrow-flight/FINDINGS.md)** (`apache/arrow` @ `fb767a7`) | **`CD-02`** (`ListFlights:907` `FromProto`), **`CD-03`** (`ListFlights:910–911` `StopToken`), **`CD-04`** (`ListActions:938–939` `StopToken`), **`CD-05`** (`AuthenticateInternal:1074` auth error), **`CD-06`** (`AuthenticateInternal:1077–1081` partial `!finished_writes`): omit `stream->Finish()` (`POST_RECV_STATUS=0`). | **`CTL-01`** (`GrpcResultStream` RAII passes); **`FP-01`** (`ListActions:935` unreachable `FromProto(ActionType)`); **`FP-02`** (retracted `592–624` claim). |
| **[`buildbox-cas/`](./buildbox-cas/FINDINGS.md)** (`BuildGrid/buildbox` @ `0926670`) | **`CD-07`** (`~GrpcStagedDirectory:1317–1323` skips cleanup `Read()` + `Finish()`, causing `RST_STREAM(8)`, dropped acks/errors, `POST_RECV_STATUS=0`, restage races; `casd` deletes async); **`CD-08`** (`uploadFdRegion:534,546` throw); **`CD-09`** (`download:381` throw). | **`CTL-02`** (`upload(string)` passes); **`INC-01`** (`CasInstance::processWriteRequest` missing `return` on `grpc::Status` at `196–200`, non-protocol). |
| **[`triton-client/`](./triton-client/FINDINGS.md)** (`triton-inference-server/client` @ `13095d2`) | **`CD-10`** (`AsyncStreamInfer` pre-`StartStream()` null deref `0xC0000005` at `1447`); **`CD-11`** (`AsyncStreamInfer` post-`StopStream()` `Write()` at `1447` in `S2_FINISHED` — returns `"Stream has been closed."` cleanly on `libgrpc++`, corrupting `RequestTimers`). | **`CTL-03`** (`StartStream`/`StopStream` guards pass); **`INC-02`** (uninitialized `enable_stream_stats_` UB at `674`); **`INC-03`** (`2d_inflight_cancel` stats-queue leak). |
| **[`wenet-asr/`](./wenet-asr/FINDINGS.md)** (`wenet-e2e/wenet` @ `d170596`) | **`CD-12`** (`operator():155 -> 52` empty stream `CHECK` abort `exit_code=3`; Cat. A WebSocket twin in PR #911); **`CD-13`** (`146–150` missing `has_decode_config()` drops 1st audio chunk, `nbest_=0` emits all `K=3` results). | **`CTL-04`** (`GrpcClient` concurrent `Read`/`Write` `25/25` + `10/10` passes; non-atomic `bool done_` noted). |

---

## 2. Verification Methodology & `.patch` Files

| Tier | Harnesses | Build Strategy & Role of `.patch` Files |
| :--- | :--- | :--- |
| **Tier 1 (Direct Upstream Source)** | `grpc-interceptor`, `triton-client`, `wenet-asr` | `build_and_run.ps1` sparse-clones pinned upstream C++ files into an ignored build/temp folder, runs the **unpatched** code first, then verifies the fix via [`fixed_caching_interceptor.h`](./grpc-interceptor/fixed_caching_interceptor.h) (`grpc`) or minimal upstream diffs ([`triton_client_fix.patch`](./triton-client/triton_client_fix.patch), [`wenet_server_fix.patch`](./wenet-asr/wenet_server_fix.patch)). |
| **Tier 2 (Extracted Loopback Harness)** | `arrow-flight`, `buildbox-cas` | Audited control-flow paths and schemas (`Flight.proto`, `buildbox_repro.proto`) are extracted into standalone `.cpp` harnesses toggling buggy/fixed variants inline instead of porting Parquet/IPC or Linux POSIX/FUSE subsystems to Windows. |

Run any suite on Windows 11 MSYS2 UCRT64 (`g++`, `protoc`, `grpc_cpp_plugin`, `libgrpc++`, `git`):
```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\reproductions\<name>\build_and_run.ps1
```

---

## 3. Limitations & Threats to Validity

1. **Loopback Server Emulation:** Harnesses test local HTTP/2 loopback servers (`127.0.0.1`), not full cluster backends (`tritonserver` GPU execution, external Arrow Flight servers, or `buildboxcasd` FUSE/CAS daemons).
2. **Tier 2 Extraction & Synthetic Fault Triggers:** `arrow-flight` and `buildbox-cas` inject malformed messages, `StopToken` cancellations, C++ exceptions, and `30 ms` delays rather than executing full Linux upstream builds.
3. **Tier 1 Non-gRPC Stubbing (`wenet-asr`):** Torch/Kaldi/glog headers are stubbed in [`harness_stubs/`](./wenet-asr/harness_stubs/) (preserving `CHECK -> std::abort()`), while `grpc_server.cc` and `grpc_client.cc` compile unmodified.
4. **Platform & Toolchain Scope:** Recorded `TEST_OUTPUT.txt` logs (`ExitCode`, `0xC0000005` SEH, `WritesDone()` behavior, millisecond latencies) reflect Windows 11 MSYS2 UCRT64 (`g++ 15.2.0`, `libgrpc++ 1.72.0`).
5. **Unconfirmed Upstream Status:** All `CD-01`..`CD-13` entries remain **Category D (`[CANDIDATE / UNCONFIRMED UPSTREAM]`)** unless subsequently accepted upstream.
