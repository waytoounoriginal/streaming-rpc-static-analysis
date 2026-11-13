# Empirical Verification Findings: BuildGrid/buildbox gRPC Streaming Protocols (`CD-07`..`CD-09`)

**Provenance & Naming Convention:** `CD-07`..`CD-09` are **Category D (`[CANDIDATE / UNCONFIRMED UPSTREAM]`)** gRPC protocol bugs mapped in [`docs/04_Candidate_Bugs_and_Disclosure_Drafts.md`](../../docs/04_Candidate_Bugs_and_Disclosure_Drafts.md) and [`reproductions/README.md`](../README.md); `"Reproduced"` means verified locally in `libgrpc++ 1.72.0` loopback runs, **not** accepted upstream. Legacy `Bug 4A/4B` and `Suite 1/2/3` are local harness tags, not Categories A–D; `CTL-02` is a conforming control and `INC-01` is an incidental non-protocol bug outside session-type scope.  
**Target Upstream Commit:** `buildbox-common` `0926670` (inspected in `repro/stagetree-teardown` worktree)  
**Test Environment:** MSYS2 UCRT64 on Windows — `libgrpc++ 1.72.0`, `libprotoc 31.1`, `g++ 15.2.0` (`-std=c++17 -O2 -Wall -Wextra -DPROTOBUF_USE_DLLS`) over real HTTP/2 loopback (`127.0.0.1:2056`) with `GRPC_TRACE=call_error,api`.

---

## Summary of Verdicts

| Canonical ID *(Legacy Tag)* | Upstream Location(s) | Local Verdict *(Upstream Status)* | Key Nuance / Impact |
|---|---|---|---|
| **`CD-07`** *(`Bug 4A` / `Suite 1`)*: `~GrpcStagedDirectory()` omits second `Read()` + `Finish()` | `common/buildboxcommon_grpccasclient.cpp:1317-1323`<br>`common/buildboxcommon_grpccasclient.h:121-124` | **REPRODUCED PROTOCOL VIOLATION & CLEANUP RACE (Cat. D)** *(intentional non-blocking `casd` nuance)* | Earlier claim of server cancelling mid-deletion is inaccurate for `buildboxcasd` (server ignores `IsCancelled()` and finishes deletion), **but** real `libgrpc++` shows `RST_STREAM(8)` cancellation (20/20), `0/20` server ack `Write`, `20/20` immediate restage collisions, silent server error loss, and `0/20` `POST_RECV_STATUS`. |
| **`CD-08` & `CD-09`** *(`Bug 4B` / `Suite 2`)*: Exception unwinding skips `Finish()` in `uploadFdRegion()` (`CD-08`) & `download()` (`CD-09`) vs **`CTL-02`** `upload(string)` | `common/buildboxcommon_grpccasclient.cpp:358-431, 433-505, 506-584`<br>`common/buildboxcommon_grpcretrier.cpp:78-100` | **REPRODUCED (Cat. D)** + **`CTL-02` VERIFIED (`TN`)** | Throwing mid-stream does not crash `libgrpc++`, cleanly triggers `RST_STREAM(8)` (`server IsCancelled() == true`), breaks server `download` `Write()`, and bypasses `ClientInterceptor::POST_RECV_STATUS` (`0/1`). RAII guard restores clean teardown (`1/1`). (`fetchStream` at `L344-346` shares the early-return `Finish()` omission.) |
| **`INC-01`** *(`Bonus` / `Suite 3`, non-protocol)*: Missing `return` in `CasInstance::processWriteRequest()` | `casd/buildboxcasd_casinstance.cpp:196-200` | **REPRODUCED (Incidental Non-Protocol C++ Bug)** | `g++ -Wall -Wextra` emits **0 warnings** (`grpc::Status` lacks unconditional `[[nodiscard]]`). Invalid `write_offset=999` returns `OK (0)`; duplicate `offset=0` corrupts buffer (`"hellohello"`) and fails with `INVALID_ARGUMENT (3)` instead of `OUT_OF_RANGE (11)`. |

---

## Detailed Analysis

### 1. `CD-07` (`Bug 4A`) — `GrpcStagedDirectory::~GrpcStagedDirectory()`

#### Upstream Code Reality vs. Earlier Claim
In `common/buildboxcommon_grpccasclient.cpp:1317-1323`:
- `~GrpcStagedDirectory()` calls `d_reader_writer->Write(StageTreeRequest())` and `d_reader_writer->WritesDone()`, then immediately returns without calling `d_reader_writer->Read(&response)` or `d_reader_writer->Finish()`.
- In `common/buildboxcommon_grpccasclient.h:121-124`, `d_context` is declared before `d_reader_writer`, so member destruction destroys `d_reader_writer` and then immediately runs `~ClientContext()` (`grpc_call_unref`).

**Important Skeptical Nuance (Why Earlier Claim Overstated Mid-Deletion Cancellation):**
- Earlier text in `docs/04_Candidate_Bugs_and_Disclosure_Drafts.md` (`L142, 284`) stated that skipping `Read()` + `Finish()` *"causes the server-side call to cancel mid-cleanup"*.
- Direct inspection of `casd/buildboxcasd_localcasinstance.cpp:572-637` proves this design choice was made intentionally to avoid blocking the client, and `buildboxcasd` compensates by **not** gating cleanup on cancellation:
  - `L572`: `received_cleanup_request = stream->Read(&cleanup_request);`
  - `L589-591`: *"The client doesn't block on the RPC after sending the request so these commands are non-blocking. See `CASClient::StagedDirectory::~StagedDirectory()`"*
  - `L592-623`: executes `pre_unstage_commands`, `staged_directory.reset()`, and `deleteDirectory` unconditionally without checking `ctx->IsCancelled()`.
  - `L634`: `if (received_cleanup_request) { stream->Write(StageTreeResponse()); }`
- Over real HTTP/2 loopback (`Suite 1A` with `0ms` delay and `Suite 1B` with `30ms` + `pre_unstage_commands`), `server received_cleanup_request` succeeded **20/20** and `server IsCancelled (before del)` was **0/20** because HTTP/2 delivers `Write(StageTreeRequest())` + client `END_STREAM` in order prior to `RST_STREAM(CANCEL)`. Thus, against `buildboxcasd`, directory deletion still executes asynchronously after the client destructor returns.

#### Real Bugs & Protocol Violations Proven Over `libgrpc++ 1.72.0`
Despite `buildboxcasd` ignoring `IsCancelled()` during deletion, omitting the second `Read()` + `Finish()` still causes five concrete failures verified at runtime:
1. **Wire Cancellation (`RST_STREAM`) & Server Ack Failure (`Suite 1A`, `1B`):** `~ClientContext()` logs `CancelWithError CLI CANCELLED:CANCELLED`, delivering HTTP/2 `RST_STREAM` error code 8 to the server (`server IsCancelled (after del): 20/20`). Server cleanup ack `stream->Write(StageTreeResponse())` fails **20/20** (`server ack Write(StageTreeResp): 0/20`; `GRPC_TRACE`: `Attempt to send message after stream was closed.` and `SEND_STATUS_FROM_SERVER status=0` fails with `CancelWithError SVR CANCELLED`).
2. **Protocol Contract Violation:** `protos/build/buildgrid/local_cas.proto:85-101` (`L97-100`) explicitly specifies: *"The server will send an additional response after cleanup is complete"*. Furthermore, `casd`'s own integration fixture (`casd/test/fixtures/buildboxcasd_localcasservice.h:1029-1035`) and benchmark (`casd/benchmark/local_benchmark.cpp:191-196`) obey the spec by calling `Write(StageTreeRequest())`, `Read(&response)`, and `Finish()`. Any third-party `StageTree` implementation respecting `IsCancelled()` would abort cleanup.
3. **Filesystem & Immediate Re-stage Race (`Suite 1B`, `1B-Restage` vs `1C`, `1C-Restage`):** Because `local_cas.proto:376-378` requires `path` to point to an empty directory or not exist yet, returning before server cleanup finishes causes immediate filesystem hazards:
   - Non-empty directory observed by client upon `~GrpcStagedDirectory()` return: **20/20** buggy vs **0/20** fixed (`30ms`, `1B` vs `1C`); even at `0ms` (`Suite 1A`), thread scheduling still raced **20/20**.
   - Incomplete `pre_unstage_commands` on return: **20/20** buggy vs **0/20** fixed.
   - Immediate re-stage collision on the same `path`: **20/20** buggy vs **0/20** fixed.
4. **Silent Loss of Server Cleanup Failures (`Suite 1D`):** When the server fails `pre_unstage_commands` or cleanup and returns `INTERNAL (13)`, the buggy destructor drops the status (`<NOT CALLED - SILENTLY DROPPED>`), whereas the fixed version surfaces `13 (Simulated pre-unstage command / cleanup error)`.
5. **Client Interceptor Bypass:** `ClientInterceptor::POST_RECV_STATUS` fired **0/20** in `Suite 1A`/`1B` and **0/1** in `Suite 1D` (`fixed=false`), versus **20/20** and **1/1** with `Read()` + `Finish()`.

---

### 2. `CD-08` & `CD-09` (`Bug 4B`) — Exception-Unwinding Stream Leaks in `uploadFdRegion()` & `download()`

#### Upstream Code Paths
- `GrpcRetrier::issueRequest()` (`common/buildboxcommon_grpcretrier.cpp:78-100`) constructs `std::unique_ptr<grpc::ClientContext> context` (`L79-89`) and invokes `d_grpcInvocation(*context)` (`L99`) with no `try/catch`.
- `GrpcCASClient::uploadFdRegion()` (`common/buildboxcommon_grpccasclient.cpp:506-584`) opens a client stream (`Write`) and inside the chunk loop throws `std::system_error` if `pread < 0` (`L533-537`) or `std::runtime_error("unexpected end of file")` if `bytesRead == 0` (`L545-550`), bypassing `WritesDone()` (`L564`) and `Finish()` (`L565`).
- `GrpcCASClient::download()` (`common/buildboxcommon_grpccasclient.cpp:358-431`) opens a server stream (`Read`) at `L371` and inside the read loop throws `std::system_error` if `write(fd, ...) < 0` (`L380-385`), bypassing `reader->Finish()` (`L395`).
- **Clean Control — `GrpcCASClient::upload(std::string, Digest)` (`L433-505`):** validates string size (`L438-445`) *before* opening the stream and unconditionally calls `WritesDone()` + `Finish()` (`L478-479`) before throwing on post-`Finish()` size mismatch (`L481`).
- **Bonus Static Finding:** `GrpcCASClient::fetchStream()` (`L280-356`) also returns early at `L345` without calling `reader->Finish()` when `!consumeStatus.ok()`.

#### Runtime Behavior (`Suite 2A`, `2B`, `2C`)
- **`uploadFdRegion` (`Suite 2A`):** Throwing mid-loop does **not** trigger a fatal assertion or abort in `libgrpc++ 1.72.0`. Instead, `~ClientContext()` cancels the active HTTP/2 call (`CancelWithError CLI CANCELLED:CANCELLED`), server sees `Received RST_STREAM with error code 8` and `server IsCancelled() == true`, `SEND_STATUS_FROM_SERVER status=3` fails with `CancelWithError SVR CANCELLED`, and client `POST_RECV_STATUS` fires **0/1**.
  - With RAII `TryCancel() + Finish()`, client receives `CANCELLED (1)`, server exits cleanly, and `POST_RECV_STATUS` fires **1/1** (`CANCELLED=1`).
  - With RAII `WritesDone() + Finish()` (safe when the server exits immediately on EOF), `server IsCancelled()` stays `false`, the server's real status (`3: \`finish_write\` was not set in the last request.`) reaches the client, and `POST_RECV_STATUS` fires **1/1** (`OTHER=1`).
- **`download` (`Suite 2B`):** Throwing after chunk 1 (`65536` bytes) aborts the stream mid-transfer; server's next `writer->Write()` fails (`true`), `GRPC_TRACE` logs `Attempt to send message after stream was closed.`, `server IsCancelled()` is `true`, and `POST_RECV_STATUS` fires **0/1**. RAII `TryCancel() + Finish()` preserves clean shutdown and restores `POST_RECV_STATUS = 1/1`.
- **`upload(string)` Control (`Suite 2C`):** Completes cleanly (`Finish().ok() == true`, `committed=131072`, `server IsCancelled() == false`, `POST_RECV_STATUS = 1/1`).

---

### 3. Why Upstream GoogleMock Unit Tests Missed `CD-07`–`CD-09` (`Bug 4A` & `Bug 4B`)
- `common/test/buildboxcommon_grpccasclient.t.cpp:395-411` (`DownloadFdNotWritable`) stubs `d_bytestreamClient->ReadRaw` with a `MockBytestreamReader` and never sets `EXPECT_CALL(*reader, Finish())`.
- `common/test/buildboxcommon_grpccasclient.t.cpp:2149-2179` (`StageDirectory`) stubs `StageTreeRaw` with a `MockGetTreeReaderWriter` expecting only one `Read()` and zero `Finish()` calls.
- Because GoogleMock objects simply destruct without HTTP/2 transport semantics, missing `Read()`/`Finish()` calls, `RST_STREAM(8)` errors, `POST_RECV_STATUS` interceptor omissions, and filesystem cleanup races remain completely invisible in unit tests.

---

### 4. Bonus Check — `CasInstance::processWriteRequest` Missing `return`

In `casd/buildboxcasd_casinstance.cpp:196-200`, the validation branch constructs:
```cpp
grpc::Status(grpc::StatusCode::OUT_OF_RANGE, "`write_offset` is not valid");
```
without `return`, falling through into data appending at `L203-206`.
- **Compiler Check:** `grpc::Status` in `grpcpp/impl/status.h` uses `GRPC_MUST_USE_RESULT_WHEN_USE_STRICT_WARNING`, not unconditional `[[nodiscard]]`; `g++ 15.2.0 -Wall -Wextra` emitted **0 warnings** on the identical discarded temporary expression.
- **Runtime Reproduction (`Suite 3A`, `3B`, `3C`):**
  - Sending `write_offset=999` with payload matching the digest (`"helloworld"`, `committed_size=10`) returned **`0 ()`** (`Status::OK`) instead of `OUT_OF_RANGE (11)`.
  - Re-sending a duplicate chunk at `write_offset=0` corrupted the server write buffer to `"hellohello"` and failed with **`INVALID_ARGUMENT (3)` (`Data size does not match digest size.`)** instead of `OUT_OF_RANGE (11)`.
  - Restoring `return` (`Suite 3C`) correctly returned **`11 (\`write_offset\` is not valid)`**.

---

## Verbatim Summary Output Excerpt (`TEST_OUTPUT.txt` L1–114 & L833–834)

```text
=== ENVIRONMENT ===
protoc: libprotoc 31.1
g++: g++.exe (Rev8, Built by MSYS2 project) 15.2.0

=== FULL REPRODUCTION SUITE OUTPUT ===
gRPC C++ Version: 1.72.0 | Loopback Server: 127.0.0.1:2056

[Suite 1A - Buggy ~GrpcStagedDirectory (0ms)] (20 iterations, delay=0ms, fixed=false)
  server received_cleanup_request : 20/20
  server IsCancelled (before del) : 0/20
  server IsCancelled (after del)  : 20/20
  server ack Write(StageTreeResp) : 0/20
  client saw non-empty dir on ret : 20/20
  interceptor POST_RECV_STATUS    : 0/20 (OK=0, CANCELLED=0)

[Suite 1B - Buggy ~GrpcStagedDirectory (30ms + pre_unstage)] (20 iterations, delay=30ms, fixed=false)
  server received_cleanup_request : 20/20
  server IsCancelled (before del) : 0/20
  server IsCancelled (after del)  : 20/20
  server ack Write(StageTreeResp) : 0/20
  client saw non-empty dir on ret : 20/20
  client saw pre_unstage missing  : 20/20
  interceptor POST_RECV_STATUS    : 0/20 (OK=0, CANCELLED=0)

[Suite 1C - Fixed ~GrpcStagedDirectory (30ms + pre_unstage)] (20 iterations, delay=30ms, fixed=true)
  server received_cleanup_request : 20/20
  server IsCancelled (before del) : 0/20
  server IsCancelled (after del)  : 0/20
  server ack Write(StageTreeResp) : 20/20
  client saw non-empty dir on ret : 0/20
  client saw pre_unstage missing  : 0/20
  client Finish().ok()            : 20/20
  interceptor POST_RECV_STATUS    : 20/20 (OK=20, CANCELLED=0)

[Suite 1B-Restage - Buggy immediate reuse of same path (30ms)] (20 iterations, delay=30ms, fixed=false)
  immediate restage collisions    : 20/20

[Suite 1C-Restage - Fixed immediate reuse of same path (30ms)] (20 iterations, delay=30ms, fixed=true)
  immediate restage collisions    : 0/20

[Suite 1D - Server Cleanup Error Propagation, fixed=false]
  server returned status          : 13 (Simulated pre-unstage command / cleanup error)
  server ack_write_ok             : false
  client Finish() observed        : <NOT CALLED - SILENTLY DROPPED>
  interceptor POST_RECV_STATUS    : 0/1

[Suite 1D - Server Cleanup Error Propagation, fixed=true]
  server returned status          : 13 (Simulated pre-unstage command / cleanup error)
  server ack_write_ok             : true
  client Finish() observed        : 13 (Simulated pre-unstage command / cleanup error)
  interceptor POST_RECV_STATUS    : 1/1

[Suite 2A - uploadFdRegion mid-stream throw: BUGGY (no WritesDone/Finish)]
  client caught exception         : Upload failed: unexpected end of file
  guard Finish() status           : <SKIPPED>
  server chunks received          : 1 (65536 bytes)
  server Read() exited cleanly    : true
  server IsCancelled()            : true
  server handler returned         : 3 (`finish_write` was not set in the last request.)
  interceptor POST_RECV_STATUS    : 0/1 (CANCELLED=0, OTHER=0)

[Suite 2A - uploadFdRegion mid-stream throw: FIXED RAII (TryCancel + Finish)]
  client caught exception         : Upload failed: unexpected end of file
  guard Finish() status           : 1 (CANCELLED)
  server chunks received          : 1 (65536 bytes)
  server Read() exited cleanly    : true
  server IsCancelled()            : true
  server handler returned         : 3 (`finish_write` was not set in the last request.)
  interceptor POST_RECV_STATUS    : 1/1 (CANCELLED=1, OTHER=0)

[Suite 2A - uploadFdRegion mid-stream throw: FIXED RAII (WritesDone + Finish)]
  client caught exception         : Upload failed: unexpected end of file
  guard Finish() status           : 3 (`finish_write` was not set in the last request.)
  server chunks received          : 1 (65536 bytes)
  server Read() exited cleanly    : true
  server IsCancelled()            : false
  server handler returned         : 3 (`finish_write` was not set in the last request.)
  interceptor POST_RECV_STATUS    : 1/1 (CANCELLED=0, OTHER=1)

[Suite 2B - download mid-stream write() throw, fixed=false]
  client caught exception         : Error in write to descriptor: No space left on device: No space left on device
  guard Finish() status           : <SKIPPED>
  server chunks sent              : 1/5 (65536 bytes)
  server writer->Write() failed   : true
  server IsCancelled()            : true
  interceptor POST_RECV_STATUS    : 0/1 (CANCELLED=0)

[Suite 2B - download mid-stream write() throw, fixed=true]
  client caught exception         : Error in write to descriptor: No space left on device: No space left on device
  guard Finish() status           : 1 (CANCELLED)
  server chunks sent              : 1/5 (65536 bytes)
  server writer->Write() failed   : true
  server IsCancelled()            : true
  interceptor POST_RECV_STATUS    : 1/1 (CANCELLED=1)

[Suite 2C - Clean Control: upload(string)]
  client Finish().ok()            : true (committed=131072)
  server chunks received          : 2
  server IsCancelled()            : false
  interceptor POST_RECV_STATUS    : 1/1 (OK=1)

[Suite 3A - CasInstance::processWriteRequest missing `return` (invalid offset=999, matching digest)]
  expected code (if fixed)        : OUT_OF_RANGE (11)
  actual client Finish() status   : 0 (), committed_size=10
  server accumulated bytes        : "helloworld"

[Suite 3B - CasInstance::processWriteRequest missing `return` (duplicate offset=0)]
  expected code (if fixed)        : OUT_OF_RANGE (11)
  actual client Finish() status   : 3 (Data size does not match digest size.)
  server corrupted buffer         : "hellohello"

[Suite 3C - Fixed CasInstance::processWriteRequest (`return grpc::Status(OUT_OF_RANGE, ...)`)]
  actual client Finish() status   : 11 (`write_offset` is not valid)

=== COMPILER DIAGNOSTIC CHECK (-Wall -Wextra on discarded grpc::Status) ===
(0 warnings emitted by g++ -Wall -Wextra because grpc::Status lacks unconditional [[nodiscard]])
```


---

## Methodology & Limitations

- **Verification Tier 2 — Extracted Standalone Loopback Harness (Not Full Upstream Build):** Full `BuildGrid/buildbox` (`buildbox-common` and `buildboxcasd`, which rely on Linux POSIX FDs, `pread`/`write`, and FUSE/CAS storage backends) was not compiled natively on Windows MSYS2. Instead, `repro_main.cpp` extracts the audited gRPC stream-control logic of `GrpcStagedDirectory`, `GrpcCASClient::{uploadFdRegion, download, upload}`, `LocalCasInstance::StageTree`, and `CasInstance::processWriteRequest`, compiled against `buildbox_repro.proto` (`protoc 31.1`) and real `libgrpc++ 1.72.0` over HTTP/2 loopback (`127.0.0.1:2056`).
- **Simulated Subsystems & Threats to Validity:**
  - `buildbox_repro.proto` is a 56-line minimal schema preserving `LocalContentAddressableStorage::StageTree` and `ByteStream::{Read,Write}` wire signatures rather than the full upstream proto tree.
  - In `Suite 1`, `pre_unstage_commands` and unstaging disk cleanup are simulated with local filesystem operations and `0 ms` / `30 ms` delays rather than real `buildboxcasd` FUSE/CAS workers; non-blocking cancellation semantics were cross-checked directly against upstream `casd/buildboxcasd_localcasinstance.cpp:572-637` (`0926670`).
  - In `Suite 2`, I/O errors are triggered by injected `std::system_error` / `std::runtime_error` exceptions inside the transfer loops rather than failing OS `pread`/`write` descriptors.
  - All runs executed on Windows 11 MSYS2 UCRT64 (`g++ 15.2.0`, `libgrpc++ 1.72.0`).
