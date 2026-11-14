# Test Plan: BuildGrid/buildbox (`buildbox-common` & `buildbox-casd`) Runtime Verification (`CD-07`..`CD-09`)

> **Provenance & Naming Note:** Tests Category D candidates **`CD-07`..`CD-09`**, control **`CTL-02`**, and incidental non-protocol bug **`INC-01`** (see [`reproductions/README.md`](../README.md) and [`docs/04_Candidate_Bugs_and_Disclosure_Drafts.md`](../../docs/04_Candidate_Bugs_and_Disclosure_Drafts.md)). Test IDs `Suite 1..3` (`1A..1D`, `2A..2C`, `3A..3C`) match `TEST_OUTPUT.txt` harness markers, **not** provenance Categories A–D.

## Objective
Perform an independent, skeptical runtime verification of the candidate gRPC streaming lifecycle bugs in `BuildGrid/buildbox` (`buildbox-common` & `buildbox-casd`) over **real `libgrpc++ 1.72.0` HTTP/2 loopback**, rather than GoogleMock unit tests. Specifically, determine whether each finding is **CONFIRMED**, **PARTIAL** (e.g., intentional trade-off with a real race/error-loss consequence), or a **FALSE POSITIVE**.

## Inspected Upstream Sources (`upstream/buildbox-common`, commit `0926670`)
1. `protos/build/buildgrid/local_cas.proto` (lines 85–102, 362–434)
   - `StageTree(stream StageTreeRequest) returns (stream StageTreeResponse)` spec:
     > *"The server will clean up the staged directory when it either receives an additional request (with all fields unset) or when the stream is closed. The server will send an additional response after cleanup is complete."*
2. `common/buildboxcommon_grpccasclient.cpp`:
   - `download(int fd, const Digest &digest)` (lines 358–431): calls `d_bytestreamClient->Read(&context, request)`, throws `std::system_error` inside the `while (reader->Read(&response))` loop if `write(fd, ...) < 0` (lines 379–385), skipping `reader->Finish()` (line 395).
   - `upload(const std::string &data, const Digest &digest)` (lines 433–499): validates size before opening stream; calls `writer->WritesDone()` and `writer->Finish()` (lines 478–479) before post-`Finish()` size validation. (Clean control.)
   - `uploadFdRegion(int fd, const Digest &digest, int64_t fileReadOffset)` (lines 506–584): calls `d_bytestreamClient->Write(&context, &response)`, throws `std::system_error` if `pread(...) < 0` (lines 533–537) or `std::runtime_error` on unexpected EOF (`bytesRead == 0`, lines 545–550) inside the write loop, skipping both `writer->WritesDone()` (line 564) and `writer->Finish()` (line 565).
   - `stage(...)` (lines 658–716) & `GrpcStagedDirectory::~GrpcStagedDirectory()` (lines 1306–1323): destructor sends `Write(StageTreeRequest())` and `WritesDone()`, then immediately returns and destroys `d_reader_writer` and `d_context` (`~ClientContext()`) without calling the second `Read(&cleanup_resp)` or `Finish()`.
   - (Additional observation: `fetchStream(...)` lines 342–350 returns early on non-OK `streamConsumer` status without calling `reader->Finish()`.)
3. `casd/buildboxcasd_localcasinstance.cpp` (`LocalCasInstance::StageTree`, lines 387–638):
   - Reads cleanup request via `stream->Read(&cleanup_request)` (line 572).
   - Contains explicit comment at lines 589–591:
     > *"The client doesn't block on the RPC after sending the request so these commands are non-blocking. See `CASClient::StagedDirectory::~StagedDirectory()`"*
   - Runs `pre_unstage_commands`, deletes staged tree (`staged_directory.reset()` and `deleteDirectory`), and writes `stream->Write(StageTreeResponse())` if `received_cleanup_request` (line 634), then returns `stage_status`.
4. `casd/buildboxcasd_casinstance.cpp` (`CasInstance::processWriteRequest`, lines 143–241):
   - Lines 196–200:
     ```cpp
     if (request_message->write_offset() != bytes_written) {
         // (Only serial writes are allowed)
         grpc::Status(grpc::StatusCode::OUT_OF_RANGE,
                      "`write_offset` is not valid");
     }
     ```
     Missing `return` statement constructs and immediately discards a temporary `grpc::Status`.
5. `common/test/buildboxcommon_grpccasclient.t.cpp`:
   - `DownloadFdNotWritable` (lines 395–411): uses `MockBytestreamReader` and never expects `Finish()`, masking exception-unwinding stream cancellation.
   - `StageDirectory` (lines 2149–2179): uses `MockGetTreeReaderWriter` (`EXPECT_CALL(*reader_writer, Read(_)).WillOnce(...)` and no `Finish()` expectation), completely masking the real HTTP/2 cancellation and directory-cleanup race. (Contrast with `casd/test/fixtures/buildboxcasd_localcasservice.h:1029-1035` and `casd/benchmark/local_benchmark.cpp:191-196`, which do follow the full `Write(empty)` -> `Read()` -> `Finish()` protocol.)

## Test Matrix

### Suite 1: `~GrpcStagedDirectory()` Teardown over Real HTTP/2 Loopback (`StageTree`)
Simulates `LocalCasInstance::StageTree` with a real staging directory + marker file on disk, optional `pre_unstage_commands` marker, configurable deletion latency (0 ms fast path vs. 30 ms realistic cleanup), and a final `stream->Write(StageTreeResponse())`.
- **Test 1A (Buggy `~GrpcStagedDirectory`, 0 ms cleanup, 20 iterations):**
  - Client executes `Write(stage_req)` -> `Read(&stage_resp)` -> `Write(StageTreeRequest())` -> `WritesDone()`, then immediately destroys `stream` and `context`.
  - Measure per-iteration:
    1. Did server receive the cleanup request (`stream->Read(&cleanup_req) == true`) or did `~ClientContext()` cancel before `Read` saw it?
    2. Did `server_ctx->IsCancelled()` become `true` during server cleanup?
    3. Did `stream->Write(StageTreeResponse())` fail (`false`) on server?
    4. Was the staged directory STILL present on disk at the exact instant `~GrpcStagedDirectory()` returned on the client (filesystem race)?
- **Test 1B (Buggy `~GrpcStagedDirectory`, 30 ms realistic cleanup + `pre_unstage_commands`, 20 iterations):**
  - Same client teardown, with server executing a 30 ms simulated `pre_unstage_commands` + directory tree deletion on disk.
  - Additionally tests **immediate re-stage collision**: client immediately attempts to stage into the same explicit directory path (checking if the directory is still non-empty when the next operation begins!).
- **Test 1C (Fixed `~GrpcStagedDirectory`, 30 ms realistic cleanup + `pre_unstage_commands`, 20 iterations):**
  - Client executes `Write(StageTreeRequest())` -> `WritesDone()` -> `Read(&cleanup_resp)` -> `Finish()`.
  - Verify `Finish().ok()`, `server_ctx->IsCancelled() == false`, `stream->Write(StageTreeResponse()) == true`, staged directory guaranteed deleted on client return, `pre_unstage_commands` completed, and zero re-stage collision.
- **Test 1D (Server Cleanup Error / Protocol Error Propagation):**
  - Server returns `INVALID_ARGUMENT` after cleanup; verify buggy destructor silently drops the error status whereas fixed destructor observes `INVALID_ARGUMENT` via `Finish()`.

### Suite 2: Mid-Stream Exception Unwinding (`ByteStream::Write` & `ByteStream::Read`)
Run with `GRPC_TRACE=call_error,api` enabled to capture any gRPC C++/core warnings or errors.
- **Test 2A (`uploadFdRegion` mid-stream throw — `pread < 0` / unexpected EOF):**
  - Client opens `ByteStream::Write`, writes chunk 1 (`finish_write = false`), then throws `std::system_error` before chunk 2 (`WritesDone()` and `Finish()` skipped), matching `GrpcRetrier::issueRequest()` unwinding `ClientContext`.
  - Observe: client `GRPC_TRACE` output, whether `ClientInterceptor::Intercept` `POST_RECV_STATUS` fires, server `request->Read()` result, server `ctx->IsCancelled()`, and server incomplete buffer state (`commit_write == false`).
  - Contrast with **Test 2A-Fixed** (RAII scope guard calling `context.TryCancel()` + `writer->WritesDone()`/`writer->Finish()` or `writer->Finish()`).
- **Test 2B (`download` mid-stream throw — `write(fd, ...) < 0`):**
  - Server streams 5 chunks (`64 KiB` each, 5 ms between chunks). Client reads chunk 1 and throws `std::system_error` (skipping `reader->Finish()`).
  - Observe: client `GRPC_TRACE` output, interceptor `POST_RECV_STATUS` behavior, server `writer->Write()` failure / `ctx->IsCancelled()` detection.
  - Contrast with **Test 2B-Fixed** (RAII scope guard calling `context.TryCancel()` + `reader->Finish()`).
- **Test 2C (Clean Control: `upload(string)`):**
  - Normal multi-chunk `ByteStream::Write` (`Write` -> `WritesDone` -> `Finish`) plus pre-open validation failure (`data.size() != digest.size_bytes()`). Confirm zero `IsCancelled()`, zero `call_error`, `Finish().ok()`.

### Suite 3: Bonus Check — `CasInstance::processWriteRequest` Missing `return`
- Faithfully mirror `CasInstance::processWriteRequest` (`buildboxcasd_casinstance.cpp:176-240`) inside a real `ByteStream::Write` server handler.
- **Test 3A (Invalid non-zero initial `write_offset`, matching total length & digest):**
  - Client sends chunk 1 with `write_offset = 999` (when `bytes_written == 0`), `data = "hello"`, `finish_write = false`; chunk 2 with `write_offset = 5`, `data = "world"`, `finish_write = true`.
  - Verify buggy handler fails to return `OUT_OF_RANGE`, appends data anyway, and returns `grpc::Status::OK` (`committed_size = 10`)!
- **Test 3B (Invalid `write_offset` with extra/mismatched bytes):**
  - Client sends chunk 1 (`write_offset = 0`, `"hello"`), chunk 2 (`write_offset = 0` duplicate, `"hello"`), `finish_write = true` for digest of `"hello"` (5 bytes).
  - Verify buggy handler returns `INVALID_ARGUMENT` (size mismatch) instead of `OUT_OF_RANGE` (`"`write_offset` is not valid"`).
- **Test 3C (Fixed `return grpc::Status(OUT_OF_RANGE, ...)`):**
  - Verify immediate `OUT_OF_RANGE` response.
