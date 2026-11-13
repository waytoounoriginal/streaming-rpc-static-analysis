# Test Plan: Runtime Verification of `grpc/grpc` `CachingInterceptor`

## 1. Objective & Scope

Perform an independent, skeptical runtime verification of the candidate protocol-conformance bug in upstream `grpc/grpc` (`examples/cpp/interceptors/caching_interceptor.h`) against real `libgrpc++ 1.72.0` (MSYS2 UCRT64 on Windows) and the real generated `keyvaluestore.proto` stubs.

This test suite evaluates:
1. **Exact upstream mechanism fidelity:** Verify how the actual unmodified `examples/cpp/interceptors/caching_interceptor.h` interacts with `keyvaluestore::KeyValueStore::GetValues` (both with a clean server and with the upstream `examples/cpp/interceptors/server.cc` reactor logic).
2. **Case A (Mid-stream / Post-`WritesDone` server errors, status masking, and cache poisoning):** Test whether upstream `CachingInterceptor` masks server errors (`INTERNAL`, `CANCELLED`) as `grpc::Status::OK`, fabricates empty responses on `Read()` failure, and poisons `cached_map_` on subsequent retries.
3. **Case B (Unfinalized backing stream lifecycle & `GRPC_TRACE`):** Test what `libgrpc++ 1.72.0` does at runtime when `stream_` (`grpc::ClientReaderWriter`) is destroyed without calling `stream_->Finish()`, including server-side cancellation/teardown effects, downstream interceptor `POST_RECV_STATUS` firing, trailing metadata delivery, and `GRPC_VERBOSITY=DEBUG` / `GRPC_TRACE=api,call_error` logs.
4. **Case C (Side-by-side fixed interceptor):** Implement a corrected `FixedCachingInterceptor` that checks `Write()` / `Read()` return values, avoids caching failed lookups, fails hijacked recv messages on stream error, calls `stream_->Finish()`, and propagates the real `grpc::Status`, and verify all Case A & Case B scenarios pass.

## 2. Discrepancies Found During Upstream Code Inspection

Before writing runtime tests, direct inspection of the upstream worktree (`30d65f9e86`) revealed critical inaccuracies in earlier thesis notes (`docs/04_Candidate_Bugs_and_Disclosure_Drafts.md`, `poc/real_repo_bugs/grpc_caching_interceptor_*`, `latex/empirical_audit.tex`):

- **Service/Method:** Upstream `CachingInterceptor` intercepts `keyvaluestore::KeyValueStore::GetValues` (`examples/protos/keyvaluestore.proto`), **not** `Greeter::SayHelloStream`.
- **Caching Mechanism:** `CachingInterceptor` does **not** loop `while (stream_->Read(&resp))` inside `PRE_SEND_CLOSE` or populate a `std::vector` in `(*map_)[req_]`. Instead, it opens a synchronous `stream_ = stub_->GetValues(&context_)` on `PRE_SEND_INITIAL_METADATA`, performs a synchronous `stream_->Write(req); stream_->Read(&resp);` per cache miss on `PRE_SEND_MESSAGE`, and inserts `{requested_key, response_}` into `std::map<std::string, std::string> cached_map_`.
- **Upstream `server.cc` quirk:** `examples/cpp/interceptors/server.cc:99` itself finishes every normal `WritesDone()` (`OnReadDone(false)`) with `Finish(grpc::Status::CANCELLED)`. Because `CachingInterceptor` skips `Finish()` and hardcodes `*status = grpc::Status::OK`, this server-side `CANCELLED` status is silently masked in the upstream example.

## 3. Test Matrix

| ID | Scenario | Outer Client API | Interceptor | Expected Failure Mode (Buggy) / Verification (Fixed) |
| :--- | :--- | :--- | :--- | :--- |
| **T0a** | Clean server happy path (`key1`, `key2`, `key1`) | Sync `ClientReaderWriter` | Upstream `CachingInterceptor` | Cache miss for `key1`/`key2`, cache hit for second `key1`, `Status::OK`. |
| **T0b** | Upstream `server.cc` behavior (`Finish(CANCELLED)` on EOF) | Sync `ClientReaderWriter` | None vs Upstream `CachingInterceptor` vs Fixed | Un-intercepted & Fixed observe `CANCELLED`; Upstream `CachingInterceptor` masks it to `OK`. |
| **T1** | Case A1: Mid-stream `INTERNAL("db failure")` after `key1`, retry `key2` | Sync `ClientReaderWriter` | Upstream vs Fixed | Buggy: phantom `""` read, `"key2"` cached as `""` (hit on retry), `Status::OK` returned.<br>Fixed: `Read` returns `false`, `"key2"` not cached, `INTERNAL("db failure")` returned. |
| **T2** | Case A1b: Mid-stream `INTERNAL("db failure")` with `client.cc` reactor pattern | Callback `ClientBidiReactor` | Upstream vs Fixed | Buggy: `OnReadDone(true)` prints `key2 : `, caches `""`, `OnDone(OK)`.<br>Fixed: `OnReadDone(false)` stops cleanly, `OnDone(INTERNAL)` fires. |
| **T3** | Case A2: Immediate `INTERNAL("db failure")` on 1st key (`key1`, `key1`) | Sync `ClientReaderWriter` | Upstream vs Fixed | Buggy: 0 server values, `"key1"` poisoned as `""`, `Status::OK` returned.<br>Fixed: `Read` returns `false`, `"key1"` not cached, `INTERNAL` returned. |
| **T4** | Case A3: Post-`WritesDone` `INTERNAL("commit failed")` after clean reads | Sync `ClientReaderWriter` | Upstream vs Fixed | Buggy: all `Write`/`Read` succeed, `PRE_RECV_STATUS` returns `Status::OK`.<br>Fixed: `stream_->Finish()` catches and returns `INTERNAL("commit failed")`. |
| **T5** | Case B1: Delayed server `Finish(OK)` + trailing metadata on `WritesDone` | Sync `ClientReaderWriter` | Upstream vs Fixed | Buggy: outer `Finish()` returns before server finishes; `~CachingInterceptor` destroys `stream_`/`context_` and cancels server call (`IsCancelled() == true`), losing trailers.<br>Fixed: waits for server, `IsCancelled() == false`, trailing metadata delivered. |
| **T6** | Case B2: Downstream interceptor hook audit (`POST_RECV_STATUS`) | Sync `ClientReaderWriter` | Upstream vs Fixed (+ observer interceptor) | Buggy: downstream observer never sees `POST_RECV_STATUS` on backing `stream_`.<br>Fixed: downstream observer sees `POST_RECV_STATUS` with actual status. |
| **T7** | Case B3: Real `libgrpc++` diagnostic logs (`GRPC_VERBOSITY=DEBUG`, `GRPC_TRACE=api,call_error`) | Sync `ClientReaderWriter` | Upstream `CachingInterceptor` | Capture verbatim gRPC C-core/C++ runtime traces during unfinalized `stream_` destruction. |

## 4. Build & Reproducibility

- **Compiler & Libraries:** MSYS2 UCRT64 `g++ 15.2.0` (`-std=c++17`), `libprotoc 31.1`, `grpc_cpp_plugin.exe`, `libgrpc++ 1.72.0`.
- **Direct Upstream Header Inclusion:** Compile directly against unmodified `examples/cpp/interceptors/caching_interceptor.h` from the upstream `grpc` worktree (`repro/caching-interceptor`) using `-I`.
- **Execution Script:** `build_and_run.ps1` generates protobuf/gRPC sources into `build/`, builds the reproducer executable, runs the full test suite (normal + traced), and writes verbatim output to `TEST_OUTPUT.txt`.
