# Runtime Verification Findings: `grpc/grpc` `CachingInterceptor`

- **Verdict:** **`CONFIRMED`** (upstream `examples/cpp/interceptors/caching_interceptor.h` exhibits every runtime failure mode tested—status masking, cache poisoning, phantom empty reads, premature HTTP/2 `RST_STREAM` server cancellation, and skipped downstream `POST_RECV_STATUS`; earlier thesis notes contain **documented discrepancies** in method names, hook placement, and `ClientBidiReactor` patch safety that are corrected below).
- **Upstream Commit Verified:** `30d65f9e86ca605e8cad30989e1e095c5fa4327e` (`examples/cpp/interceptors/caching_interceptor.h`, unmodified)
- **Toolchain & Runtime:** MSYS2 UCRT64 `g++ 15.2.0` (`-std=c++17`), `libprotoc 31.1`, `libgrpc++ 1.72.0` over real HTTP/2 loopback (`127.0.0.1:<ephemeral>`).

---

## 1. Summary of Confirmed Runtime Defects

### 1.1 Unconditional Status Masking (`PRE_RECV_STATUS` without `stream_->Finish()`)
In `examples/cpp/interceptors/caching_interceptor.h:101-105`:
```cpp
if (methods->QueryInterceptionHookPoint(
        grpc::experimental::InterceptionHookPoints::PRE_RECV_STATUS)) {
  auto* status = methods->GetRecvStatus();
  *status = grpc::Status::OK;
}
```
`CachingInterceptor` never invokes `stream_->Finish()` on its backing `ClientReaderWriter` (`stream_`), and unconditionally overwrites `*status = grpc::Status::OK`.

- **Mid-stream failure (`T1`, `T2`):** When the server fails on the 2nd key with `grpc::Status(INTERNAL, "db failure")`, both synchronous (`ClientReaderWriter`, `T1`) and callback (`ClientBidiReactor` mirroring `client.cc`, `T2`) callers receive `OK` (`finish_code=0` / `on_done_code=0`, empty message):
  ```text
  [T1_UPSTREAM_BUGGY] steps=3 server_reads=2 writes_done_ok=true finish_code=0 finish_msg="" finish_ms=0
  [T2_UPSTREAM_BUGGY] completed=1 on_done_code=0 on_done_msg="" server_reads=2 reads_count=3
  ```
- **Immediate failure on 1st key (`T3`):** Even with 0 server responses, client observes `finish_code=0 finish_msg=""`.
- **Post-`WritesDone` failure (`T4`):** Even when all `Write()` / `Read()` pairs succeed and the server returns `INTERNAL("commit failed")` after client `WritesDone()`, `CachingInterceptor` masks it to `finish_code=0`.
- **Upstream `examples/cpp/interceptors/server.cc:99` interaction (`T0b`):** Upstream `server.cc` itself calls `Finish(grpc::Status::CANCELLED)` inside `OnReadDone(bool ok)` when `!ok` (normal client `WritesDone()` EOF). Running a direct client without `CachingInterceptor` returns `finish_code=1 (1=CANCELLED)`; `CachingInterceptor` silently hides this by returning `finish_code=0`:
  ```text
  [T0b_NO_INTERCEPTOR] finish_code=1 (1=CANCELLED)
  [T0b_UPSTREAM_BUGGY] finish_code=0 (masked to OK!)
  [T0b_FIXED]          finish_code=1 (propagated CANCELLED)
  ```

### 1.2 Phantom Empty Responses & Cache Poisoning on Failure
In `examples/cpp/interceptors/caching_interceptor.h:83-89, 96-100`, on a cache miss inside `PRE_SEND_MESSAGE`, `CachingInterceptor` executes:
```cpp
stream_->Write(req);
keyvaluestore::Response resp;
stream_->Read(&resp);
response_ = resp.value();
cached_map_.insert({requested_key, response_});
```
Neither `Write()` nor `Read()` return values are checked, and `PRE_RECV_MESSAGE` never calls `methods->FailHijackedRecvMessage()`.

- When the server aborts on `"key2"` (`T1`, `T2`) or `"key1"` (`T3`) without sending a `Response`, `stream_->Read(&resp)` returns `false` leaving `resp.value() == ""`.
- `CachingInterceptor` returns `read_ok=true value=""` to the caller (`T1_UPSTREAM_BUGGY` `step[1]`) and inserts `{"key2", ""}` into `cached_map_`.
- On retry (`step[2]` / 3rd request), `CachingInterceptor` logs `key2 found in map` (server never sees a 3rd request: `server_reads=2`) and serves the poisoned empty string `""` (`step[2] key=key2 write_ok=true read_ok=true value=""`).

### 1.3 Unfinalized Backing Stream Teardown, Wire `RST_STREAM`, & Skipped Downstream Hooks (`Case B`)
Because `stream_->Finish()` is never called:
1. **Premature return & server-side `RST_STREAM` cancellation (`T5`, `T7`):** When the server performs 150ms of post-`WritesDone()` finalization work and attaches trailing metadata (`x-server-commit: committed`), `CachingInterceptor` returns `OK` immediately (`finish_ms=0`) without awaiting trailers. Destroying `CachingInterceptor` destroys `stream_` and `context_`, invoking `grpc_call_unref` on an unfinished call. Real `libgrpc++ 1.72.0` debug traces (`GRPC_VERBOSITY=DEBUG GRPC_TRACE=api,call_error`) show:
   ```text
   filter_stack_call.cc:271] grpc_call_unref(c=0x1d11d639cb0)
   filter_stack_call.cc:336] CancelWithError CLI CANCELLED:CANCELLED
   filter_stack_call.cc:359] set_final_status SVR UNKNOWN:Error received from peer ipv4:127.0.0.1:57055 {grpc_status:1, grpc_message:"Received RST_STREAM with error code 8"}
   filter_stack_call.cc:766] ops[0]: SEND_STATUS_FROM_SERVER status=0 details=(null)
   key=x-server-commit value=63 6f 6d 6d 69 74 74 65 64 'committed'
   filter_stack_call.cc:336] CancelWithError SVR CANCELLED
   completion_queue.cc:1328] RETURN_EVENT[0x1d11d616a90]: OP_COMPLETE: tag:0x7fb2dff600 ERROR
   ```
   The server handler observes `context->IsCancelled() == true` (`[T5_UPSTREAM_BUGGY] server_exited=1 server_saw_cancelled=true`), and its final `SEND_STATUS_FROM_SERVER` fails.
2. **Downstream interceptor `POST_RECV_STATUS` never fires (`T6`):** A `BackingObserverInterceptor` placed downstream via `GetInterceptedChannel()` observes `pre_send_close=1`, `post_recv_status=0`, `observed_status=<NONE>`.

---

## 2. Verification of Side-by-Side Fix (`FixedCachingInterceptor`)

`fixed_caching_interceptor.h` remedies all defects by:
1. Checking `bool w_ok = stream_->Write(req); bool r_ok = w_ok && stream_->Read(&resp);` and only inserting into `cached_map_` when `w_ok && r_ok`.
2. Calling `methods->FailHijackedRecvMessage()` (and `methods->FailHijackedSendMessage()` on subsequent writes after failure) instead of returning phantom empty `Response` messages.
3. Calling `stream_->Finish()` on stream failure or completion and propagating the actual `grpc::Status` (including deferring eager `PRE_RECV_STATUS` batches submitted at `ClientBidiReactor::StartCall()` time until `PRE_SEND_CLOSE` or mid-stream termination).

Under the exact same tests and traces (`TEST_OUTPUT.txt`):
- **Mid-stream failure (`T1`, `T2`):** `T1_FIXED` returns `read_ok=false` on `key2`, `write_ok=false read_ok=false` on retry, and `finish_code=13 finish_msg="db failure"`. `T2_FIXED` (`ClientBidiReactor`) stops at `read[1](key2,ok=0,val="")` and delivers `on_done_code=13 on_done_msg="db failure"`.
- **Immediate & post-`WritesDone` failures (`T3`, `T4`):** Both propagate `finish_code=13` with `"db failure"` / `"commit failed"` and do not poison `cached_map_`.
- **Delayed server completion (`T5`, `T6`, `T7`):** `T5_FIXED` awaits server completion (`finish_ms=151` / `164` in trace run), server sees `server_saw_cancelled=false`, downstream `BackingObserver` sees `post_recv_status=1 observed_code=0 trailer_commit="committed"`, and `GRPC_TRACE` confirms `set_final_status SVR OK` and `set_final_status CLI OK` with zero `CancelWithError` / `RST_STREAM` on the backing call.

---

## 3. Corrections Required in Earlier Thesis Docs & POCs

Direct inspection and compilation against real upstream `grpc/grpc` uncovered four factual discrepancies in earlier thesis files (`docs/04_Candidate_Bugs_and_Disclosure_Drafts.md`, `latex/empirical_audit.tex`, `poc/real_repo_bugs/real_repos.proto`, `poc/real_repo_bugs/grpc_caching_interceptor_buggy.cpp`, `poc/real_repo_bugs/grpc_caching_interceptor.session.json`):

1. **Incorrect Service/Method Name:** Earlier docs and POCs claimed `CachingInterceptor` intercepts `Greeter.SayHelloStream` (`HelloRequest`/`HelloReply`). In reality, `caching_interceptor.h:48-50` intercepts `keyvaluestore::KeyValueStore::GetValues` (`examples/protos/keyvaluestore.proto`).
2. **Incorrect Caching Loop & Hook Point:** `docs/04_Candidate_Bugs_and_Disclosure_Drafts.md` (`lines 151-163, 315-327`) depicted a `while (stream_->Read(&resp)) { cached_response_.push_back(resp); }` loop inside `PRE_SEND_CLOSE` storing into `(*map_)[req_]`. In reality, `caching_interceptor.h:52-94` performs a single synchronous `stream_->Write(req); stream_->Read(&resp);` per cache miss inside `PRE_SEND_MESSAGE` and inserts into `cached_map_`; `PRE_SEND_CLOSE` only calls `stream_->WritesDone();`.
3. **Naive Patch Deadlocks on Upstream `client.cc` (`ClientBidiReactor`):** `examples/cpp/interceptors/client.cc` uses `grpc::ClientBidiReactor`. In `libgrpc++` (`grpcpp/support/client_callback.h:503`), `ClientCallbackReaderWriterImpl::StartCall()` posts `finish_ops_` (`PRE_RECV_STATUS`) **eagerly at call start** (right after key 1, before `StartWritesDone()` / `PRE_SEND_CLOSE`). Consequently:
   - In the buggy upstream `CachingInterceptor`, `*status = grpc::Status::OK` actually executes inside `StartCall()` before later keys are even written.
   - Simply calling `stream_->Finish()` unconditionally inside `PRE_RECV_STATUS` (as suggested in `docs/04_Candidate_Bugs_and_Disclosure_Drafts.md:321`) would call `Finish()` before `WritesDone()` on `ClientBidiReactor` callers, deadlocking against a streaming server unless `PRE_RECV_STATUS` is deferred until `PRE_SEND_CLOSE` or stream error.
4. **Additional Upstream Example Quirks Observed:**
   - `examples/cpp/interceptors/caching_interceptor.h:76,79` uses `std::cout` / `std::endl` without `#include <iostream>` (relying on `client.cc` include order).
   - `examples/cpp/interceptors/server.cc:99` returns `Finish(grpc::Status::CANCELLED)` on normal client `WritesDone()` EOF (`!ok`), which `CachingInterceptor` masks to `OK`.


---

## Methodology & Limitations

- **Verification Tier 1 — Direct Unmodified Upstream Compilation:** This reproduction compiles the unmodified upstream [`examples/cpp/interceptors/caching_interceptor.h`](https://github.com/grpc/grpc/blob/30d65f9e86ca605e8cad30989e1e095c5fa4327e/examples/cpp/interceptors/caching_interceptor.h) header (`30d65f9e86`) directly against `protoc 31.1`-generated `keyvaluestore.proto` stubs and real `libgrpc++ 1.72.0` over HTTP/2 loopback (`127.0.0.1`). No buggy client-interceptor logic was rewritten or mocked.
- **Harness & Environment Scope:**
  - Server behaviors (`KeyValueStore::GetValues` error injection, delayed trailing-metadata finalization, and the `examples/cpp/interceptors/server.cc` `Finish(CANCELLED)` pattern in `T0b`) are driven by the test server inside `repro_caching_interceptor.cc`.
  - All runs executed on Windows 11 MSYS2 UCRT64 (`g++ 15.2.0`, `libgrpc++ 1.72.0`). Wire/debug trace timings (`GRPC_TRACE=api,call_error`) reflect `libgrpc++ 1.72.0` on Windows rather than upstream Linux CI builds.
