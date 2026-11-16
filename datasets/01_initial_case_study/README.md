# Dataset 1: Initial Case Study (`datasets/01_initial_case_study/`)

These two self-contained examples demonstrate the complete pipeline on both **client** (`grpc::ClientReaderWriter`) and **dual server** (`grpc::ServerReaderWriter`) endpoints—compiling `.proto` `MethodOptions` into dual `grpc.session.v1` state machines, verifying C++ source files statically via Clang AST (`ClangSessionChecker`), and executing the exact same handlers over real `libgrpc++ 1.72.0` HTTP/2 loopback.

---

## 1. [`01_pingpong_handshake/`](./01_pingpong_handshake/handshake.proto) — Level 1 Sequencing, Termination & Endpoint Duality ($\overline{S}$)

- **Protocol (`Exchange`):** `!HandshakeMsg; ?AckMsg; rec X. (+{next: !HandshakeMsg; X, stop: WritesDone; ?AckMsg; end})`
- **Verified Targets:**
  - [`client_good.cpp`](./01_pingpong_handshake/client_good.cpp) (`PASS`) & [`server_good.cpp`](./01_pingpong_handshake/server_good.cpp) (`PASS` against generated [`exchange_server_dual.session.json`](./01_pingpong_handshake/exchange_server_dual.session.json)) — completes with `Status::OK`, `count=2`.
  - [`client_bad_swap.cpp`](./01_pingpong_handshake/client_bad_swap.cpp) (`FAIL` statically: illegal `Read()` in `S0`) — deadlocks at runtime (`DEADLINE_EXCEEDED=4` after `200 ms`).
  - [`client_bad_leak.cpp`](./01_pingpong_handshake/client_bad_leak.cpp) (`FAIL` statically: stream leak in `S2` on early return) — triggers `server_saw_cancelled=1`.
  - [`server_bad_order.cpp`](./01_pingpong_handshake/server_bad_order.cpp) (`FAIL` statically against `server_dual`: illegal `Write()` in `S0`).
  - [`server_bad_missing_summary.cpp`](./01_pingpong_handshake/server_bad_missing_summary.cpp) (`FAIL` statically against `server_dual`: returns `OK` in non-terminal `S2`) — client receives `got_summary=0`.

## 2. [`02_config_stream/`](./02_config_stream/config_stream.proto) — Level 2 `oneof` Variant Progression & Post-Close Guard

- **Protocol (`Upload`):** `!Frame[init]; rec X. (+{data: !Frame[chunk]; X, done: WritesDone; ?Summary; end})` on `oneof body { InitConfig init = 1; bytes chunk = 2; }`.
- **Verified Targets:**
  - [`client_good.cpp`](./02_config_stream/client_good.cpp) (`PASS`) & [`server_good.cpp`](./02_config_stream/server_good.cpp) (`PASS` against generated [`upload_server_dual.session.json`](./02_config_stream/upload_server_dual.session.json)) — `Status::OK`, `chunks=2, bytes=16`.
  - [`client_bad_oneof.cpp`](./02_config_stream/client_bad_oneof.cpp) (`FAIL` statically: Level 2 `oneof` `chunk` before `init`) — rejected at runtime (`INVALID_ARGUMENT=3`).
  - [`client_bad_post_close.cpp`](./02_config_stream/client_bad_post_close.cpp) (`FAIL` statically: `Write()` in `S2` after `WritesDone()`) — `Write()` returns `false` at runtime.

## 3. Running Static + Runtime Verification

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\datasets\01_initial_case_study\build_and_run.ps1
```

Static-only (requires generated `.pb.h`/`.grpc.pb.h` headers from `protoc`):
```powershell
python .\datasets\01_initial_case_study\verify_static.py
```
