# Bachelor Thesis Proof of Concept (POC)
## Static Analysis of Streaming RPC Protocols in C++: A Behavioural Types Approach
**Author:** Mihai Tira  
**Domain:** Compile-time Protocol Conformance Verification for Synchronous C++ gRPC Streaming

---

## 1. Overview & Service Specification

This POC implements end-to-end static session type verification against a **proper, production-grade gRPC service**: the **WeNet ASR Streaming RPC** (`wenet.ASR.Recognize`), an open-source speech recognition system used in production.

Unlike initial toy prototypes, this implementation uses:
1. **Official Protobuf IDL:** [`poc/wenet.proto`](./wenet.proto) defining bidirectional streaming RPC:
   ```protobuf
   syntax = "proto3";
   package wenet;

   service ASR {
     rpc Recognize (stream Request) returns (stream Response) {
       option (wenet.session_type) = 
         "!Request[decode_config]; rec X. (+{write_chunk: !Request[audio_data]; X, writes_done: ?Response; rec Y. (&{recv_resp: ?Response; Y, recv_eof: end})})";
     }
   }

   message Request {
     message DecodeConfig {
       int32 nbest_config = 1;
       bool continuous_decoding_config = 2;
     }

     oneof RequestPayload {
       DecodeConfig decode_config = 1;
       bytes audio_data = 2;
     }
   }
   ```
2. **Official Code Generation:** Compiled using Google's `protoc` (v31.1) and `grpc_cpp_plugin` (v1.72.0) into authentic C++ headers:
   - `wenet.pb.h` / `wenet.pb.cc` (generated Protobuf message classes and accessors)
   - `wenet.grpc.pb.h` / `wenet.grpc.pb.cc` (generated `wenet::ASR::Stub`, `Recognize()`, and `grpc::ClientReaderWriter<Request, Response>`)
3. **Formal Binary Session Type Specification:** Embedded directly as formal comments in [`poc/wenet.proto`](./wenet.proto) and compiled into [`poc/wenet.session.json`](./wenet.session.json) adhering to the thesis `grpc.session.v1` FSM schema.
4. **Architectural Drawing & State Diagrams:** See the interactive visual diagrams in the generated artifact: `poc_architecture_and_formalism_drawing.md`.

---

## 2. Verified Protocol Invariants

The static checker verifies two tiers of behavioural type properties:

### Level 1: Stream Lifecycle & Call Sequencing
- **State Progression:** `S0_CONFIG` $\xrightarrow{!\text{Config}}$ `S1_STREAMING` $\xrightarrow{!\text{Audio}^*}$ `S1_STREAMING` $\xrightarrow{\text{WritesDone}()}$ `S2_WRITES_DONE` $\xrightarrow{?\text{Response}^*}$ `S2_WRITES_DONE` $\xrightarrow{\text{Finish}()}$ `S3_FINISHED` (`end`).
- **Half-Close Requirement:** No `stream->Write()` calls are permitted after `stream->WritesDone()`.
- **Termination / Leak Prevention:** Every open stream must reach terminal state `S3_FINISHED` before going out of scope or returning. Early function returns that bypass `Finish()` are flagged as lifecycle leaks.
- **Bi-Terminal Session Duality:** Early bailouts on network/stream errors (e.g., `if (!stream->Write(...)) return 1;`) transition to the fault terminal state and are soundly recognized as valid error handling.

### Level 2: Protobuf `oneof` Variant Progression
- **Handshake Invariant:** The WeNet server requires that the first message sent on the stream is a `DecodeConfig` (`case 1`).
- **Violation Catching:** If a client sends an `audio_data` chunk (`case 2`) while still in `S0_CONFIG`, the static checker intercepts the violation at compile time, preventing runtime server pipeline crashes.

---

## 3. Directory Layout

```
poc/
├── session_compiler/          # Binary session type parser and FSM synthesizer
│   ├── ast_nodes.py           # AST definitions and dual session endpoint generator
│   ├── parser.py              # Recursive-descent session type parser
│   ├── fsm_builder.py         # Deterministic FSM builder (grpc.session.v1)
│   ├── proto_extractor.py     # Protobuf service/RPC option extractor
│   └── test_compiler.py       # Unit and regression test suite
├── compile_proto_session.py   # CLI compiler: .proto options -> .session.json
├── wenet.proto                # Production WeNet ASR service definition
├── wenet.session.json         # Compiled binary session type FSM (grpc.session.v1)
├── wenet.pb.h / .pb.cc        # Real generated Protobuf C++ accessors
├── wenet.grpc.pb.h / .pb.cc   # Real generated gRPC synchronous stub and stream interfaces
├── client_good.cpp            # Conforming client adhering to all session type rules
├── client_bad_ordering.cpp    # Mutated client: premature audio chunk in S0 (Level 2 violation)
├── client_bad_unfinalized.cpp # Mutated client: early return leaking unfinalized stream (Level 1 violation)
├── grpc_session_check.py      # Clang AST static analyzer driver
├── run_poc.py                 # Master demonstration runner
├── run_on_real_repos.py       # Static checker runner across real open-source ASTs
├── real_repo_bugs/            # Isolated test fixtures and session contracts from 5 real repos
└── README.md                  # This documentation
```

---

## 4. How to Run

Compile Protobuf session types into FSM contracts:
```powershell
python poc/compile_proto_session.py poc/wenet.proto
```

Execute the master runner directly:
```powershell
python poc/run_poc.py
```

Run unit tests for the session compiler:
```powershell
python poc/session_compiler/test_compiler.py
```

Run the static checker directly on individual files:
```powershell
# Verify conforming client
python poc/grpc_session_check.py

# Or analyze a specific client
python -c "from poc.grpc_session_check import ClangSessionChecker; c = ClangSessionChecker('poc/wenet.session.json'); c.analyze_file('poc/client_good.cpp')"
```

Run static analysis across unmodified open-source repositories:
```powershell
python poc/run_on_real_repos.py
```

---

## 5. Verification Results

```
================================================================================
   BACHELOR THESIS PROOF OF CONCEPT (POC)
   Static Analysis of Streaming RPC Protocols in C++: A Behavioural Types Approach
   Author: Mihai Tira
================================================================================
Proper gRPC Service: wenet.ASR.Recognize (from WeNet ASR Production System)
Protocol Definition: poc/wenet.proto (compiled via official protoc + grpc_cpp_plugin)
Session Type FSM:    poc/wenet.session.json (grpc.session.v1 schema)
--------------------------------------------------------------------------------

Target C++ Client            | Protocol Property Tested     | Static Checker
--------------------------------------------------------------------------------
client_good.cpp              | Conforming Session Flow      | PASS (Verified)
client_bad_ordering.cpp      | Level 2 oneof Inversion      | CAUGHT (Violation)
client_bad_unfinalized.cpp   | Level 1 Stream Leak          | CAUGHT (Violation)
================================================================================

>>> POC VERIFICATION COMPLETE: ALL STATIC PROOFS CONFIRMED ON REAL gRPC SERVICE <<<
```
