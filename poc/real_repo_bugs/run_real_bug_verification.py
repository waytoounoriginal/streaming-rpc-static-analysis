#!/usr/bin/env python3
"""
run_real_bug_verification.py — Static Analyzer Verification Suite for Real-World Repo Bugs
Part of Bachelor Thesis: "Static Analysis of Streaming RPC Protocols in C++: A Behavioural Types Approach"
Author: Mihai Tira

Runs our actual Clang AST static session-type analyzer (`poc/grpc_session_check.py`) against:
1. Conforming repository upper abstractions (e.g. Arrow Flight `GrpcResultStream`, Google Cloud Storage
   single-loop `write_object_spec` progression, fixed BuildBox 2-phase `StageTree`, conforming Triton client).
2. Real-world repository bugs and upper-abstraction bypasses (Arrow Flight `ListFlights` & `Handshake`
   early-return leaks, BuildBox `~GrpcStagedDirectory` missing 2nd Read + Finish, gRPC `CachingInterceptor`
   missing Finish, Triton use-after-StopStream, Google Cloud Storage missing `clear_write_object_spec`,
   and WeNet ASR client & server protocol violations).
"""

import sys
from pathlib import Path

# Ensure UTF-8 output on Windows consoles
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

POC_DIR = Path(__file__).resolve().parent.parent
REAL_BUGS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(POC_DIR))

from grpc_session_check import ClangSessionChecker


def main():
    print("=" * 95)
    print("   CLANG AST STATIC ANALYZER — REAL-WORLD REPOSITORY BUG & UPPER-ABSTRACTION SUITE")
    print("   Engine: poc/grpc_session_check.py (clang++ -Xclang -ast-dump=json + grpc.session.v1 FSM)")
    print("=" * 95)

    suite = [
        # 1. Apache Arrow Flight
        {
            "repo": "apache/arrow (Upper Abstraction)",
            "spec": REAL_BUGS_DIR / "arrow_list_flights.session.json",
            "cpp": REAL_BUGS_DIR / "arrow_result_stream_good.cpp",
            "expect_pass": True,
            "desc": "GrpcResultStream::Finish() drains & calls stream->Finish() on error"
        },
        {
            "repo": "apache/arrow (Bug #1: ListFlights)",
            "spec": REAL_BUGS_DIR / "arrow_list_flights.session.json",
            "cpp": REAL_BUGS_DIR / "arrow_list_flights_buggy.cpp",
            "expect_pass": False,
            "desc": "Bypasses GrpcResultStream; RETURN_NOT_OK in Read loop skips Finish()"
        },
        {
            "repo": "apache/arrow (Bug #2: Handshake)",
            "spec": REAL_BUGS_DIR / "arrow_handshake.session.json",
            "cpp": REAL_BUGS_DIR / "arrow_handshake_buggy.cpp",
            "expect_pass": False,
            "desc": "AuthenticateInternal skips Finish() on auth error (1074) and !finished_writes (1079)"
        },
        # 2. BuildBox Common (Remote Execution CAS)
        {
            "repo": "BuildGrid/buildbox (Bug: StageTree)",
            "spec": REAL_BUGS_DIR / "buildbox_stagetree.session.json",
            "cpp": REAL_BUGS_DIR / "buildbox_staged_dir_buggy.cpp",
            "expect_pass": False,
            "desc": "~GrpcStagedDirectory() exits in S4_WAIT_CLEANUP_ACK (missing 2nd Read & Finish)"
        },
        {
            "repo": "BuildGrid/buildbox (Fixed StageTree)",
            "spec": REAL_BUGS_DIR / "buildbox_stagetree.session.json",
            "cpp": REAL_BUGS_DIR / "buildbox_staged_dir_fixed.cpp",
            "expect_pass": True,
            "desc": "Reads 2nd StageTreeResponse cleanup ack and calls Finish()"
        },
        # 3. grpc/grpc Official CachingInterceptor
        {
            "repo": "grpc/grpc (Bug: CachingInterceptor)",
            "spec": REAL_BUGS_DIR / "grpc_caching_interceptor.session.json",
            "cpp": REAL_BUGS_DIR / "grpc_caching_interceptor_buggy.cpp",
            "expect_pass": False,
            "desc": "CachingInterceptor::Intercept calls Write/Read/WritesDone without Finish()"
        },
        # 4. Triton Inference Server C++ Client
        {
            "repo": "triton-inference-server/client (Bug)",
            "spec": REAL_BUGS_DIR / "triton_stream_infer.session.json",
            "cpp": REAL_BUGS_DIR / "triton_client_buggy.cpp",
            "expect_pass": False,
            "desc": "AsyncStreamInfer calls Write() after StopStream() in terminal S2_FINISHED"
        },
        {
            "repo": "triton-inference-server/client (Good)",
            "spec": REAL_BUGS_DIR / "triton_stream_infer.session.json",
            "cpp": REAL_BUGS_DIR / "triton_client_good.cpp",
            "expect_pass": True,
            "desc": "Conforming StartStream -> Write/Read -> StopStream (WritesDone + Finish)"
        },
        # 5. Google Cloud C++ Storage
        {
            "repo": "googleapis/google-cloud-cpp (Good)",
            "spec": REAL_BUGS_DIR / "gcloud_storage_write.session.json",
            "cpp": REAL_BUGS_DIR / "gcloud_storage_good.cpp",
            "expect_pass": True,
            "desc": "InsertObjectMedia sets write_object_spec then calls clear_write_object_spec()"
        },
        {
            "repo": "googleapis/google-cloud-cpp (Bug)",
            "spec": REAL_BUGS_DIR / "gcloud_storage_write.session.json",
            "cpp": REAL_BUGS_DIR / "gcloud_storage_buggy.cpp",
            "expect_pass": False,
            "desc": "Caught on iteration 2 (k=2 unrolling): missing clear_write_object_spec()"
        },
        # 6. WeNet ASR (Client + Server Dual)
        {
            "repo": "wenet-e2e/wenet (Client Conforming)",
            "spec": POC_DIR / "wenet.session.json",
            "cpp": POC_DIR / "client_good.cpp",
            "expect_pass": True,
            "desc": "DecodeConfig (case 1) -> AudioData* (case 2) -> WritesDone -> Read* -> Finish"
        },
        {
            "repo": "wenet-e2e/wenet (Client Level 2 Bug)",
            "spec": POC_DIR / "wenet.session.json",
            "cpp": POC_DIR / "client_bad_ordering.cpp",
            "expect_pass": False,
            "desc": "Sends audio_data (case 2) in S0_CONFIG before DecodeConfig (case 1)"
        },
        {
            "repo": "wenet-e2e/wenet (Client Level 1 Bug)",
            "spec": POC_DIR / "wenet.session.json",
            "cpp": POC_DIR / "client_bad_unfinalized.cpp",
            "expect_pass": False,
            "desc": "Early return in S1_STREAMING destroys stream without WritesDone/Finish"
        },
        {
            "repo": "wenet-e2e/wenet (Server Empty-Stream)",
            "spec": REAL_BUGS_DIR / "wenet_server_dual.session.json",
            "cpp": REAL_BUGS_DIR / "wenet_server_buggy.cpp",
            "expect_pass": False,
            "desc": "Server OnFinish() writes final Response in S0_WAIT_CONFIG on 0-msg stream"
        },
    ]

    summary_rows = []
    all_matched = True

    for item in suite:
        checker = ClangSessionChecker(item["spec"])
        passed = checker.analyze_file(item["cpp"])
        matched = (passed == item["expect_pass"])
        if not matched:
            all_matched = False
        summary_rows.append((item, passed, matched, len(checker.diagnostics)))

    print("\n" + "=" * 95)
    print(f"{'Repository & Case':<38} | {'File':<34} | {'Analyzer Verdict':<18}")
    print("-" * 95)
    for item, passed, matched, num_diags in summary_rows:
        if passed:
            verdict = "\033[1;32mPASS (Conforming)\033[0m"
        else:
            verdict = f"\033[1;31mCAUGHT ({num_diags} err)\033[0m"
        print(f"{item['repo']:<38} | {item['cpp'].name:<34} | {verdict}")
    print("=" * 95)

    if all_matched:
        print(f"\n\033[1;32m[+] ALL {len(suite)}/14 REAL-WORLD REPOSITORY CASES VERIFIED ACCURATELY BY CLANG AST ANALYZER!\033[0m\n")
        sys.exit(0)
    else:
        print("\n\033[1;31m[-] MISMATCH IN ONE OR MORE ANALYZER VERDICTS\033[0m\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
