#!/usr/bin/env python3
"""
poc/run_on_real_repos.py

Direct static verification of UNMODIFIED C++ source files from 6 real-world open-source
gRPC repositories using Clang's JSON AST (`clang++ -Xclang -ast-dump=json`) and
`ClangSessionChecker` (`poc/grpc_session_check.py`).

Repositories Verified:
  1. grpc/grpc (examples/cpp/interceptors/caching_interceptor.h)
  2. apache/arrow (cpp/src/arrow/flight/transport/grpc/grpc_client.cc)
  3. BuildGrid/buildbox-common (common/buildboxcommon_grpccasclient.cpp)
  4. wenet-e2e/wenet (runtime/core/grpc/grpc_client.cc & grpc_server.cc)
  5. triton-inference-server/client (src/c++/library/grpc_client.cc)
  6. googleapis/google-cloud-cpp (google/cloud/storage/internal/grpc/stub.cc)
"""

import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional

sys.path.insert(0, str(Path(__file__).parent))
from grpc_session_check import ClangSessionChecker

# Directory containing cloned repositories and pre-dumped Clang AST JSONs.
# Can be configured via the REPOS_AST_DIR environment variable or default cache.
_DEFAULT_CANDIDATES = [
    Path(os.environ.get("REPOS_AST_DIR", "")) if os.environ.get("REPOS_AST_DIR") else None,
    Path(__file__).resolve().parent / "cache",
    Path.home() / ".cache" / "grpc_analysis",
    Path.home() / ".gemini" / "antigravity" / "brain" / "7c7305bd-445c-4aa4-be0d-e62387bc6887" / "scratch",
]
SCRATCH_DIR = next((p for p in _DEFAULT_CANDIDATES if p and p.exists()), Path(__file__).resolve().parent / "cache")
REPOS_DIR = SCRATCH_DIR / "repos"
POC_DIR = Path(__file__).resolve().parent
BUGS_DIR = POC_DIR / "real_repo_bugs"


def load_ast_json(ast_path: Path) -> dict:
    raw = ast_path.read_bytes()
    if raw[:2] in (b"\xff\xfe", b"\xfe\xff"):
        text = raw.decode("utf-16")
    else:
        text = raw.decode("utf-8", errors="replace")
    decoder = json.JSONDecoder()
    idx = 0
    n = len(text)
    objs = []
    while idx < n:
        # Find next '{'
        next_brace = text.find("{", idx)
        if next_brace == -1:
            break
        try:
            obj, end_idx = decoder.raw_decode(text, next_brace)
            objs.append(obj)
            idx = end_idx
        except json.JSONDecodeError:
            idx = next_brace + 1
    if len(objs) == 1:
        return objs[0]
    return {"kind": "TranslationUnitDecl", "inner": objs}


def find_methods_in_ast(ast_root: dict, target_names: List[str],
                        require_body: bool = True) -> Dict[str, dict]:
    """Finds CXXMethodDecl / CXXDestructorDecl / FunctionDecl nodes with bodies by name."""
    found: Dict[str, dict] = {}

    def walk(node: dict):
        if not isinstance(node, dict):
            return
        kind = node.get("kind", "")
        name = node.get("name", "")
        if kind in ("CXXMethodDecl", "CXXDestructorDecl", "FunctionDecl") and name in target_names:
            has_body = any(
                isinstance(ch, dict) and ch.get("kind") == "CompoundStmt"
                for ch in node.get("inner", [])
            )
            if not require_body or has_body:
                # Keep the first body-bearing match (e.g. GrpcCASClient::upload(string) at line 433)
                if name not in found:
                    found[name] = node
        for ch in node.get("inner", []):
            walk(ch)

    walk(ast_root)
    return found


def main():
    print("=" * 95)
    print("DIRECT CLANG AST STATIC VERIFICATION ON UNMODIFIED REAL-WORLD C++ REPOSITORIES")
    print("=" * 95)

    results = []

    # =========================================================================
    # 1. grpc/grpc — examples/cpp/interceptors/caching_interceptor.h
    # =========================================================================
    grpc_cpp = REPOS_DIR / "curated/grpc/examples/cpp/interceptors/caching_interceptor.h"
    grpc_ast = load_ast_json(SCRATCH_DIR / "build_grpc_interceptors/caching_interceptor_ast.json")
    grpc_methods = find_methods_in_ast(grpc_ast, ["Intercept"])
    checker_grpc = ClangSessionChecker(BUGS_DIR / "grpc_caching_interceptor.session.json")
    print(f"\n[1/10] Repo: grpc/grpc | File: {grpc_cpp.relative_to(REPOS_DIR)}")
    print(f"       Method: CachingInterceptor::Intercept (line {grpc_methods['Intercept']['loc']['line']})")
    passed_1 = checker_grpc.analyze_real_methods(grpc_cpp, [grpc_methods["Intercept"]])
    results.append((
        "grpc/grpc",
        "examples/cpp/interceptors/caching_interceptor.h:39",
        "CachingInterceptor::Intercept",
        False,  # Expected violation
        passed_1,
        checker_grpc.diagnostics
    ))

    # =========================================================================
    # 2. apache/arrow — GrpcResultStream RAII Wrapper (grpc_client.cc:512-549)
    # =========================================================================
    arrow_cpp = REPOS_DIR / "curated/arrow/cpp/src/arrow/flight/transport/grpc/grpc_client.cc"
    arrow_rs_ast = load_ast_json(SCRATCH_DIR / "build_arrow/result_stream_ast.json")
    arrow_rs_methods = find_methods_in_ast(arrow_rs_ast, ["Next", "~GrpcResultStream"])
    checker_arrow_rs = ClangSessionChecker(BUGS_DIR / "arrow_list_flights.session.json")
    print(f"\n[2/10] Repo: apache/arrow | File: {arrow_cpp.relative_to(REPOS_DIR)}")
    print(f"       Methods: GrpcResultStream::Next (line {arrow_rs_methods['Next']['loc']['line']}) -> ~GrpcResultStream (line {arrow_rs_methods['~GrpcResultStream']['loc']['line']})")
    passed_2 = checker_arrow_rs.analyze_real_methods(
        arrow_cpp,
        [arrow_rs_methods["Next"], arrow_rs_methods["~GrpcResultStream"]],
        initial_stream_state="S0_READING"
    )
    results.append((
        "apache/arrow",
        "cpp/src/arrow/flight/transport/grpc/grpc_client.cc:512,540",
        "GrpcResultStream::Next + ~GrpcResultStream (RAII)",
        True,   # Expected conforming
        passed_2,
        checker_arrow_rs.diagnostics
    ))

    # =========================================================================
    # 3. apache/arrow — GrpcClientImpl::ListFlights (grpc_client.cc:892)
    # =========================================================================
    arrow_lf_ast = load_ast_json(SCRATCH_DIR / "build_arrow/list_flights_ast.json")
    arrow_lf_methods = find_methods_in_ast(arrow_lf_ast, ["ListFlights"])
    checker_arrow_lf = ClangSessionChecker(BUGS_DIR / "arrow_list_flights.session.json")
    print(f"\n[3/10] Repo: apache/arrow | File: {arrow_cpp.relative_to(REPOS_DIR)}")
    print(f"       Method: GrpcClientImpl::ListFlights (line {arrow_lf_methods['ListFlights']['loc']['line']})")
    passed_3 = checker_arrow_lf.analyze_real_methods(arrow_cpp, [arrow_lf_methods["ListFlights"]])
    results.append((
        "apache/arrow",
        "cpp/src/arrow/flight/transport/grpc/grpc_client.cc:892",
        "GrpcClientImpl::ListFlights",
        False,  # Expected violation (5 RETURN_NOT_OK early returns without Finish())
        passed_3,
        checker_arrow_lf.diagnostics
    ))

    # =========================================================================
    # 4. apache/arrow — GrpcClientImpl::AuthenticateInternal (grpc_client.cc:1067)
    # =========================================================================
    arrow_auth_ast = load_ast_json(SCRATCH_DIR / "build_arrow/authenticate_ast.json")
    arrow_auth_methods = find_methods_in_ast(arrow_auth_ast, ["AuthenticateInternal"])
    checker_arrow_auth = ClangSessionChecker(BUGS_DIR / "arrow_handshake.session.json")
    print(f"\n[4/10] Repo: apache/arrow | File: {arrow_cpp.relative_to(REPOS_DIR)}")
    print(f"       Method: GrpcClientImpl::AuthenticateInternal (line {arrow_auth_methods['AuthenticateInternal']['loc']['line']})")
    passed_4 = checker_arrow_auth.analyze_real_methods(
        arrow_cpp, [arrow_auth_methods["AuthenticateInternal"]]
    )
    results.append((
        "apache/arrow",
        "cpp/src/arrow/flight/transport/grpc/grpc_client.cc:1067",
        "GrpcClientImpl::AuthenticateInternal",
        False,  # Expected violation (RETURN_NOT_OK on line 1074 & early return on line 1079 skip Finish())
        passed_4,
        checker_arrow_auth.diagnostics
    ))

    # =========================================================================
    # 5. BuildGrid/buildbox-common — GrpcCASClient::stage + ~GrpcStagedDirectory
    # =========================================================================
    bb_cpp = REPOS_DIR / "new_candidates/buildbox-common/common/buildboxcommon_grpccasclient.cpp"
    bb_stage_ast = load_ast_json(SCRATCH_DIR / "build_buildbox/stage_ast.json")
    bb_dtor_ast = load_ast_json(SCRATCH_DIR / "build_buildbox/staged_dir_ast.json")
    bb_stage_m = find_methods_in_ast(bb_stage_ast, ["stage"])["stage"]
    bb_dtor_m = find_methods_in_ast(bb_dtor_ast, ["~GrpcStagedDirectory"])["~GrpcStagedDirectory"]
    checker_bb = ClangSessionChecker(BUGS_DIR / "buildbox_stagetree.session.json")
    print(f"\n[5/10] Repo: BuildGrid/buildbox-common | File: {bb_cpp.relative_to(REPOS_DIR)}")
    print(f"       Methods: GrpcCASClient::stage (line {bb_stage_m['loc']['line']}) -> ~GrpcStagedDirectory (line {bb_dtor_m['loc']['line']})")
    passed_5 = checker_bb.analyze_real_methods(bb_cpp, [bb_stage_m, bb_dtor_m])
    results.append((
        "BuildGrid/buildbox-common",
        "common/buildboxcommon_grpccasclient.cpp:658,1317",
        "GrpcCASClient::stage + ~GrpcStagedDirectory",
        False,  # Expected violation (missing 2nd Read() and Finish() in ~GrpcStagedDirectory)
        passed_5,
        checker_bb.diagnostics
    ))

    # =========================================================================
    # 6. wenet-e2e/wenet — GrpcClient (runtime/core/grpc/grpc_client.cc)
    # =========================================================================
    wenet_cli_cpp = REPOS_DIR / "new_candidates/wenet/runtime/core/grpc/grpc_client.cc"
    wenet_cli_ast = load_ast_json(SCRATCH_DIR / "build_wenet/wenet_client_ast.json")
    wenet_cli_m = find_methods_in_ast(wenet_cli_ast, ["Connect", "SendBinaryData", "Join"])
    checker_wenet_cli = ClangSessionChecker(POC_DIR / "wenet.session.json")
    print(f"\n[6/10] Repo: wenet-e2e/wenet | File: {wenet_cli_cpp.relative_to(REPOS_DIR)}")
    print(f"       Methods: GrpcClient::Connect (line {wenet_cli_m['Connect']['loc']['line']}) -> SendBinaryData (line {wenet_cli_m['SendBinaryData']['loc']['line']}) -> Join (line {wenet_cli_m['Join']['loc']['line']})")
    passed_6 = checker_wenet_cli.analyze_real_methods(
        wenet_cli_cpp,
        [wenet_cli_m["Connect"], wenet_cli_m["SendBinaryData"], wenet_cli_m["Join"]]
    )
    results.append((
        "wenet-e2e/wenet",
        "runtime/core/grpc/grpc_client.cc:37,51,77",
        "GrpcClient::Connect -> SendBinaryData -> Join",
        True,   # Expected conforming client
        passed_6,
        checker_wenet_cli.diagnostics
    ))

    # =========================================================================
    # 7. wenet-e2e/wenet — GrpcConnectionHandler::operator() (grpc_server.cc:143)
    # =========================================================================
    wenet_srv_cpp = REPOS_DIR / "new_candidates/wenet/runtime/core/grpc/grpc_server.cc"
    wenet_srv_ast = load_ast_json(SCRATCH_DIR / "build_wenet/wenet_server_ast.json")
    wenet_srv_m = find_methods_in_ast(wenet_srv_ast, ["operator()"])
    checker_wenet_srv = ClangSessionChecker(BUGS_DIR / "wenet_server_dual.session.json")
    print(f"\n[7/10] Repo: wenet-e2e/wenet | File: {wenet_srv_cpp.relative_to(REPOS_DIR)}")
    print(f"       Method: GrpcConnectionHandler::operator() (line {wenet_srv_m['operator()']['loc']['line']})")
    passed_7 = checker_wenet_srv.analyze_real_methods(
        wenet_srv_cpp, [wenet_srv_m["operator()"]]
    )
    results.append((
        "wenet-e2e/wenet",
        "runtime/core/grpc/grpc_server.cc:143",
        "GrpcConnectionHandler::operator() (Server Dual)",
        False,  # Expected violation (unconditional OnSpeechEnd() on 0-iteration Read() loop)
        passed_7,
        checker_wenet_srv.diagnostics
    ))

    # =========================================================================
    # 8. triton-inference-server/client — Normal Stream Lifecycle (grpc_client.cc)
    # =========================================================================
    triton_cpp = REPOS_DIR / "curated/triton-client/src/c++/library/grpc_client.cc"
    triton_ast = load_ast_json(SCRATCH_DIR / "build_triton/triton_client_ast.json")
    triton_m = find_methods_in_ast(triton_ast, ["StartStream", "AsyncStreamInfer", "StopStream", "AsyncStreamTransfer"])
    checker_triton_ok = ClangSessionChecker(BUGS_DIR / "triton_stream_infer.session.json")
    print(f"\n[8/10] Repo: triton-inference-server/client | File: {triton_cpp.relative_to(REPOS_DIR)}")
    print(f"       Methods: StartStream (line {triton_m['StartStream']['loc']['line']}) -> AsyncStreamInfer (line {triton_m['AsyncStreamInfer']['loc']['line']}) -> StopStream (line {triton_m['StopStream']['loc']['line']}) -> AsyncStreamTransfer (line {triton_m['AsyncStreamTransfer']['loc']['line']})")
    passed_8 = checker_triton_ok.analyze_real_methods(
        triton_cpp,
        [triton_m["StartStream"], triton_m["AsyncStreamInfer"], triton_m["StopStream"], triton_m["AsyncStreamTransfer"]]
    )
    results.append((
        "triton-inference-server/client",
        "src/c++/library/grpc_client.cc:1356,1423,1402,1681",
        "StartStream -> AsyncStreamInfer -> StopStream -> AsyncStreamTransfer",
        True,   # Expected conforming
        passed_8,
        checker_triton_ok.diagnostics
    ))

    # =========================================================================
    # 9. triton-inference-server/client — Post-StopStream Reuse (grpc_client.cc)
    # =========================================================================
    checker_triton_bug = ClangSessionChecker(BUGS_DIR / "triton_stream_infer.session.json")
    print(f"\n[9/10] Repo: triton-inference-server/client | File: {triton_cpp.relative_to(REPOS_DIR)}")
    print(f"       Methods: StartStream (1356) -> StopStream (1402) -> AsyncStreamTransfer (1681) -> AsyncStreamInfer (1423) [Post-Finish Reuse]")
    passed_9 = checker_triton_bug.analyze_real_methods(
        triton_cpp,
        [triton_m["StartStream"], triton_m["StopStream"], triton_m["AsyncStreamTransfer"], triton_m["AsyncStreamInfer"]]
    )
    results.append((
        "triton-inference-server/client",
        "src/c++/library/grpc_client.cc:1356,1402,1495",
        "StartStream -> StopStream -> AsyncStreamInfer",
        False,  # Expected violation (Write on closed stream because StopStream omits grpc_stream_.reset())
        passed_9,
        checker_triton_bug.diagnostics
    ))

    # =========================================================================
    # 10. googleapis/google-cloud-cpp — GrpcStub::InsertObjectMedia (stub.cc:325)
    # =========================================================================
    gcloud_cpp = REPOS_DIR / "curated/google-cloud-cpp/google/cloud/storage/internal/grpc/stub.cc"
    gcloud_ast = load_ast_json(SCRATCH_DIR / "build_gcloud/insert_object_media_ast.json")
    gcloud_m = find_methods_in_ast(gcloud_ast, ["InsertObjectMedia"])
    checker_gcloud = ClangSessionChecker(BUGS_DIR / "gcloud_storage_write.session.json")
    print(f"\n[10/16] Repo: googleapis/google-cloud-cpp | File: {gcloud_cpp.relative_to(REPOS_DIR)}")
    print(f"        Method: GrpcStub::InsertObjectMedia (line {gcloud_m['InsertObjectMedia']['loc']['line']})")
    passed_10 = checker_gcloud.analyze_real_methods(
        gcloud_cpp, [gcloud_m["InsertObjectMedia"]]
    )
    results.append((
        "googleapis/google-cloud-cpp",
        "google/cloud/storage/internal/grpc/stub.cc:325",
        "GrpcStub::InsertObjectMedia (k=2 do-while loop)",
        True,   # Expected conforming (clear_write_object_spec() on line 364)
        passed_10,
        checker_gcloud.diagnostics
    ))

    # =========================================================================
    # 11. googleapis/google-cloud-cpp — GrpcStub::UploadChunk (stub.cc:620)
    # =========================================================================
    gcloud_uc_ast = load_ast_json(SCRATCH_DIR / "build_gcloud/upload_chunk_ast.json")
    gcloud_uc_m = find_methods_in_ast(gcloud_uc_ast, ["UploadChunk"])
    checker_gcloud_uc = ClangSessionChecker(BUGS_DIR / "gcloud_storage_write.session.json")
    print(f"\n[11/16] Repo: googleapis/google-cloud-cpp | File: {gcloud_cpp.relative_to(REPOS_DIR)}")
    print(f"        Method: GrpcStub::UploadChunk (line {gcloud_uc_m['UploadChunk']['loc']['line']})")
    passed_11 = checker_gcloud_uc.analyze_real_methods(
        gcloud_cpp, [gcloud_uc_m["UploadChunk"]]
    )
    results.append((
        "googleapis/google-cloud-cpp",
        "google/cloud/storage/internal/grpc/stub.cc:620",
        "GrpcStub::UploadChunk (k=2 resumable upload)",
        True,   # Expected conforming (clear_upload_id() on line 683)
        passed_11,
        checker_gcloud_uc.diagnostics
    ))

    # =========================================================================
    # 12. apache/arrow — GrpcClientImpl::ListActions (grpc_client.cc:923)
    #     [Universal Level-1 SERVER_STREAMING Automaton — Zero Custom .session.json]
    # =========================================================================
    arrow_la_ast = load_ast_json(SCRATCH_DIR / "build_arrow/list_actions_ast.json")
    arrow_la_m = find_methods_in_ast(arrow_la_ast, ["ListActions"])
    checker_arrow_la = ClangSessionChecker.from_default_level1("SERVER_STREAMING", "ListActions")
    print(f"\n[12/16] Repo: apache/arrow | File: {arrow_cpp.relative_to(REPOS_DIR)}")
    print(f"        Method: GrpcClientImpl::ListActions (line {arrow_la_m['ListActions']['loc']['line']}) [Default Level-1 FSM]")
    passed_12 = checker_arrow_la.analyze_real_methods(arrow_cpp, [arrow_la_m["ListActions"]])
    results.append((
        "apache/arrow",
        "cpp/src/arrow/flight/transport/grpc/grpc_client.cc:923",
        "GrpcClientImpl::ListActions [Default Level-1]",
        False,  # Expected violation (newly discovered: RETURN_NOT_OK on lines 935 & 939 skip Finish())
        passed_12,
        checker_arrow_la.diagnostics
    ))

    # =========================================================================
    # 13. apache/arrow — FinishableDataStream::DoFinish (grpc_client.cc:271)
    #     [Universal Level-1 SERVER_STREAMING Automaton — Zero Custom .session.json]
    # =========================================================================
    arrow_ds_ast = load_ast_json(SCRATCH_DIR / "build_arrow/data_stream_ast.json")
    arrow_ds_m = find_methods_in_ast(arrow_ds_ast, ["DoFinish"])
    checker_arrow_ds = ClangSessionChecker.from_default_level1("SERVER_STREAMING", "DoGet")
    print(f"\n[13/16] Repo: apache/arrow | File: {arrow_cpp.relative_to(REPOS_DIR)}")
    print(f"        Method: FinishableDataStream::DoFinish (line {arrow_ds_m['DoFinish']['loc']['line']}) [Default Level-1 FSM]")
    passed_13 = checker_arrow_ds.analyze_real_methods(
        arrow_cpp, [arrow_ds_m["DoFinish"]], initial_stream_state="S0_READING"
    )
    results.append((
        "apache/arrow",
        "cpp/src/arrow/flight/transport/grpc/grpc_client.cc:271",
        "FinishableDataStream::DoFinish [Default Level-1]",
        True,   # Expected conforming (drains stream and calls stream_->Finish() on line 287)
        passed_13,
        checker_arrow_ds.diagnostics
    ))

    # =========================================================================
    # 14. BuildGrid/buildbox-common — GrpcCASClient::upload(string) (line 433)
    #     [Universal Level-1 CLIENT_STREAMING Automaton — Zero Custom .session.json]
    # =========================================================================
    bb_up_ast = load_ast_json(SCRATCH_DIR / "build_buildbox/upload_ast.json")
    bb_up_m = find_methods_in_ast(bb_up_ast, ["upload", "uploadFdRegion"])
    checker_bb_up = ClangSessionChecker.from_default_level1("CLIENT_STREAMING", "Write")
    print(f"\n[14/16] Repo: BuildGrid/buildbox-common | File: {bb_cpp.relative_to(REPOS_DIR)}")
    print(f"        Method: GrpcCASClient::upload(string) (line {bb_up_m['upload']['loc']['line']}) [Default Level-1 FSM]")
    passed_14 = checker_bb_up.analyze_real_methods(bb_cpp, [bb_up_m["upload"]])
    results.append((
        "BuildGrid/buildbox-common",
        "common/buildboxcommon_grpccasclient.cpp:433",
        "GrpcCASClient::upload(string) [Default Level-1]",
        True,   # Expected conforming (calls WritesDone + Finish before any post-upload throw)
        passed_14,
        checker_bb_up.diagnostics
    ))

    # =========================================================================
    # 15. BuildGrid/buildbox-common — GrpcCASClient::uploadFdRegion (line 506)
    #     [Universal Level-1 CLIENT_STREAMING Automaton — Zero Custom .session.json]
    # =========================================================================
    checker_bb_upfd = ClangSessionChecker.from_default_level1("CLIENT_STREAMING", "Write")
    print(f"\n[15/16] Repo: BuildGrid/buildbox-common | File: {bb_cpp.relative_to(REPOS_DIR)}")
    print(f"        Method: GrpcCASClient::uploadFdRegion (line {bb_up_m['uploadFdRegion']['loc']['line']}) [Default Level-1 FSM]")
    passed_15 = checker_bb_upfd.analyze_real_methods(bb_cpp, [bb_up_m["uploadFdRegion"]])
    results.append((
        "BuildGrid/buildbox-common",
        "common/buildboxcommon_grpccasclient.cpp:506",
        "GrpcCASClient::uploadFdRegion [Default Level-1]",
        False,  # Expected violation (throws mid-stream at lines 534 & 546 before WritesDone/Finish)
        passed_15,
        checker_bb_upfd.diagnostics
    ))

    # =========================================================================
    # 16. BuildGrid/buildbox-common — GrpcCASClient::download (line 358)
    #     [Universal Level-1 SERVER_STREAMING Automaton — Zero Custom .session.json]
    # =========================================================================
    bb_dl_ast = load_ast_json(SCRATCH_DIR / "build_buildbox/download_ast.json")
    bb_dl_m = find_methods_in_ast(bb_dl_ast, ["download"])
    checker_bb_dl = ClangSessionChecker.from_default_level1("SERVER_STREAMING", "Read")
    print(f"\n[16/16] Repo: BuildGrid/buildbox-common | File: {bb_cpp.relative_to(REPOS_DIR)}")
    print(f"        Method: GrpcCASClient::download (line {bb_dl_m['download']['loc']['line']}) [Default Level-1 FSM]")
    passed_16 = checker_bb_dl.analyze_real_methods(bb_cpp, [bb_dl_m["download"]])
    results.append((
        "BuildGrid/buildbox-common",
        "common/buildboxcommon_grpccasclient.cpp:358",
        "GrpcCASClient::download [Default Level-1]",
        False,  # Expected violation (throws mid-stream at line 381 inside Read() loop before Finish)
        passed_16,
        checker_bb_dl.diagnostics
    ))

    # =========================================================================
    # Summary Table
    # =========================================================================
    print("\n" + "=" * 105)
    print(f"{'REPO':28} | {'REAL FILE & LINE':45} | {'EXPECTED':10} | {'ACTUAL':10} | {'MATCH'}")
    print("-" * 105)
    all_matched = True
    for repo, file_loc, desc, expected, actual, diags in results:
        exp_str = "CONFORMING" if expected else "VIOLATION"
        act_str = "CONFORMING" if actual else f"VIOLATION ({len(diags)})"
        matched = (expected == actual)
        if not matched:
            all_matched = False
        mark = "OK" if matched else "MISMATCH"
        print(f"{repo:28} | {file_loc:45} | {exp_str:10} | {act_str:14} | {mark}")
    print("=" * 105)

    if not all_matched:
        sys.exit(1)
    print("\n[+] ALL 16 REAL-REPOSITORY TARGETS ACROSS ALL 6 OPEN-SOURCE REPOS VERIFIED EXACTLY AS EXPECTED!")


if __name__ == "__main__":
    main()
