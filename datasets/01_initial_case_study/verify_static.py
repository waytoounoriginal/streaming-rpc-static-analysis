#!/usr/bin/env python3
"""
verify_static.py — End-to-End Static Verification for Minimal Client + Dual Server Examples

1. Compiles custom (session_type) MethodOptions from .proto -> dual client & server .session.json FSMs.
2. Runs ClangSessionChecker (clang++ -Xclang -ast-dump=json) across all minimal client & server handlers.
3. Asserts 100% agreement between expected protocol conformance/violations and static diagnostics.
"""

import sys
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

MIN_DIR = Path(__file__).resolve().parent
ROOT_DIR = MIN_DIR.parent.parent
POC_DIR = ROOT_DIR / "poc"
GEN_DIR = MIN_DIR / "generated"
sys.path.insert(0, str(POC_DIR))

from session_compiler import parse_session_type, extract_session_specs_from_proto, compile_protocol_fsm
from grpc_session_check import ClangSessionChecker
import json


def compile_proto_both(proto_path: Path, out_dir: Path):
    specs = extract_session_specs_from_proto(proto_path)
    if not specs:
        raise RuntimeError(f"No session_type option found in {proto_path}")
    for spec in specs:
        ast = parse_session_type(spec["session_spec"])
        for role, suffix in (("client", "client"), ("server", "server_dual")):
            contract = compile_protocol_fsm(
                session_ast=ast,
                package=spec["package"],
                service=spec["service"],
                method=spec["method"],
                rpc_type=spec["rpc_type"],
                client_stream_type=spec["client_stream_type"],
                server_stream_type=spec["server_stream_type"],
                role=role,
                payload_mappings=spec.get("payload_mappings"),
                payload_field_name=spec.get("payload_field_name", "payload"),
                payload_groups=spec.get("payload_groups"),
            )
            out_file = out_dir / f"{spec['method'].lower()}_{suffix}.session.json"
            out_file.write_text(json.dumps(contract, indent=2), encoding="utf-8")


def main() -> int:
    print("=" * 84)
    print("  MINIMAL END-TO-END EXAMPLES — COMPILER & CLANG AST STATIC VERIFICATION")
    print("=" * 84)

    ex1 = MIN_DIR / "01_pingpong_handshake"
    ex2 = MIN_DIR / "02_config_stream"

    compile_proto_both(ex1 / "handshake.proto", ex1)
    compile_proto_both(ex2 / "config_stream.proto", ex2)

    extra_flags = ["-std=c++17", "-I", str(GEN_DIR)]

    cases = [
        # Example 1: Level 1 + Client/Server Duality
        ("Ex1 Client Good",          ex1 / "exchange_client.session.json",      ex1 / "client_good.cpp",               True),
        ("Ex1 Client Bad Swap",      ex1 / "exchange_client.session.json",      ex1 / "client_bad_swap.cpp",           False),
        ("Ex1 Client Bad Leak",      ex1 / "exchange_client.session.json",      ex1 / "client_bad_leak.cpp",           False),
        ("Ex1 Server Dual Good",     ex1 / "exchange_server_dual.session.json", ex1 / "server_good.cpp",               True),
        ("Ex1 Server Bad Order",     ex1 / "exchange_server_dual.session.json", ex1 / "server_bad_order.cpp",          False),
        ("Ex1 Server Missing Summ.", ex1 / "exchange_server_dual.session.json", ex1 / "server_bad_missing_summary.cpp", False),
        # Example 2: Level 2 oneof Progression + Post-Close Check
        ("Ex2 Client Good (oneof)",  ex2 / "upload_client.session.json",        ex2 / "client_good.cpp",               True),
        ("Ex2 Client Bad oneof",     ex2 / "upload_client.session.json",        ex2 / "client_bad_oneof.cpp",          False),
        ("Ex2 Client Bad PostClose", ex2 / "upload_client.session.json",        ex2 / "client_bad_post_close.cpp",     False),
        ("Ex2 Server Dual Good",     ex2 / "upload_server_dual.session.json",   ex2 / "server_good.cpp",               True),
    ]

    all_ok = True
    rows = []
    for label, spec_path, cpp_path, expect_pass in cases:
        checker = ClangSessionChecker(spec_path=spec_path)
        passed = checker.analyze_file(cpp_path, extra_flags=extra_flags)
        matched = (passed == expect_pass)
        if not matched:
            all_ok = False
        rows.append((label, "PASS" if expect_pass else "FAIL", "PASS" if passed else f"FAIL ({len(checker.diagnostics)})", matched))

    print("\n" + "=" * 84)
    print(f"{'Test Case':<28} | {'Expected':<10} | {'Checker':<14} | {'Match'}")
    print("-" * 84)
    for label, exp, got, matched in rows:
        print(f"{label:<28} | {exp:<10} | {got:<14} | {'OK' if matched else 'MISMATCH'}")
    print("=" * 84)
    print(f"Summary: {sum(1 for r in rows if r[3])}/{len(rows)} cases matched expectations.")
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
