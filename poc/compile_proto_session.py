#!/usr/bin/env python3
"""
compile_proto_session.py — Protobuf Session Type Compiler CLI
Part of Bachelor Thesis: "Static Analysis of Streaming RPC Protocols in C++: A Behavioural Types Approach"
Author: Mihai Tira

Compiles Protobuf custom options into dual grpc.session.v1 sidecar contracts:
  Usage:
    python poc/compile_proto_session.py poc/wenet.proto --out-dir poc/
    python poc/compile_proto_session.py poc/real_repo_bugs/real_repos.proto --out-dir poc/real_repo_bugs/generated/
"""

import argparse
import json
import sys
from pathlib import Path

# Ensure UTF-8 console output on Windows
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

sys.path.insert(0, str(Path(__file__).parent))
from session_compiler import (
    parse_session_type,
    compile_protocol_fsm,
    extract_session_specs_from_proto,
    dual,
)


def main():
    parser = argparse.ArgumentParser(
        description="Compile Protobuf IDL session_type options into grpc.session.v1 FSM sidecars."
    )
    parser.add_argument("proto_file", type=Path, help="Path to .proto file containing session_type options")
    parser.add_argument("--out-dir", type=Path, default=None, help="Output directory for generated .session.json files")
    parser.add_argument("--dual", action="store_true", default=True, help="Also emit dual server session contract")
    parser.add_argument("--verbose", action="store_true", help="Print generated FSM JSON to console")

    args = parser.parse_args()

    proto_path = args.proto_file.resolve()
    if not proto_path.exists():
        print(f"[-] Error: File not found: {proto_path}", file=sys.stderr)
        sys.exit(1)

    out_dir = args.out_dir.resolve() if args.out_dir else proto_path.parent
    out_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 80)
    print("   PROTOBUF BINARY SESSION TYPE COMPILER (protoc-session-v1)")
    print(f"   Source IDL: {proto_path.name}")
    print(f"   Output Directory: {out_dir}")
    print("=" * 80)

    specs = extract_session_specs_from_proto(proto_path)
    if not specs:
        print(f"[-] No RPC methods with (session_type) options found in {proto_path.name}")
        sys.exit(0)

    compiled_count = 0

    for spec in specs:
        method = spec["method"]
        session_str = spec["session_spec"]
        print(f"\n[*] Compiling RPC: {spec['service']}.{method} ({spec['rpc_type']})")
        print(f"    Session String: \"{session_str}\"")

        # 1. Parse into AST
        try:
            ast = parse_session_type(session_str)
            print(f"    [+] AST: {ast.to_string()}")
        except Exception as e:
            print(f"    [-] Parse Error: {e}", file=sys.stderr)
            continue

        # 2. Compile Client Contract
        client_contract = compile_protocol_fsm(
            session_ast=ast,
            package=spec["package"],
            service=spec["service"],
            method=method,
            rpc_type=spec["rpc_type"],
            client_stream_type=spec["client_stream_type"],
            server_stream_type=spec["server_stream_type"],
            role="client"
        )
        client_file = out_dir / f"{method.lower()}_client.session.json"
        client_file.write_text(json.dumps(client_contract, indent=2), encoding="utf-8")
        print(f"    [+] Emitted Client Contract: {client_file.name} ({len(client_contract['states'])} states)")
        compiled_count += 1

        # 3. Compile Dual Server Contract
        if args.dual:
            server_contract = compile_protocol_fsm(
                session_ast=ast,
                package=spec["package"],
                service=spec["service"],
                method=method,
                rpc_type=spec["rpc_type"],
                client_stream_type=spec["client_stream_type"],
                server_stream_type=spec["server_stream_type"],
                role="server"
            )
            server_file = out_dir / f"{method.lower()}_server_dual.session.json"
            server_file.write_text(json.dumps(server_contract, indent=2), encoding="utf-8")
            print(f"    [+] Emitted Server Dual Contract: {server_file.name} ({len(server_contract['states'])} states)")
            compiled_count += 1

        if args.verbose:
            print("\n" + json.dumps(client_contract, indent=2))

    print("\n" + "=" * 80)
    print(f"[+] Successfully compiled {compiled_count} session contract(s) from {proto_path.name}")
    print("=" * 80)


if __name__ == "__main__":
    main()
