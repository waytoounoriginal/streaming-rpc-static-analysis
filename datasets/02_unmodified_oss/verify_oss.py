#!/usr/bin/env python3
"""
verify_oss.py — Declarative Static Checker Runner for Dataset 2 (Unmodified OSS Methods).

Reads datasets/02_unmodified_oss/manifest.json and evaluates ClangSessionChecker against
cached Clang AST JSON dumps across 16 real methods from 6 pinned open-source C++ gRPC repos.
"""

import json
import os
from pathlib import Path
import sys
from typing import Dict, List, Optional

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

DS2_DIR = Path(__file__).resolve().parent
ROOT_DIR = DS2_DIR.parent.parent
POC_DIR = ROOT_DIR / "poc"
sys.path.insert(0, str(POC_DIR))

from grpc_session_check import ClangSessionChecker
from session_compiler import compile_protocol_fsm, extract_session_specs_from_proto, parse_session_type


def compile_oss_contracts():
    """Synthesizes all Dataset 2 contracts directly from .proto MethodOptions."""
    contracts_out = DS2_DIR / "contracts"
    contracts_out.mkdir(parents=True, exist_ok=True)
    proto_jobs = [
        (POC_DIR / "regression" / "real_repo_bugs" / "real_repos.proto", ("client",)),
        (POC_DIR / "regression" / "wenet_demo" / "wenet.proto", ("client", "server")),
    ]
    for ppath, roles in proto_jobs:
        for spec in extract_session_specs_from_proto(ppath):
            ast = parse_session_type(spec["session_spec"])
            for role in roles:
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
                    method_aliases=spec.get("method_aliases"),
                )
                suffix = "client" if role == "client" else "server_dual"
                out_file = contracts_out / f"{spec['method'].lower()}_{suffix}.session.json"
                out_file.write_text(json.dumps(contract, indent=2), encoding="utf-8")


def resolve_cache_dir() -> Optional[Path]:
    """Resolves the AST/source cache directory from REPOS_AST_DIR or local .cache/."""
    env_dir = os.environ.get("REPOS_AST_DIR", "").strip()
    if env_dir and Path(env_dir).exists():
        return Path(env_dir)
    local_cache = DS2_DIR / ".cache"
    if local_cache.exists():
        return local_cache
    return None


def load_ast_json(ast_path: Path) -> dict:
    """Decodes one or more Clang JSON AST objects emitted by `-Xclang -ast-dump=json`."""
    raw = ast_path.read_bytes()
    text = raw.decode("utf-16") if raw[:2] in (b"\xff\xfe", b"\xfe\xff") else raw.decode("utf-8", errors="replace")
    decoder = json.JSONDecoder()
    idx = 0
    n = len(text)
    objs = []
    while idx < n:
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


def find_methods_in_ast(ast_root: dict, target_names: List[str], require_body: bool = True) -> Dict[str, dict]:
    """Finds CXXMethodDecl / CXXDestructorDecl / FunctionDecl nodes with CompoundStmt bodies."""
    found: Dict[str, dict] = {}

    def walk(node: dict):
        if not isinstance(node, dict):
            return
        kind = node.get("kind", "")
        name = node.get("name", "")
        if kind in ("CXXMethodDecl", "CXXDestructorDecl", "FunctionDecl") and name in target_names:
            has_body = any(isinstance(ch, dict) and ch.get("kind") == "CompoundStmt" for ch in node.get("inner", []))
            if (not require_body or has_body) and name not in found:
                found[name] = node
        for ch in node.get("inner", []):
            walk(ch)

    walk(ast_root)
    return found


def load_manifest() -> dict:
    return json.loads((DS2_DIR / "manifest.json").read_text(encoding="utf-8"))


def build_checker(entry: dict) -> ClangSessionChecker:
    if "contract" in entry:
        return ClangSessionChecker(DS2_DIR / entry["contract"])
    d1 = entry["default_level1"]
    return ClangSessionChecker.from_default_level1(d1["rpc_type"], d1["method_name"])


def resolve_ast_path(cache_dir: Path, filename: str) -> Path:
    direct = cache_dir / "asts" / filename
    if direct.exists():
        return direct
    matches = list(cache_dir.rglob(filename))
    if matches:
        return matches[0]
    raise FileNotFoundError(f"Missing AST dump {filename} under {cache_dir}")


def main() -> int:
    cache_dir = resolve_cache_dir()
    if cache_dir is None:
        print("[-] No AST cache found under datasets/02_unmodified_oss/.cache or $REPOS_AST_DIR.")
        return 1

    compile_oss_contracts()
    manifest = load_manifest()
    print("=" * 102)
    print("  DATASET 2 — UNMODIFIED UPSTREAM OSS VERIFICATION (No Repo-Specific Heuristics)")
    print("=" * 102)

    rows = []
    for entry in manifest["targets"]:
        merged_methods: Dict[str, dict] = {}
        for af in entry["ast_files"]:
            ast_obj = load_ast_json(resolve_ast_path(cache_dir, af))
            merged_methods.update(find_methods_in_ast(ast_obj, entry["methods"]))

        ordered_nodes = [merged_methods[m] for m in entry["methods"]]
        src_ref = Path(entry["upstream_file"])
        checker = build_checker(entry)
        raw_init = entry.get("initial_stream_state")
        init_state = checker.initial_state if raw_init == "__INITIAL__" else raw_init
        passed = checker.analyze_real_methods(src_ref, ordered_nodes, initial_stream_state=init_state)

        exp = entry["expect_pass"]
        matched = (passed == exp)
        rows.append((entry, passed, matched, len(checker.diagnostics)))

    print(f"\n{'ID':<7} | {'Target':<30} | {'Adjudication':<24} | {'Actual':<14} | {'Verdict'}")
    print("-" * 102)
    for entry, passed, matched, ndiags in rows:
        adj = entry.get("adjudication", "conforming" if entry["expect_pass"] else "unconfirmed_candidate")
        adj_lbl = "Conforming Control" if adj == "confirmed_conforming" else "Cat-D Repro (Unconf.)"
        act_str = "PASS" if passed else f"FLAGGED ({ndiags})"
        verdict = "TN" if (entry["expect_pass"] and passed) else ("FP" if (entry["expect_pass"] and not passed) else ("FLAG-REPRO" if not passed else "MISS"))
        print(f"{entry['id']:<7} | {entry['label']:<30} | {adj_lbl:<24} | {act_str:<14} | {verdict}")
    print("-" * 102)

    tn = sum(1 for e, p, _, _ in rows if e.get("adjudication") == "confirmed_conforming" and p)
    fp = sum(1 for e, p, _, _ in rows if e.get("adjudication") == "confirmed_conforming" and not p)
    cand_flag = sum(1 for e, p, _, _ in rows if e.get("adjudication") == "unconfirmed_candidate_reproduced" and not p)
    cand_tot = sum(1 for e, _, _, _ in rows if e.get("adjudication") == "unconfirmed_candidate_reproduced")
    print(f"Conforming Controls (N={tn + fp}): TN={tn}, FP={fp}, Specificity={tn/(tn+fp) if (tn+fp) else 1.0:.3f}")
    print(f"Cat-D Reproduced Candidates [Unconfirmed Upstream] (N={cand_tot}): Flagged={cand_flag}/{cand_tot}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
