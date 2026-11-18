#!/usr/bin/env python3
"""
collect_metrics.py — Quantitative Benchmark & Metrics Collector for the C++ gRPC Session Checker.

Measures repeated post-warmup wall-clock latency (median & inclusive IQR for FSM compilation,
live Clang JSON AST extraction T_frontend, cached AST JSON decoding T_decode, and checker traversal
T_check), distinguishing R1 = T_check / (T_frontend + T_check) from R2 = T_check / (T_decode + T_check),
alongside detection effectiveness across:
  1. Dataset 1: datasets/01_initial_case_study/ (10 controlled client + dual-server handlers)
  2. Dataset 2: datasets/02_unmodified_oss/ (16 unmodified upstream methods across 6 OSS repos)

Outputs:
  - evaluation/METRICS_REPORT.md
  - evaluation/metrics_summary.json
"""

from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import statistics
import subprocess
import sys
import time
from typing import Dict, List, Optional, Tuple

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

EVAL_DIR = Path(__file__).resolve().parent
ROOT_DIR = EVAL_DIR.parent
POC_DIR = ROOT_DIR / "poc"
DS1_DIR = ROOT_DIR / "datasets" / "01_initial_case_study"
DS2_DIR = ROOT_DIR / "datasets" / "02_unmodified_oss"

sys.path.insert(0, str(POC_DIR))
sys.path.insert(0, str(DS2_DIR))

from grpc_session_check import ClangSessionChecker
from session_compiler import (
    compile_protocol_fsm,
    extract_session_specs_from_proto,
    parse_session_type,
)
from verify_oss import (
    compile_oss_contracts,
    build_checker,
    find_methods_in_ast,
    load_ast_json,
    load_manifest,
    resolve_ast_path,
    resolve_cache_dir,
)

WARMUP_COMP = 3
REPEATS_COMP = 15
WARMUP_DS1 = 1
REPEATS_DS1 = 5
WARMUP_DS2 = 1
REPEATS_DS2 = 5


def median_iqr(xs: List[float], digits: int = 3) -> Tuple[float, float]:
    """Returns (median, inclusive_iqr) for sample list `xs`."""
    if not xs:
        return (0.0, 0.0)
    med = statistics.median(xs)
    if len(xs) < 2:
        return (round(med, digits), 0.0)
    qs = statistics.quantiles(xs, n=4, method="inclusive")
    return (round(med, digits), round(qs[2] - qs[0], digits))


def count_loc(cpp_path: Path) -> Dict[str, int]:
    """Counts total lines, non-blank/non-comment SLOC, and inline checker annotations."""
    lines = cpp_path.read_text(encoding="utf-8", errors="replace").splitlines()
    sloc = sum(1 for ln in lines if ln.strip() and not ln.strip().startswith("//"))
    annot = sum(1 for ln in lines if "clang::annotate" in ln)
    return {"total_lines": len(lines), "sloc": sloc, "cpp_annotations": annot}


def _max_ast_line(node: dict) -> Optional[int]:
    """Finds the largest source line number present inside a Clang AST subtree."""
    best = None
    for key in ("loc", "range"):
        sub = node.get(key)
        if isinstance(sub, dict):
            for candidate in (sub, sub.get("begin"), sub.get("end"), sub.get("spellingLoc"), sub.get("expansionLoc")):
                if isinstance(candidate, dict) and isinstance(candidate.get("line"), int):
                    ln = candidate["line"]
                    best = ln if best is None else max(best, ln)
    for ch in node.get("inner", []):
        if isinstance(ch, dict):
            sub_best = _max_ast_line(ch)
            if sub_best is not None:
                best = sub_best if best is None else max(best, sub_best)
    return best


def count_method_loc(method_nodes: List[dict]) -> int:
    """Computes total upstream method span LOC from Clang AST loc/range nodes."""
    total = 0
    for node in method_nodes:
        rng = node.get("range", {})
        b = rng.get("begin", {}).get("line") or node.get("loc", {}).get("line")
        e = rng.get("end", {}).get("line") or _max_ast_line(node)
        if isinstance(b, int) and isinstance(e, int) and e >= b:
            total += (e - b + 1)
    return total


def compute_confusion(records: List[dict]) -> dict:
    """Computes binary classification metrics + pooled median/IQR latency for Dataset 1."""
    tp = sum(1 for r in records if not r["expect_pass"] and not r["actual_pass"])
    fn = sum(1 for r in records if not r["expect_pass"] and r["actual_pass"])
    tn = sum(1 for r in records if r["expect_pass"] and r["actual_pass"])
    fp = sum(1 for r in records if r["expect_pass"] and not r["actual_pass"])

    total = len(records)
    precision = tp / (tp + fp) if (tp + fp) > 0 else 1.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 1.0
    specificity = tn / (tn + fp) if (tn + fp) > 0 else 1.0
    accuracy = (tp + tn) / total if total > 0 else 1.0
    f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) > 0 else 0.0

    # Pool across all handler medians and all raw post-warmup samples
    case_plain_meds = [r["plain_ms_median"] for r in records]
    case_clang_meds = [r["clang_ms_median"] for r in records]
    case_chk_meds = [r["checker_ms_median"] for r in records]
    case_tot_meds = [r["total_ms_median"] for r in records]
    case_r1_meds = [r["r1_pct_median"] for r in records]
    case_ovh_meds = [r["overhead_pct_median"] for r in records]

    plain_med, plain_iqr = median_iqr(case_plain_meds, digits=2)
    clang_med, clang_iqr = median_iqr(case_clang_meds, digits=2)
    chk_med, chk_iqr = median_iqr(case_chk_meds, digits=3)
    tot_med, tot_iqr = median_iqr(case_tot_meds, digits=2)
    r1_med, r1_iqr = median_iqr(case_r1_meds, digits=4)
    ovh_med, ovh_iqr = median_iqr(case_ovh_meds, digits=2)

    total_sloc = sum(r.get("sloc", 0) for r in records)
    total_cpp_annots = sum(r.get("cpp_annotations", 0) for r in records)

    return {
        "n": total,
        "warmup": WARMUP_DS1,
        "repeats": REPEATS_DS1,
        "tp": tp,
        "fp": fp,
        "tn": tn,
        "fn": fn,
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "specificity": round(specificity, 4),
        "accuracy": round(accuracy, 4),
        "f1": round(f1, 4),
        "total_sloc": total_sloc,
        "total_cpp_annotations": total_cpp_annots,
        "cpp_annot_per_100_loc": round((100.0 * total_cpp_annots / total_sloc) if total_sloc else 0.0, 4),
        "plain_ms_median": plain_med,
        "plain_ms_iqr": plain_iqr,
        "clang_ms_median": clang_med,
        "clang_ms_iqr": clang_iqr,
        "checker_ms_median": chk_med,
        "checker_ms_iqr": chk_iqr,
        "total_ms_median": tot_med,
        "total_ms_iqr": tot_iqr,
        "r1_pct_median": r1_med,
        "r1_pct_iqr": r1_iqr,
        "overhead_pct_median": ovh_med,
        "overhead_pct_iqr": ovh_iqr,
        "max_loop_configs": max((r.get("max_loop_configs", 0) for r in records), default=0),
        "incomplete_loops": sum(r.get("incomplete_loops", 0) for r in records),
    }


def _compile_proto_once(ppath: Path, roles: Tuple[str, ...]) -> Tuple[int, int, Dict[str, Dict[str, int]]]:
    specs = extract_session_specs_from_proto(ppath)
    proto_text = ppath.read_text(encoding="utf-8", errors="replace")
    proto_opt_lines = sum(
        1 for ln in proto_text.splitlines()
        if ln.strip().startswith("option ") and ("session_type" in ln or "method_aliases" in ln)
    )
    compiled_roles: Dict[str, Dict[str, int]] = {}
    for spec in specs:
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
            states = contract["states"]
            transitions = sum(len(s.get("transitions", [])) for s in states.values())
            entry = compiled_roles.setdefault(role, {"states": 0, "transitions": 0})
            entry["states"] += len(states)
            entry["transitions"] += transitions
    return len(specs), max(proto_opt_lines, len(specs)), compiled_roles


def measure_fsm_compiler() -> List[dict]:
    """Benchmarks .proto session option extraction + duality + DFSM compilation with warm-up & IQR."""
    proto_targets = [
        ("handshake.proto", DS1_DIR / "01_pingpong_handshake" / "handshake.proto", ("client", "server")),
        ("config_stream.proto", DS1_DIR / "02_config_stream" / "config_stream.proto", ("client", "server")),
        ("wenet.proto", POC_DIR / "regression" / "wenet_demo" / "wenet.proto", ("client", "server")),
        ("real_repos.proto", POC_DIR / "regression" / "real_repo_bugs" / "real_repos.proto", ("client",)),
    ]
    rows = []
    for name, ppath, roles in proto_targets:
        for _ in range(WARMUP_COMP):
            _compile_proto_once(ppath, roles)
        samples = []
        methods, opt_lines, compiled_roles = 0, 0, {}
        for _ in range(REPEATS_COMP):
            t0 = time.perf_counter()
            methods, opt_lines, compiled_roles = _compile_proto_once(ppath, roles)
            samples.append((time.perf_counter() - t0) * 1000.0)
        comp_med, comp_iqr = median_iqr(samples, digits=3)
        rows.append({
            "proto": name,
            "methods": methods,
            "proto_option_lines": opt_lines,
            "warmup": WARMUP_COMP,
            "repeats": REPEATS_COMP,
            "compile_ms_median": comp_med,
            "compile_ms_iqr": comp_iqr,
            "roles": compiled_roles,
        })
    return rows


def run_single_file_case(
    label: str,
    spec_path: Path,
    cpp_path: Path,
    expect_pass: bool,
    extra_flags: Optional[List[str]] = None,
) -> dict:
    """Runs paired warmup + repeated plain clang++ -fsyntax-only vs. full AST+checker pipeline for Dataset 1."""
    plain_cmd = [
        "clang++",
        "--target=x86_64-w64-windows-gnu",
        "--sysroot=C:/msys64/ucrt64",
        "-DPROTOBUF_USE_DLLS",
        "-I", str(cpp_path.parent),
        "-I", str(cpp_path.parent.parent),
        "-I", r"C:\msys64\ucrt64\include",
    ]
    if extra_flags:
        plain_cmd.extend(extra_flags)
    else:
        plain_cmd.append("-std=c++17")
    plain_cmd.extend(["-fsyntax-only", str(cpp_path)])

    def _exec_plain_once() -> float:
        t0 = time.perf_counter()
        res = subprocess.run(plain_cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
        if res.returncode != 0:
            raise RuntimeError(f"Plain clang++ -fsyntax-only failed on {cpp_path}:\n{res.stderr}")
        return (time.perf_counter() - t0) * 1000.0

    def _exec_once() -> Tuple[bool, int, int, int, float, float]:
        checker = ClangSessionChecker(spec_path=spec_path)
        clang_time_holder = {"ms": 0.0}
        orig_dump = checker.run_clang_ast_dump

        def timed_dump(cf: Path, ast_filter: str = "Run", extra_flags: Optional[List[str]] = None):
            ts = time.perf_counter()
            res = orig_dump(cf, ast_filter=ast_filter, extra_flags=extra_flags)
            clang_time_holder["ms"] += (time.perf_counter() - ts) * 1000.0
            return res

        checker.run_clang_ast_dump = timed_dump
        buf = io.StringIO()
        t0 = time.perf_counter()
        with redirect_stdout(buf):
            actual_pass = checker.analyze_file(cpp_path, extra_flags=extra_flags)
        total_ms = (time.perf_counter() - t0) * 1000.0
        clang_ms = clang_time_holder["ms"]
        checker_ms = max(0.0, total_ms - clang_ms)
        return (
            actual_pass,
            len(checker.diagnostics),
            getattr(checker, "max_loop_configs_explored", 0),
            getattr(checker, "incomplete_loops", 0),
            clang_ms,
            checker_ms,
        )

    for _ in range(WARMUP_DS1):
        _exec_plain_once()
        _exec_once()

    plain_samples: List[float] = []
    clang_samples: List[float] = []
    chk_samples: List[float] = []
    tot_samples: List[float] = []
    r1_samples: List[float] = []
    ovh_samples: List[float] = []
    actual_pass = True
    diags = 0
    max_lc = 0
    inc_l = 0
    for _ in range(REPEATS_DS1):
        pl_ms = _exec_plain_once()
        actual_pass, diags, max_lc, inc_l, cl_ms, ch_ms = _exec_once()
        tot = cl_ms + ch_ms
        r1 = (100.0 * ch_ms / tot) if tot > 0 else 0.0
        ovh = (100.0 * (tot - pl_ms) / pl_ms) if pl_ms > 0 else 0.0
        plain_samples.append(pl_ms)
        clang_samples.append(cl_ms)
        chk_samples.append(ch_ms)
        tot_samples.append(tot)
        r1_samples.append(r1)
        ovh_samples.append(ovh)

    pl_med, pl_iqr = median_iqr(plain_samples, digits=2)
    cl_med, cl_iqr = median_iqr(clang_samples, digits=2)
    ch_med, ch_iqr = median_iqr(chk_samples, digits=3)
    tot_med, tot_iqr = median_iqr(tot_samples, digits=2)
    r1_med, r1_iqr = median_iqr(r1_samples, digits=4)
    ovh_med, ovh_iqr = median_iqr(ovh_samples, digits=2)
    loc_info = count_loc(cpp_path)

    return {
        "label": label,
        "file": str(cpp_path.relative_to(DS1_DIR)).replace("\\", "/"),
        "expect_pass": expect_pass,
        "actual_pass": actual_pass,
        "matched": actual_pass == expect_pass,
        "diags": diags,
        "max_loop_configs": max_lc,
        "incomplete_loops": inc_l,
        "sloc": loc_info["sloc"],
        "cpp_annotations": loc_info["cpp_annotations"],
        "plain_ms_median": pl_med,
        "plain_ms_iqr": pl_iqr,
        "clang_ms_median": cl_med,
        "clang_ms_iqr": cl_iqr,
        "checker_ms_median": ch_med,
        "checker_ms_iqr": ch_iqr,
        "total_ms_median": tot_med,
        "total_ms_iqr": tot_iqr,
        "r1_pct_median": r1_med,
        "r1_pct_iqr": r1_iqr,
        "overhead_pct_median": ovh_med,
        "overhead_pct_iqr": ovh_iqr,
    }


def collect_dataset_1() -> List[dict]:
    ex1 = DS1_DIR / "01_pingpong_handshake"
    ex2 = DS1_DIR / "02_config_stream"
    extra_flags = ["-std=c++17", "-I", str(DS1_DIR / "generated")]
    cases = [
        ("Ex1 Client Good", ex1 / "exchange_client.session.json", ex1 / "client_good.cpp", True),
        ("Ex1 Client Bad Swap", ex1 / "exchange_client.session.json", ex1 / "client_bad_swap.cpp", False),
        ("Ex1 Client Bad Leak", ex1 / "exchange_client.session.json", ex1 / "client_bad_leak.cpp", False),
        ("Ex1 Server Dual Good", ex1 / "exchange_server_dual.session.json", ex1 / "server_good.cpp", True),
        ("Ex1 Server Bad Order", ex1 / "exchange_server_dual.session.json", ex1 / "server_bad_order.cpp", False),
        ("Ex1 Server Missing Summ.", ex1 / "exchange_server_dual.session.json", ex1 / "server_bad_missing_summary.cpp", False),
        ("Ex2 Client Good (oneof)", ex2 / "upload_client.session.json", ex2 / "client_good.cpp", True),
        ("Ex2 Client Bad oneof", ex2 / "upload_client.session.json", ex2 / "client_bad_oneof.cpp", False),
        ("Ex2 Client Bad PostClose", ex2 / "upload_client.session.json", ex2 / "client_bad_post_close.cpp", False),
        ("Ex2 Server Dual Good", ex2 / "upload_server_dual.session.json", ex2 / "server_good.cpp", True),
    ]
    return [run_single_file_case(lbl, sp, cp, exp, extra_flags=extra_flags) for lbl, sp, cp, exp in cases]


def collect_dataset_2() -> Tuple[bool, List[dict]]:
    cache_dir = resolve_cache_dir()
    if cache_dir is None:
        return False, []

    compile_oss_contracts()
    manifest = load_manifest()
    rows = []
    for entry in manifest["targets"]:
        ast_paths = [resolve_ast_path(cache_dir, af) for af in entry["ast_files"]]
        ast_bytes = sum(p.stat().st_size for p in ast_paths)

        def _run_ds2_once():
            t_decode0 = time.perf_counter()
            merged_methods: Dict[str, dict] = {}
            for ap in ast_paths:
                ast_obj = load_ast_json(ap)
                merged_methods.update(find_methods_in_ast(ast_obj, entry["methods"]))
            decode_ms = (time.perf_counter() - t_decode0) * 1000.0
            ordered_nodes = [merged_methods[m] for m in entry["methods"]]
            sloc_span = count_method_loc(ordered_nodes)

            checker = build_checker(entry)
            raw_init = entry.get("initial_stream_state")
            init_state = checker.initial_state if raw_init == "__INITIAL__" else raw_init
            buf = io.StringIO()
            t_chk0 = time.perf_counter()
            with redirect_stdout(buf):
                actual_pass = checker.analyze_real_methods(
                    Path(entry["upstream_file"]), ordered_nodes, initial_stream_state=init_state
                )
            checker_ms = (time.perf_counter() - t_chk0) * 1000.0
            return (
                actual_pass,
                len(checker.diagnostics),
                getattr(checker, "max_loop_configs_explored", 0),
                getattr(checker, "incomplete_loops", 0),
                sloc_span,
                decode_ms,
                checker_ms,
            )

        for _ in range(WARMUP_DS2):
            _run_ds2_once()

        decode_samples: List[float] = []
        chk_samples: List[float] = []
        tot_samples: List[float] = []
        r2_samples: List[float] = []
        actual_pass, diags, max_lc, inc_l, sloc_span = True, 0, 0, 0, 0
        for _ in range(REPEATS_DS2):
            actual_pass, diags, max_lc, inc_l, sloc_span, dec_ms, ch_ms = _run_ds2_once()
            tot = dec_ms + ch_ms
            r2 = (100.0 * ch_ms / tot) if tot > 0 else 0.0
            decode_samples.append(dec_ms)
            chk_samples.append(ch_ms)
            tot_samples.append(tot)
            r2_samples.append(r2)

        dec_med, dec_iqr = median_iqr(decode_samples, digits=2)
        ch_med, ch_iqr = median_iqr(chk_samples, digits=3)
        tot_med, tot_iqr = median_iqr(tot_samples, digits=2)
        r2_med, r2_iqr = median_iqr(r2_samples, digits=2)

        spec_kind = "custom" if "contract" in entry else "default-L1"
        rows.append({
            "id": entry["id"],
            "label": entry["label"],
            "repo": entry.get("repo", ""),
            "commit": entry.get("commit", ""),
            "file": entry["upstream_file"],
            "adjudication": entry.get("adjudication", "confirmed_conforming" if entry["expect_pass"] else "unconfirmed_candidate_reproduced"),
            "spec_provenance": entry.get("spec_provenance", ""),
            "expect_pass": entry["expect_pass"],
            "actual_pass": actual_pass,
            "matched": actual_pass == entry["expect_pass"],
            "diags": diags,
            "max_loop_configs": max_lc,
            "incomplete_loops": inc_l,
            "spec_kind": spec_kind,
            "sloc": sloc_span,
            "cpp_annotations": 0,
            "ast_mb": round(ast_bytes / (1024.0 * 1024.0), 2),
            "ast_decode_ms_median": dec_med,
            "ast_decode_ms_iqr": dec_iqr,
            "checker_ms_median": ch_med,
            "checker_ms_iqr": ch_iqr,
            "total_ms_median": tot_med,
            "total_ms_iqr": tot_iqr,
            "r2_pct_median": r2_med,
            "r2_pct_iqr": r2_iqr,
        })
    return True, rows


def detect_toolchain_info() -> dict:
    import platform
    import subprocess
    clang_ver = "unknown"
    try:
        res = subprocess.run(["clang++", "--version"], capture_output=True, text=True, timeout=5)
        if res.stdout:
            clang_ver = res.stdout.splitlines()[0].strip()
    except Exception:
        pass
    return {
        "os": platform.platform(),
        "python": platform.python_version(),
        "clang": clang_ver,
    }


def compute_ds2_provenance_summary(ds2_rows: List[dict]) -> dict:
    ctrl_rows = [r for r in ds2_rows if r.get("adjudication") == "confirmed_conforming"]
    cand_rows = [r for r in ds2_rows if r.get("adjudication") == "unconfirmed_candidate_reproduced"]
    tn = sum(1 for r in ctrl_rows if r["actual_pass"])
    fp = sum(1 for r in ctrl_rows if not r["actual_pass"])
    cand_flagged = sum(1 for r in cand_rows if not r["actual_pass"])
    cand_missed = sum(1 for r in cand_rows if r["actual_pass"])
    spec = tn / (tn + fp) if (tn + fp) > 0 else 1.0

    dec_med, dec_iqr = median_iqr([r["ast_decode_ms_median"] for r in ds2_rows], digits=2)
    chk_med, chk_iqr = median_iqr([r["checker_ms_median"] for r in ds2_rows], digits=3)
    r2_med, r2_iqr = median_iqr([r["r2_pct_median"] for r in ds2_rows], digits=2)

    ctrl_dec_med, ctrl_dec_iqr = median_iqr([r["ast_decode_ms_median"] for r in ctrl_rows], digits=2)
    ctrl_chk_med, ctrl_chk_iqr = median_iqr([r["checker_ms_median"] for r in ctrl_rows], digits=3)
    ctrl_r2_med, ctrl_r2_iqr = median_iqr([r["r2_pct_median"] for r in ctrl_rows], digits=2)

    cand_dec_med, cand_dec_iqr = median_iqr([r["ast_decode_ms_median"] for r in cand_rows], digits=2)
    cand_chk_med, cand_chk_iqr = median_iqr([r["checker_ms_median"] for r in cand_rows], digits=3)
    cand_r2_med, cand_r2_iqr = median_iqr([r["r2_pct_median"] for r in cand_rows], digits=2)

    return {
        "n_total": len(ds2_rows),
        "warmup": WARMUP_DS2,
        "repeats": REPEATS_DS2,
        "n_controls": len(ctrl_rows),
        "controls_tn": tn,
        "controls_fp": fp,
        "controls_specificity": round(spec, 4),
        "controls_max_loop_configs": max((r.get("max_loop_configs", 0) for r in ctrl_rows), default=0),
        "controls_decode_ms_median": ctrl_dec_med,
        "controls_decode_ms_iqr": ctrl_dec_iqr,
        "controls_checker_ms_median": ctrl_chk_med,
        "controls_checker_ms_iqr": ctrl_chk_iqr,
        "controls_r2_pct_median": ctrl_r2_med,
        "controls_r2_pct_iqr": ctrl_r2_iqr,
        "n_candidates_unconfirmed": len(cand_rows),
        "candidates_flagged": cand_flagged,
        "candidates_missed": cand_missed,
        "candidates_max_loop_configs": max((r.get("max_loop_configs", 0) for r in cand_rows), default=0),
        "candidates_decode_ms_median": cand_dec_med,
        "candidates_decode_ms_iqr": cand_dec_iqr,
        "candidates_checker_ms_median": cand_chk_med,
        "candidates_checker_ms_iqr": cand_chk_iqr,
        "candidates_r2_pct_median": cand_r2_med,
        "candidates_r2_pct_iqr": cand_r2_iqr,
        "cpp_annot_per_100_loc": 0.0,
        "ast_decode_ms_median": dec_med,
        "ast_decode_ms_iqr": dec_iqr,
        "checker_ms_median": chk_med,
        "checker_ms_iqr": chk_iqr,
        "r2_pct_median": r2_med,
        "r2_pct_iqr": r2_iqr,
        "max_loop_configs": max((r.get("max_loop_configs", 0) for r in ds2_rows), default=0),
        "incomplete_loops": sum(r.get("incomplete_loops", 0) for r in ds2_rows),
    }


def format_markdown(env_info, fsm_rows, ds1_rows, ds1_sum, ds2_ok, ds2_rows, ds2_prov) -> str:
    lines = [
        "# Quantitative Evaluation & Benchmark Metrics (`evaluation/collect_metrics.py`)",
        "",
        f"- **Environment:** {env_info['os']} | Python `{env_info['python']}` | `{env_info['clang']}`",
        f"- **Timing Protocol:** Post-warmup median and inclusive IQR (Compiler: {WARMUP_COMP}+{REPEATS_COMP}; DS1: {WARMUP_DS1}+{REPEATS_DS1}; DS2: {WARMUP_DS2}+{REPEATS_DS2}).",
        "- $O_{\\text{build}} = (T_{\\text{pipeline}} - T_{\\text{plain}}) / T_{\\text{plain}}$ (paired end-to-end overhead over plain `clang++ -fsyntax-only`); $R_2 = T_{\\text{check}} / (T_{\\text{decode}} + T_{\\text{check}})$ (cached AST JSON).",
        "",
        "## 1. Controlled Case Study vs. Unmodified OSS Ground-Truth Controls & Category D Triage",
        "",
        "### 1a. Controlled Dataset 1 (`datasets/01_initial_case_study/`)",
        "",
        "| Dataset | Cases ($N$) | TP / FP / TN / FN | Prec | Rec | Spec | $F_1$ | Plain Med [IQR] (ms) | Pipeline Med [IQR] (ms) | Check Med [IQR] (ms) | $O_{\\text{build}}$ Med [IQR] (%) |",
        "| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |",
        f"| Dataset 1: Initial Case Study | {ds1_sum['n']} | {ds1_sum['tp']} / {ds1_sum['fp']} / {ds1_sum['tn']} / {ds1_sum['fn']} | "
        f"{ds1_sum['precision']:.3f} | {ds1_sum['recall']:.3f} | {ds1_sum['specificity']:.3f} | {ds1_sum['f1']:.3f} | "
        f"{ds1_sum['plain_ms_median']:.1f} [{ds1_sum['plain_ms_iqr']:.1f}] | "
        f"{ds1_sum['total_ms_median']:.1f} [{ds1_sum['total_ms_iqr']:.1f}] | "
        f"{ds1_sum['checker_ms_median']:.2f} [{ds1_sum['checker_ms_iqr']:.2f}] | "
        f"{ds1_sum['overhead_pct_median']:.2f}% [{ds1_sum['overhead_pct_iqr']:.2f}%] |",
    ]
    if ds2_ok and ds2_prov:
        total_diags = sum(r["diags"] for r in ds2_rows)
        lines.extend([
            "",
            "### 1b. Unmodified OSS Baseline (`datasets/02_unmodified_oss/`, Provenance-Separated)",
            "",
            "| Cohort (Adjudication) | Cases ($N$) | Outcome Split | Specificity | Decode Med [IQR] (ms) | Check Med [IQR] (ms) | $R_2$ Med [IQR] (%) | Max Loop Configs |",
            "| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |",
            f"| Confirmed Conforming Controls | {ds2_prov['n_controls']} | TN={ds2_prov['controls_tn']} / FP={ds2_prov['controls_fp']} | "
            f"{ds2_prov['controls_specificity']:.3f} | "
            f"{ds2_prov['controls_decode_ms_median']:.1f} [{ds2_prov['controls_decode_ms_iqr']:.1f}] | "
            f"{ds2_prov['controls_checker_ms_median']:.2f} [{ds2_prov['controls_checker_ms_iqr']:.2f}] | "
            f"{ds2_prov['controls_r2_pct_median']:.1f}% [{ds2_prov['controls_r2_pct_iqr']:.1f}%] | "
            f"{ds2_prov['controls_max_loop_configs']} |",
            f"| Category D Repro Candidates [Unconfirmed] | {ds2_prov['n_candidates_unconfirmed']} | Flagged={ds2_prov['candidates_flagged']} / Missed={ds2_prov['candidates_missed']} | "
            f"-- (unconfirmed) | "
            f"{ds2_prov['candidates_decode_ms_median']:.1f} [{ds2_prov['candidates_decode_ms_iqr']:.1f}] | "
            f"{ds2_prov['candidates_checker_ms_median']:.2f} [{ds2_prov['candidates_checker_ms_iqr']:.2f}] | "
            f"{ds2_prov['candidates_r2_pct_median']:.1f}% [{ds2_prov['candidates_r2_pct_iqr']:.1f}%] | "
            f"{ds2_prov['candidates_max_loop_configs']} |",
            "",
            f"> **Dataset 2 Provenance Note:** Evaluated on cached upstream Clang AST JSONs ({total_diags} diagnostics total without repo-specific heuristics). Dataset 1 max loop configs = {ds1_sum['max_loop_configs']}, Dataset 2 Controls = {ds2_prov['controls_max_loop_configs']}, Dataset 2 Candidates = {ds2_prov['candidates_max_loop_configs']}. Category D candidates remain unconfirmed upstream and excluded from confirmed Recall.",
        ])

    lines.extend([
        "",
        r"## 2. Session Compiler Overhead (`.proto` `MethodOptions` $\rightarrow$ Dual `grpc.session.v1` FSMs)",
        "",
        r"| Protocol Schema | RPCs | Opt Lines | Compile Med [IQR] (ms) | Client $\|Q\| / \|\delta\|$ | Dual Server $\|Q\| / \|\delta\|$ |",
        "| :--- | :---: | :---: | :---: | :---: | :---: |",
    ])
    for r in fsm_rows:
        c = r["roles"].get("client", {"states": "-", "transitions": "-"})
        sv = r["roles"].get("server", {"states": "-", "transitions": "-"})
        lines.append(
            f"| `{r['proto']}` | {r['methods']} | {r['proto_option_lines']} | "
            f"{r['compile_ms_median']:.2f} [{r['compile_ms_iqr']:.2f}] | "
            f"{c['states']} / {c['transitions']} | {sv['states']} / {sv['transitions']} |"
        )

    lines.extend([
        "",
        "## 3. Dataset 1 Breakdown (`datasets/01_initial_case_study/`, 10 Handlers)",
        "",
        "| Case | SLOC | Annots | Plain Med [IQR] (ms) | Pipeline Med [IQR] (ms) | Check Med (ms) | $O_{\\text{build}}$ (%) | Verdict | Diags |",
        "| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |",
    ])
    for r in ds1_rows:
        v = "PASS" if r["actual_pass"] else "FAIL"
        tag = "OK" if r["matched"] else "MISMATCH"
        lines.append(
            f"| {r['label']} | {r['sloc']} | {r['cpp_annotations']} | "
            f"{r['plain_ms_median']:.1f} [{r['plain_ms_iqr']:.1f}] | "
            f"{r['total_ms_median']:.1f} [{r['total_ms_iqr']:.1f}] | "
            f"{r['checker_ms_median']:.2f} | "
            f"{r['overhead_pct_median']:.2f}% [{r['overhead_pct_iqr']:.2f}%] | {v} ({tag}) | {r['diags']} |"
        )

    if ds2_ok:
        lines.extend([
            "",
            "## 4. Dataset 2 Breakdown (`datasets/02_unmodified_oss/`, 16 Targets)",
            "",
            "| ID | Case | Adjudication | Spec | LOC | AST MB | Decode Med (ms) | Check Med (ms) | $R_2$ (%) | Verdict | Diags |",
            "| :--- | :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |",
        ])
        for r in ds2_rows:
            adj = "Conforming" if r.get("adjudication") == "confirmed_conforming" else "Cat-D Unconf."
            if r.get("adjudication") == "confirmed_conforming":
                v = "TN (PASS)" if r["actual_pass"] else "FP (FLAG)"
            else:
                v = "FLAG-REPRO" if not r["actual_pass"] else "MISS"
            lines.append(
                f"| `{r['id']}` | {r['label']} | {adj} | `{r['spec_kind']}` | {r['sloc']} | {r['ast_mb']:.2f} | "
                f"{r['ast_decode_ms_median']:.2f} | {r['checker_ms_median']:.2f} | {r['r2_pct_median']:.1f}% | {v} | {r['diags']} |"
            )

    return "\n".join(lines) + "\n"


def main():
    env_info = detect_toolchain_info()
    fsm_rows = measure_fsm_compiler()
    ds1_rows = collect_dataset_1()
    ds1_sum = compute_confusion(ds1_rows)
    ds2_ok, ds2_rows = collect_dataset_2()
    ds2_prov = compute_ds2_provenance_summary(ds2_rows) if ds2_ok else None

    summary_payload = {
        "environment": env_info,
        "fsm_compiler": fsm_rows,
        "dataset_1_minimal_examples": {"summary": ds1_sum, "cases": ds1_rows},
        "dataset_2_unmodified_real_repos": {
            "available": ds2_ok,
            "provenance_summary": ds2_prov,
            "cases": ds2_rows,
        },
    }

    (EVAL_DIR / "metrics_summary.json").write_text(json.dumps(summary_payload, indent=2), encoding="utf-8")
    md_text = format_markdown(env_info, fsm_rows, ds1_rows, ds1_sum, ds2_ok, ds2_rows, ds2_prov)
    (EVAL_DIR / "METRICS_REPORT.md").write_text(md_text, encoding="utf-8")
    print(md_text)


if __name__ == "__main__":
    main()
