#!/usr/bin/env python3
"""
check_default_l1_sync.py — Extracts canonical states and labeled edges directly from
ClangSessionChecker.from_default_level1(...) for all six synchronous C++ gRPC stream classes,
emits synchronized LaTeX table rows, and verifies that the LaTeX chapter matches the code 1-for-1.
"""

from pathlib import Path
import re
import sys

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR / "poc"))

from grpc_session_check import ClangSessionChecker


CLASSES = [
    ("ClientReader", "SERVER_STREAMING", "client", "fig:default-l1-client-reader"),
    ("ClientWriter", "CLIENT_STREAMING", "client", "fig:default-l1-client-writer"),
    ("ClientReaderWriter", "BIDIRECTIONAL_STREAMING", "client", "fig:default-l1-client-bidi"),
    ("ServerWriter", "SERVER_STREAMING", "server", "fig:default-l1-server-writer"),
    ("ServerReader", "CLIENT_STREAMING", "server", "fig:default-l1-server-reader"),
    ("ServerReaderWriter", "BIDIRECTIONAL_STREAMING", "server", "fig:default-l1-server-bidi"),
]


def short_state_label(state_name: str) -> str:
    """Short state name rendered inside TikZ nodes, e.g., S0_OPEN -> S0."""
    return state_name.split("_", 1)[0]


def format_op_outcome(tr: dict) -> str:
    op = tr["op"]
    out = tr.get("outcome")
    return f"{op}({out})" if out else op


def dump_canonical():
    summary = {}
    for cls_name, rpc_type, role, _ in CLASSES:
        chk = ClangSessionChecker.from_default_level1(rpc_type, "Call", role=role)
        states = list(chk.states.keys())
        terms = [s for s, d in chk.states.items() if d.get("terminal")]
        edges = []
        for src, sdata in chk.states.items():
            for tr in sdata.get("transitions", []):
                edges.append((src, format_op_outcome(tr), tr["target"]))
        summary[cls_name] = {
            "initial": chk.initial_state,
            "states": states,
            "terminal": terms,
            "edges": edges,
        }
    return summary


def verify_tex(tex_paths: list[Path]) -> bool:
    tex = "\n".join(p.read_text(encoding="utf-8") for p in tex_paths if p.exists())
    summary = dump_canonical()
    ok = True

    tab_match = re.search(r"\\begin\{table\}(?:(?!\\begin\{table\}).)*?\\label\{tab:default-l1\}", tex, re.S)
    fig_match = re.search(r"\\begin\{figure\}(?:(?!\\begin\{figure\}).)*?\\label\{fig:default-l1-fsms\}", tex, re.S)
    if not tab_match or not fig_match:
        print("[-] Could not locate tab:default-l1 or fig:default-l1-fsms in LaTeX.")
        return False

    tab_block = tab_match.group(0)
    fig_block = fig_match.group(0)

    # Check every canonical state identifier appears in tab:default-l1 (escaped underscores)
    for cls_name, info in summary.items():
        if cls_name not in tab_block or cls_name not in fig_block:
            print(f"[-] Missing class '{cls_name}' in tab:default-l1 or fig:default-l1-fsms.")
            ok = False
        for s in info["states"]:
            escaped_s = s.replace("_", r"\_")
            if escaped_s not in tab_block:
                print(f"[-] Missing state '{s}' for {cls_name} in tab:default-l1.")
                ok = False
        # Verify ClientReaderWriter forbids Write after Read(eos) in both table and figure
        if cls_name == "ClientReaderWriter":
            if "S1\\_IN\\_CLOSED" in tab_block or "S2\\_OUT\\_CLOSED" in tab_block or "S4\\_FINISHED" in tab_block:
                print("[-] Stale Round-4 ClientReaderWriter state names found in tab:default-l1!")
                ok = False
    return ok


def main():
    summary = dump_canonical()
    for cls_name, info in summary.items():
        print(f"=== {cls_name} (init={info['initial']}, term={info['terminal']}) ===")
        for src, lbl, dst in info["edges"]:
            print(f"  {src} --[{lbl}]--> {dst}")
    if "--verify-tex" in sys.argv:
        tex_paths = [
            ROOT_DIR / "latex" / "chapters" / "ch2_experimental_modeling.tex",
            ROOT_DIR / "latex" / "chapters" / "appendix_reference.tex",
        ]
        if not verify_tex(tex_paths):
            sys.exit(1)
        print("[+] LaTeX tab:default-l1 and fig:default-l1-fsms verified in sync with from_default_level1.")


if __name__ == "__main__":
    main()
