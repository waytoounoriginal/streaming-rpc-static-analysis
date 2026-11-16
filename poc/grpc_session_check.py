#!/usr/bin/env python3
"""
grpc_session_check.py — Clang AST-based Static Typestate Checker for gRPC Streaming
Part of Bachelor Thesis: "Types Won't Save You, But Typestates Will: Static Protocol Checking for C++ gRPC"
Author: Mihai Tira

Structured AST control-flow interpreter for synchronous C++ gRPC streaming endpoints:
1. Clang AST JSON extraction via `clang++ -Xclang -ast-dump=json -fsyntax-only`.
2. Binary Session Type DFSM ingestion (`grpc.session.v1` or 6-class default Level 1 automata).
3. Power-set correlated lattice `(stream_states, payload_states, bool_prov, cancel_requested, ptr_null, server_eos_mode)`
   with 5-channel transfer `(fallthrough, return, throw, break, continue)`, finite-site fixed-point loops,
   and statement-boundary throw state propagation inside `try`/`catch`.
4. Orthogonal product-domain Level 2 payload tracking (`oneof` groups + independent field slots).
5. Endpoint-specific termination model separating:
   - Client `Finish(OK)` vs `Finish(non-OK)` (F_status_nonOK) vs write/half-close abort (F_abort),
   - Server explicit non-`OK` returns (F_app_err), verified `IsCancelled()` aborts (F_abort),
     confirmed `Read(eos)` (`!IsCancelled()`), and ambiguous `Read(eos)` without `IsCancelled()`.
"""

from dataclasses import dataclass, field
import json
from pathlib import Path
import subprocess
import sys
from typing import Dict, List, Optional, Set, Tuple

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


@dataclass
class AbstractState:
    """One correlated reachable abstract configuration at an AST program point."""
    stream_states: Dict[str, str] = field(default_factory=dict)
    payload_states: Dict[str, Dict[str, int]] = field(default_factory=dict)
    bool_prov: Dict[str, Tuple[str, str, str]] = field(default_factory=dict)
    cancel_requested_streams: Set[str] = field(default_factory=set)
    ptr_null_streams: Set[str] = field(default_factory=set)
    eos_observed_streams: Set[str] = field(default_factory=set)
    # Maps server stream var -> {"ambiguous", "confirmed_norm", "confirmed_cancel"} after Read(eos)
    server_eos_mode: Dict[str, str] = field(default_factory=dict)
    # Tracks client incoming completion readiness in {"Yes", "No", "Unknown"}
    recv_complete: Dict[str, str] = field(default_factory=dict)
    # Tracks WriteOptions variables that currently have set_last_message() enabled
    last_msg_options: Set[str] = field(default_factory=set)
    # Tracks streams (notably server streams) where WriteLast / set_last_message was already sent
    write_last_streams: Set[str] = field(default_factory=set)
    # Tracks grpc::Status variable bindings in {"OK", "non_ok", "Unknown"}
    status_vars: Dict[str, str] = field(default_factory=dict)

    def clone(self) -> "AbstractState":
        return AbstractState(
            stream_states=dict(self.stream_states),
            payload_states={k: dict(v) for k, v in self.payload_states.items()},
            bool_prov=dict(self.bool_prov),
            cancel_requested_streams=set(self.cancel_requested_streams),
            ptr_null_streams=set(self.ptr_null_streams),
            eos_observed_streams=set(self.eos_observed_streams),
            server_eos_mode=dict(self.server_eos_mode),
            recv_complete=dict(self.recv_complete),
            last_msg_options=set(self.last_msg_options),
            write_last_streams=set(self.write_last_streams),
            status_vars=dict(self.status_vars),
        )

    def freeze_key(self) -> tuple:
        return (
            tuple(sorted(self.stream_states.items())),
            tuple((m, tuple(sorted(g.items()))) for m, g in sorted(self.payload_states.items())),
            tuple(sorted(self.bool_prov.items())),
            tuple(sorted(self.cancel_requested_streams)),
            tuple(sorted(self.ptr_null_streams)),
            tuple(sorted(self.eos_observed_streams)),
            tuple(sorted(self.server_eos_mode.items())),
            tuple(sorted(self.recv_complete.items())),
            tuple(sorted(self.last_msg_options)),
            tuple(sorted(self.write_last_streams)),
            tuple(sorted(self.status_vars.items())),
        )


@dataclass
class WalkResult:
    """Explicit control-flow transfer channels from interpreting an AST block."""
    fallthrough: List[AbstractState]
    terminated: bool
    return_exits: List[AbstractState] = field(default_factory=list)
    break_exits: List[AbstractState] = field(default_factory=list)
    continue_exits: List[AbstractState] = field(default_factory=list)


def dedup_states(states: List[AbstractState]) -> List[AbstractState]:
    """Deduplicates reachable configurations while preserving deterministic order."""
    seen: Set[tuple] = set()
    out: List[AbstractState] = []
    for st in states:
        k = st.freeze_key()
        if k not in seen:
            seen.add(k)
            out.append(st)
    return out


def find_decl_ref_name(node: dict) -> Optional[str]:
    """Recursively searches for a variable, parameter, or `this->field` reference."""
    if not isinstance(node, dict):
        return None
    kind = node.get("kind")
    if kind == "DeclRefExpr":
        ref = node.get("referencedDecl", {})
        ref_kind = ref.get("kind", "")
        ref_name = ref.get("name") or node.get("name")
        if ref_kind in ("VarDecl", "ParmVarDecl") and ref_name:
            return ref_name
    elif kind == "MemberExpr":
        qtype = node.get("type", {}).get("qualType", "")
        name = node.get("name")
        if name and qtype != "<bound member function type>":
            inner = node.get("inner", [])
            if inner and inner[0].get("kind") == "CXXThisExpr":
                return name
    for child in node.get("inner", []):
        res = find_decl_ref_name(child)
        if res:
            return res
    return None


def has_member_call(node: dict, target_member: str) -> bool:
    """Checks whether an AST subtree invokes a specific member method."""
    if not isinstance(node, dict):
        return False
    if node.get("kind") == "CXXMemberCallExpr":
        inner = node.get("inner", [])
        if inner and inner[0].get("kind") == "MemberExpr" and inner[0].get("name") == target_member:
            return True
    return any(has_member_call(ch, target_member) for ch in node.get("inner", []))


def has_any_call(node: dict) -> bool:
    """Checks whether a statement subtree contains any potentially throwing call/throw expression."""
    if not isinstance(node, dict):
        return False
    if node.get("kind") in ("CXXMemberCallExpr", "CallExpr", "CXXConstructExpr", "CXXThrowExpr"):
        return True
    return any(has_any_call(ch) for ch in node.get("inner", []))


def find_member_call(node: dict, target_members: Tuple[str, ...]) -> Optional[Tuple[Optional[str], str]]:
    """Finds the first matching stream call `(receiver_var, op_name)` inside an AST subtree."""
    if not isinstance(node, dict):
        return None
    if node.get("kind") == "CXXMemberCallExpr":
        inner = node.get("inner", [])
        if inner and inner[0].get("kind") == "MemberExpr":
            mname = inner[0].get("name", "")
            if mname in target_members:
                return (find_decl_ref_name(inner[0]), mname)
    for ch in node.get("inner", []):
        hit = find_member_call(ch, target_members)
        if hit is not None:
            return hit
    return None


def is_false_literal_expr(node: Optional[dict]) -> bool:
    """Checks whether an expression represents literal boolean `false` or integer `0`."""
    if not node:
        return True
    kind = node.get("kind", "")
    if kind == "CXXBoolLiteralExpr" and node.get("value") is False:
        return True
    if kind == "IntegerLiteral" and str(node.get("value", "")) == "0":
        return True
    if kind in ("ImplicitCastExpr", "ParenExpr"):
        return any(is_false_literal_expr(ch) for ch in node.get("inner", []))
    return False


def _has_nullptr_literal(node: Optional[dict]) -> bool:
    if not isinstance(node, dict):
        return False
    if node.get("kind") == "CXXNullPtrLiteralExpr":
        return True
    return any(_has_nullptr_literal(ch) for ch in node.get("inner", []))


def _unwrap_cast_paren(node: dict) -> dict:
    """Strips transparent ParenExpr / ImplicitCastExpr / ExprWithCleanups wrappers."""
    cur = node
    while isinstance(cur, dict) and cur.get("kind") in ("ImplicitCastExpr", "ParenExpr", "ExprWithCleanups"):
        inner = cur.get("inner", [])
        if not inner:
            break
        cur = inner[0]
    return cur


def _has_false_comparison(node: dict) -> Optional[dict]:
    """If `node` is `expr == false` or `false == expr`, returns inner `expr`."""
    unwrapped = _unwrap_cast_paren(node)
    if unwrapped.get("kind") == "BinaryOperator" and unwrapped.get("opcode") == "==":
        ch = unwrapped.get("inner", [])
        if len(ch) == 2:
            if is_false_literal_expr(ch[1]):
                return ch[0]
            if is_false_literal_expr(ch[0]):
                return ch[1]
    return None


def is_negated_expr(node: dict) -> bool:
    """Checks for `!expr` or `expr == false` patterns."""
    unwrapped = _unwrap_cast_paren(node)
    if unwrapped.get("kind") == "UnaryOperator" and unwrapped.get("opcode") == "!":
        return True
    if _has_false_comparison(unwrapped) is not None:
        return True
    return any(
        is_negated_expr(ch)
        for ch in node.get("inner", [])
        if node.get("kind") == "BinaryOperator" and node.get("opcode") == "&&"
    )


def _extract_negated_target(cond: dict) -> Optional[dict]:
    """Returns the inner expression negated by `!x` or `x == false`, else None."""
    unwrapped = _unwrap_cast_paren(cond)
    if unwrapped.get("kind") == "UnaryOperator" and unwrapped.get("opcode") == "!":
        inner = unwrapped.get("inner", [])
        return inner[0] if inner else None
    cmp_inner = _has_false_comparison(unwrapped)
    if cmp_inner is not None:
        return cmp_inner
    if unwrapped.get("kind") == "BinaryOperator" and unwrapped.get("opcode") in ("&&", "||"):
        for ch in unwrapped.get("inner", []):
            sub = _extract_negated_target(ch)
            if sub is not None:
                return sub
    return None


def _is_stream_qualtype(qtype: str) -> bool:
    return any(
        tok in qtype
        for tok in (
            "ClientReader",
            "ClientWriter",
            "ClientReaderWriter",
            "ReaderInterface",
            "WriterInterface",
            "ReaderWriterInterface",
            "ServerReader",
            "ServerWriter",
            "ServerReaderWriter",
        )
    )


_NON_OK_STATUS_NAMES = frozenset({
    "CANCELLED", "UNKNOWN", "INVALID_ARGUMENT", "DEADLINE_EXCEEDED",
    "NOT_FOUND", "ALREADY_EXISTS", "PERMISSION_DENIED", "RESOURCE_EXHAUSTED",
    "FAILED_PRECONDITION", "ABORTED", "OUT_OF_RANGE", "UNIMPLEMENTED",
    "INTERNAL", "UNAVAILABLE", "DATA_LOSS", "UNAUTHENTICATED",
})


def _is_status_qualtype(qtype: str) -> bool:
    return "Status" in qtype and "StatusCode" not in qtype


def _classify_status_expr(node: Optional[dict], status_vars: Dict[str, str]) -> str:
    """Classifies a grpc::Status AST expression into {'OK', 'non_ok', 'Unknown'}."""
    if not isinstance(node, dict):
        return "Unknown"
    kind = node.get("kind", "")

    if kind in (
        "ReturnStmt",
        "ExprWithCleanups",
        "ImplicitCastExpr",
        "ParenExpr",
        "MaterializeTemporaryExpr",
        "CXXBindTemporaryExpr",
        "CXXFunctionalCastExpr",
    ):
        inner = node.get("inner", [])
        if len(inner) == 1:
            return _classify_status_expr(inner[0], status_vars)
        if not inner:
            return "Unknown"
        # Single status return may wrap multiple temporaries; check direct sub-exprs consistently
        sub_vals = [_classify_status_expr(ch, status_vars) for ch in inner]
        if len(set(sub_vals)) == 1:
            return sub_vals[0]
        return "Unknown"

    if kind == "DeclRefExpr":
        ref = node.get("referencedDecl", {})
        name = ref.get("name") or node.get("name", "")
        qt = node.get("type", {}).get("qualType", "")
        ref_kind = ref.get("kind", "")
        if name == "OK" and ("Status" in qt or "StatusCode" in qt or ref_kind in ("EnumConstantDecl", "VarDecl")):
            return "OK"
        if ref_kind == "EnumConstantDecl" and "StatusCode" in qt and name != "OK":
            return "non_ok"
        if name in _NON_OK_STATUS_NAMES and ("Status" in qt or "StatusCode" in qt):
            return "non_ok"
        if name in status_vars:
            return status_vars[name]
        return "Unknown"

    if kind == "MemberExpr":
        mname = node.get("name", "")
        qt = node.get("type", {}).get("qualType", "")
        if mname == "OK" and ("Status" in qt or not qt):
            return "OK"
        if mname in _NON_OK_STATUS_NAMES and ("Status" in qt or not qt):
            return "non_ok"
        return "Unknown"

    if kind in ("CXXConstructExpr", "CXXTemporaryObjectExpr"):
        qt = node.get("type", {}).get("qualType", "")
        ch = node.get("inner", [])
        if "Status" in qt or not qt:
            if not ch:
                return "OK"
            return _classify_status_expr(ch[0], status_vars)
        return "Unknown"

    if kind in ("CallExpr", "CXXMemberCallExpr"):
        inner = node.get("inner", [])
        if inner:
            callee = inner[0]
            ckind = callee.get("kind", "")
            cname = callee.get("name") or callee.get("referencedDecl", {}).get("name", "")
            if ckind in ("DeclRefExpr", "MemberExpr") and cname == "OK":
                cqt = callee.get("type", {}).get("qualType", "")
                if "Status" in cqt or not cqt:
                    return "OK"
            if ckind in ("DeclRefExpr", "MemberExpr") and cname in _NON_OK_STATUS_NAMES:
                cqt = callee.get("type", {}).get("qualType", "")
                if "Status" in cqt or not cqt:
                    return "non_ok"
        return "Unknown"

    if kind == "ConditionalOperator":
        ch = node.get("inner", [])
        if len(ch) == 3:
            t_res = _classify_status_expr(ch[1], status_vars)
            f_res = _classify_status_expr(ch[2], status_vars)
            return t_res if t_res == f_res else "Unknown"
        return "Unknown"

    inner = node.get("inner", [])
    if len(inner) == 1:
        return _classify_status_expr(inner[0], status_vars)
    return "Unknown"


def decode_clang_json_stream(s: str) -> List[dict]:
    """Decodes one or more concatenated JSON objects emitted by clang -ast-dump=json."""
    decoder = json.JSONDecoder()
    idx = 0
    decls = []
    while idx < len(s):
        s_slice = s[idx:].lstrip()
        if not s_slice:
            break
        idx = len(s) - len(s_slice)
        try:
            obj, end_idx = decoder.raw_decode(s, idx)
            decls.append(obj)
            idx = end_idx
        except Exception:
            idx += 1
    return decls


def find_all_methods(nodes: List[dict], results: Optional[List[dict]] = None) -> List[dict]:
    """Recursively extracts all function/method/destructor AST nodes that have a CompoundStmt body."""
    if results is None:
        results = []
    for n in nodes:
        if not isinstance(n, dict):
            continue
        kind = n.get("kind", "")
        if kind in ("FunctionDecl", "CXXMethodDecl", "CXXDestructorDecl", "CXXConstructorDecl"):
            has_body = any(ch.get("kind") == "CompoundStmt" for ch in n.get("inner", []))
            if has_body:
                results.append(n)
        if "inner" in n and kind in ("TranslationUnitDecl", "NamespaceDecl", "CXXRecordDecl", "LinkageSpecDecl"):
            find_all_methods(n["inner"], results)
    return results


class ClangSessionChecker:
    """Path-sensitive structured AST verifier for gRPC streaming binary session contracts."""

    MAX_LOOP_STATES = 64

    def __init__(self, spec_path: Optional[Path] = None, spec_dict: Optional[dict] = None):
        if spec_dict is not None:
            self.spec = spec_dict
        elif spec_path is not None:
            with open(spec_path, "r", encoding="utf-8") as f:
                self.spec = json.load(f)
        else:
            raise ValueError("Either spec_path or spec_dict must be provided")

        self.protocol = self.spec["protocol"]
        self.method_names = set(self.protocol.get("method_aliases", [self.protocol["method"]]))
        self.method_names.add(self.protocol["method"])
        self.states = self.spec["states"]
        self.initial_state = self.spec["initial_state"]
        self.terminal_states = {
            name for name, data in self.states.items() if data.get("terminal", False)
        }

        self.payload_groups: Dict[str, Tuple[str, int]] = {}
        raw_groups = self.spec.get("payload_groups") or {}
        if raw_groups:
            for setter, info in raw_groups.items():
                self.payload_groups[setter] = (info["group"], int(info["case"]))
        elif "payload_setters" in self.spec:
            default_grp = self._infer_default_group()
            for setter, val in self.spec["payload_setters"].items():
                self.payload_groups[setter] = (default_grp, int(val))
        else:
            for sdata in self.states.values():
                for tr in sdata.get("transitions", []):
                    self._register_guard_setters(tr.get("guard"))

        self.payload_setters: Dict[str, int] = {
            k: v[1] for k, v in self.payload_groups.items()
        }
        self._group_names: Set[str] = {grp for grp, _ in self.payload_groups.values()}
        for sdata in self.states.values():
            for tr in sdata.get("transitions", []):
                g = tr.get("guard") or {}
                if g.get("field"):
                    self._group_names.add(g["field"])

        self.diagnostics: List[Tuple[str, str]] = []
        self._emitted_keys: Set[Tuple[int, str]] = set()
        self.incomplete_loops: int = 0
        self.max_loop_configs_explored: int = 0
        self.observed_server_eos_modes: Set[str] = set()
        self.is_default_level1: bool = bool(self.protocol.get("is_default_level1", False))
        self.conditional_verifications: int = 0
        self.unknown_status_returns: int = 0
        self.server_app_err_returns: int = 0

    def _default_recv_complete(self) -> str:
        if self.protocol.get("role", "client") == "server":
            return "Yes"
        if self.protocol.get("rpc_type") == "CLIENT_STREAMING":
            return "Yes"
        return "Unknown" if self.is_default_level1 else "Yes"

    def _infer_default_group(self) -> str:
        for sdata in self.states.values():
            for tr in sdata.get("transitions", []):
                g = tr.get("guard") or {}
                if g.get("field"):
                    return g["field"]
                for cl in g.get("clauses", []):
                    if cl.get("field"):
                        return cl["field"]
        return "payload"

    def _register_guard_setters(self, guard: Optional[dict]):
        if not guard:
            return
        if guard.get("type") == "all":
            for cl in guard.get("clauses", []):
                self._register_guard_setters(cl)
            return
        if guard.get("type") == "oneof_case" and guard.get("case_name") and guard.get("expected_case") is not None:
            grp = guard.get("field") or "payload"
            self.payload_groups[guard["case_name"]] = (grp, int(guard["expected_case"]))

    @classmethod
    def from_default_level1(cls, rpc_type: str, method_name: str, role: str = "client") -> "ClangSessionChecker":
        """Constructs the universal Level-1 gRPC Stream Automaton matching gRPC 1.72.0 sync_stream.h."""
        if role == "server":
            if rpc_type == "SERVER_STREAMING":
                spec = {
                    "schema_version": "grpc.session.v1",
                    "protocol": {"method": method_name, "rpc_type": "SERVER_STREAMING", "role": "server", "is_default_level1": True},
                    "payload_setters": {},
                    "initial_state": "S0_WRITING",
                    "states": {
                        "S0_WRITING": {
                            "terminal": False,
                            "transitions": [
                                {"op": "Write", "outcome": "ok", "target": "S0_WRITING", "guard": None},
                                {"op": "ServerReturn", "outcome": "OK", "target": "S1_FINISHED", "guard": None},
                            ],
                        },
                        "S1_FINISHED": {"terminal": True, "transitions": []},
                    },
                }
            elif rpc_type == "CLIENT_STREAMING":
                spec = {
                    "schema_version": "grpc.session.v1",
                    "protocol": {"method": method_name, "rpc_type": "CLIENT_STREAMING", "role": "server", "is_default_level1": True},
                    "payload_setters": {},
                    "initial_state": "S0_READING",
                    "states": {
                        "S0_READING": {
                            "terminal": False,
                            "transitions": [
                                {"op": "Read", "outcome": "ok", "target": "S0_READING", "guard": None},
                                {"op": "Read", "outcome": "eos", "target": "S1_INPUT_CLOSED", "guard": None},
                                {"op": "ServerReturn", "outcome": "OK", "target": "S2_FINISHED", "guard": None},
                            ],
                        },
                        "S1_INPUT_CLOSED": {
                            "terminal": False,
                            "transitions": [
                                {"op": "ServerReturn", "outcome": "OK", "target": "S2_FINISHED", "guard": None},
                            ],
                        },
                        "S2_FINISHED": {"terminal": True, "transitions": []},
                    },
                }
            else:
                spec = {
                    "schema_version": "grpc.session.v1",
                    "protocol": {"method": method_name, "rpc_type": "BIDIRECTIONAL_STREAMING", "role": "server", "is_default_level1": True},
                    "payload_setters": {},
                    "initial_state": "S0_OPEN",
                    "states": {
                        "S0_OPEN": {
                            "terminal": False,
                            "transitions": [
                                {"op": "Read", "outcome": "ok", "target": "S0_OPEN", "guard": None},
                                {"op": "Write", "outcome": "ok", "target": "S0_OPEN", "guard": None},
                                {"op": "Read", "outcome": "eos", "target": "S1_INPUT_CLOSED", "guard": None},
                                {"op": "ServerReturn", "outcome": "OK", "target": "S2_FINISHED", "guard": None},
                            ],
                        },
                        "S1_INPUT_CLOSED": {
                            "terminal": False,
                            "transitions": [
                                {"op": "Write", "outcome": "ok", "target": "S1_INPUT_CLOSED", "guard": None},
                                {"op": "ServerReturn", "outcome": "OK", "target": "S2_FINISHED", "guard": None},
                            ],
                        },
                        "S2_FINISHED": {"terminal": True, "transitions": []},
                    },
                }
            return cls(spec_dict=spec)

        if rpc_type == "SERVER_STREAMING":
            spec = {
                "schema_version": "grpc.session.v1",
                "protocol": {"method": method_name, "rpc_type": "SERVER_STREAMING", "role": "client", "is_default_level1": True},
                "payload_setters": {},
                "initial_state": "S0_READING",
                "states": {
                    "S0_READING": {
                        "terminal": False,
                        "transitions": [
                            {"op": "Read", "outcome": "ok", "target": "S0_READING", "guard": None},
                            {"op": "Read", "outcome": "eos", "target": "S1_EOS_AWAIT_FINISH", "guard": None},
                            {"op": "Finish", "outcome": "OK", "target": "S2_FINISHED", "guard": None},
                        ],
                    },
                    "S1_EOS_AWAIT_FINISH": {
                        "terminal": False,
                        "transitions": [
                            {"op": "Finish", "outcome": "OK", "target": "S2_FINISHED", "guard": None},
                        ],
                    },
                    "S2_FINISHED": {"terminal": True, "transitions": []},
                },
            }
        elif rpc_type == "CLIENT_STREAMING":
            spec = {
                "schema_version": "grpc.session.v1",
                "protocol": {"method": method_name, "rpc_type": "CLIENT_STREAMING", "role": "client", "is_default_level1": True},
                "payload_setters": {},
                "initial_state": "S0_WRITING",
                "states": {
                    "S0_WRITING": {
                        "terminal": False,
                        "transitions": [
                            {"op": "Write", "outcome": "ok", "target": "S0_WRITING", "guard": None},
                            {"op": "WritesDone", "outcome": "ok", "target": "S1_HALF_CLOSED", "guard": None},
                            {"op": "Finish", "outcome": "OK", "target": "S2_FINISHED", "guard": None},
                        ],
                    },
                    "S1_HALF_CLOSED": {
                        "terminal": False,
                        "transitions": [
                            {"op": "Finish", "outcome": "OK", "target": "S2_FINISHED", "guard": None},
                        ],
                    },
                    "S2_FINISHED": {"terminal": True, "transitions": []},
                },
            }
        else:
            spec = {
                "schema_version": "grpc.session.v1",
                "protocol": {"method": method_name, "rpc_type": "BIDIRECTIONAL_STREAMING", "role": "client", "is_default_level1": True},
                "payload_setters": {},
                "initial_state": "S0_OPEN",
                "states": {
                    "S0_OPEN": {
                        "terminal": False,
                        "transitions": [
                            {"op": "Write", "outcome": "ok", "target": "S0_OPEN", "guard": None},
                            {"op": "Read", "outcome": "ok", "target": "S0_OPEN", "guard": None},
                            {"op": "WritesDone", "outcome": "ok", "target": "S1_WRITE_CLOSED", "guard": None},
                            {"op": "Read", "outcome": "eos", "target": "S2_RESPONSE_EOS", "guard": None},
                            {"op": "Finish", "outcome": "OK", "target": "S3_FINISHED", "guard": None},
                        ],
                    },
                    "S1_WRITE_CLOSED": {
                        "terminal": False,
                        "transitions": [
                            {"op": "Read", "outcome": "ok", "target": "S1_WRITE_CLOSED", "guard": None},
                            {"op": "Read", "outcome": "eos", "target": "S2_RESPONSE_EOS", "guard": None},
                            {"op": "Finish", "outcome": "OK", "target": "S3_FINISHED", "guard": None},
                        ],
                    },
                    "S2_RESPONSE_EOS": {
                        "terminal": False,
                        "transitions": [
                            {"op": "Finish", "outcome": "OK", "target": "S3_FINISHED", "guard": None},
                        ],
                    },
                    "S3_FINISHED": {"terminal": True, "transitions": []},
                },
            }
        return cls(spec_dict=spec)

    def emit_diagnostic(self, file_path: str, line: int, col: int, severity: str, message: str):
        key = (line, message)
        if key in self._emitted_keys:
            return
        self._emitted_keys.add(key)

        diag = f"{file_path}:{line}:{col}: {severity}: [grpc-session] {message}"
        self.diagnostics.append((severity, diag))
        if severity == "error":
            print(f"\033[1;31m{diag}\033[0m")
        elif severity == "warning":
            print(f"\033[1;33m{diag}\033[0m")
        else:
            print(f"\033[1;32m{diag}\033[0m")

    def run_clang_ast_dump(self, cpp_file: Path, ast_filter: str = "Run",
                           extra_flags: Optional[List[str]] = None) -> List[dict]:
        """Runs clang++ with `-Xclang -ast-dump=json -fsyntax-only` to extract AST declarations."""
        clang_cmd = [
            "clang++",
            "--target=x86_64-w64-windows-gnu",
            "--sysroot=C:/msys64/ucrt64",
            "-DPROTOBUF_USE_DLLS",
            "-I", str(cpp_file.parent),
            "-I", str(cpp_file.parent.parent),
            "-I", r"C:\msys64\ucrt64\include",
        ]
        if extra_flags:
            clang_cmd.extend(extra_flags)
        else:
            clang_cmd.append("-std=c++17")
        clang_cmd.extend([
            "-Xclang", f"-ast-dump-filter={ast_filter}",
            "-Xclang", "-ast-dump=json",
            "-fsyntax-only",
            str(cpp_file),
        ])
        res = subprocess.run(clang_cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
        if res.returncode != 0 and not res.stdout:
            raise RuntimeError(f"Clang AST dump failed:\n{res.stderr}")
        return decode_clang_json_stream(res.stdout)

    def analyze_file(self, cpp_file: Path, extra_flags: Optional[List[str]] = None) -> bool:
        """Analyzes all matching client stubs or server handlers in a C++ translation unit."""
        self.diagnostics = []
        self._emitted_keys = set()
        self.incomplete_loops = 0
        self.max_loop_configs_explored = 0
        self.observed_server_eos_modes = set()
        self.conditional_verifications = 0
        self.unknown_status_returns = 0
        self.server_app_err_returns = 0
        role = self.protocol.get("role", "client")
        svc_name = self.protocol.get("service", "gRPC")
        print(f"\n[*] Analyzing C++ source: {cpp_file.name}")
        print(f"[*] Target gRPC Service: {svc_name}.{self.protocol['method']} (role={role})")

        decls = self.run_clang_ast_dump(cpp_file, extra_flags=extra_flags)

        target_funcs = []
        for d in find_all_methods(decls):
            loc = d.get("loc", {})
            file_included = loc.get("file")
            if file_included and not file_included.endswith(cpp_file.name):
                continue
            if role == "server":
                has_server_stream_param = any(
                    ch.get("kind") == "ParmVarDecl"
                    and any(st in ch.get("type", {}).get("qualType", "")
                            for st in ("ServerReader", "ServerWriter", "ServerReaderWriter"))
                    for ch in d.get("inner", [])
                )
                if has_server_stream_param:
                    target_funcs.append(d)
            else:
                if any(has_member_call(d, m) for m in self.method_names):
                    target_funcs.append(d)

        if not target_funcs:
            print(f"[-] No target analysis function found in {cpp_file.name}")
            return False

        for func in target_funcs:
            self._analyze_function(func, cpp_file)

        errors = [d for sev, d in self.diagnostics if sev == "error"]
        if not errors:
            print(f"\033[1;32m[+] PASSED: All protocol invariants statically verified for {cpp_file.name}\033[0m")
            return True
        print(f"\033[1;31m[-] FAILED: Detected {len(errors)} protocol violation(s) in {cpp_file.name}\033[0m")
        return False

    def analyze_real_methods(self, cpp_file: Path, methods: List[dict],
                             initial_stream_state: Optional[str] = None) -> bool:
        """Analyzes a sequence of C++ methods across an object lifecycle."""
        self.diagnostics = []
        self._emitted_keys = set()
        self.incomplete_loops = 0
        self.max_loop_configs_explored = 0
        self.observed_server_eos_modes = set()
        self.conditional_verifications = 0
        self.unknown_status_returns = 0
        self.server_app_err_returns = 0

        frontier = [AbstractState()]
        if initial_stream_state:
            frontier[0].stream_states["stream"] = initial_stream_state
            frontier[0].recv_complete["stream"] = self._default_recv_complete()

        for idx, func in enumerate(methods):
            is_last = (idx == len(methods) - 1)
            frontier = self._analyze_function(
                func, cpp_file,
                shared_frontier=frontier,
                check_end_of_func=is_last,
            )

        errors = [d for sev, d in self.diagnostics if sev == "error"]
        return len(errors) == 0

    def _collect_typed_stream_vars(self, node: dict, out: Optional[Set[str]] = None) -> Set[str]:
        if out is None:
            out = set()
        if not isinstance(node, dict):
            return out
        kind = node.get("kind", "")
        if kind in ("VarDecl", "ParmVarDecl", "MemberExpr"):
            qt = node.get("type", {}).get("qualType", "")
            name = node.get("name")
            if name and _is_stream_qualtype(qt):
                out.add(name)
        for ch in node.get("inner", []):
            self._collect_typed_stream_vars(ch, out)
        return out

    def _analyze_function(self, func: dict, cpp_file: Path,
                          shared_frontier: Optional[List[AbstractState]] = None,
                          check_end_of_func: bool = True) -> List[AbstractState]:
        func_line = func.get("loc", {}).get("line") or self._get_line(func, 1)
        frontier = shared_frontier if shared_frontier is not None else [AbstractState()]
        known_stream_vars = self._collect_typed_stream_vars(func)

        if self.protocol.get("role") == "server":
            server_params = []
            for ch in func.get("inner", []):
                if ch.get("kind") == "ParmVarDecl":
                    qtype = ch.get("type", {}).get("qualType", "")
                    if any(st in qtype for st in ("ServerReader", "ServerWriter", "ServerReaderWriter")):
                        server_params.append(ch.get("name") or "stream")
            for st in frontier:
                for pname in server_params:
                    st.stream_states.setdefault(pname, self.initial_state)
                    st.recv_complete.setdefault(pname, self._default_recv_complete())
                if not server_params and not st.stream_states:
                    st.stream_states["stream"] = self.initial_state
                    st.recv_complete["stream"] = self._default_recv_complete()

        body = next((inn for inn in func.get("inner", []) if inn.get("kind") == "CompoundStmt"), None)
        if not body:
            return frontier

        res = self._walk_stmts(
            body.get("inner", []),
            cpp_file,
            frontier,
            check_return_termination=check_end_of_func,
            transport_abort_ctx=False,
            known_stream_vars=known_stream_vars,
        )

        if check_end_of_func and not res.terminated:
            self._check_terminal_frontier(res.fallthrough, cpp_file, func_line, "at end of function")

        combined_exits = res.return_exits + ([] if res.terminated else res.fallthrough)
        return dedup_states(combined_exits) if combined_exits else dedup_states(frontier)

    def _check_terminal_frontier(self, frontier: List[AbstractState], cpp_file: Path, line: int, context_desc: str):
        for st in frontier:
            for stream_var, current_state in list(st.stream_states.items()):
                if current_state not in self.terminal_states:
                    self.emit_diagnostic(
                        cpp_file.name, line, 1 if context_desc == "at end of function" else 9, "error",
                        f"Stream lifecycle leak (Level 1 termination): Stream '{stream_var}' destroyed {context_desc} "
                        f"in non-terminal state '{current_state}'. Expected terminal state {sorted(self.terminal_states)} "
                        f"(missing WritesDone() and Finish()). Server HTTP/2 resources will leak.",
                    )

    def _apply_server_eos_refinement(self, states: List[AbstractState], mode: str):
        updated = False
        for st in states:
            for svar in list(st.server_eos_mode.keys()):
                st.server_eos_mode[svar] = mode
                updated = True
        if updated:
            self.observed_server_eos_modes.add(mode)

    def _apply_read_eos(self, state: AbstractState, stream_var: Optional[str], cpp_file: Path, line: int):
        """Advances active stream handle(s) on `Read() == false` (eos) when an eos transition exists."""
        targets = [stream_var] if (stream_var and stream_var in state.stream_states) else list(state.stream_states.keys())
        is_server = (self.protocol.get("role") == "server")
        for svar in targets:
            cur = state.stream_states.get(svar, self.initial_state)
            state_data = self.states.get(cur, {})
            transitions = state_data.get("transitions", [])
            eos_trans = [
                t for t in transitions
                if t["op"] == "Read" and t.get("outcome") == "eos"
            ]
            if eos_trans:
                state.stream_states[svar] = eos_trans[0]["target"]
                state.eos_observed_streams.add(svar)
                state.recv_complete[svar] = "Yes"
            elif any(t["op"] == "Read" for t in transitions):
                # Read()==false in a single-message Read state (e.g. ?Ack; end) permits direct Finish()
                state.eos_observed_streams.add(svar)
                state.recv_complete[svar] = "Yes"
            if is_server:
                state.server_eos_mode[svar] = "ambiguous"
                self.observed_server_eos_modes.add("ambiguous")

    def _is_op_enabled_in_state(self, state: AbstractState, svar: Optional[str], op_name: str) -> bool:
        """Checks Level-1 precondition: op is legal in the receiver's current DFSM state."""
        target_svar = svar if (svar and svar in state.stream_states) else next(iter(state.stream_states.keys()), None)
        if target_svar is None:
            return False
        if op_name in ("Write", "WriteLast") and target_svar in state.write_last_streams:
            return False
        check_op = "Write" if op_name == "WriteLast" else op_name
        cur = state.stream_states.get(target_svar, self.initial_state)
        transitions = self.states.get(cur, {}).get("transitions", [])
        return any(t["op"] == check_op for t in transitions)

    def _classify_io_abort(self, cond: dict, state: AbstractState) -> Tuple[Optional[str], bool, bool]:
        """
        Inspects condition provenance across four outcomes after checking operation preconditions:
        - Server `IsCancelled()` check (`!IsCancelled()` excluded) -> `(IsCancelled, True, True)` on server.
        - Negated `Write` / `WriteLast` / `WritesDone` (when legal in `state`) -> `(op, True, True)`.
        - Negated `Read` (when legal in `state`):
          - Client: `(Read, False, True)` -> allows Finish() inside then-branch while requiring Finish().
          - Server: `(Read, False, False)` -> ambiguous EOS until IsCancelled() checked.
        """
        is_server = (self.protocol.get("role") == "server")
        if is_server and not is_negated_expr(cond) and has_member_call(cond, "IsCancelled"):
            return ("IsCancelled", True, True)

        neg_target = _extract_negated_target(cond)
        if neg_target is None:
            return (None, False, False)

        direct_io = find_member_call(neg_target, ("Write", "WriteLast", "WritesDone", "Read"))
        if direct_io is not None:
            svar, op = direct_io
            if not self._is_op_enabled_in_state(state, svar, op):
                return (None, False, False)
            if op in ("Write", "WriteLast", "WritesDone"):
                return (op, True, True)
            return (op, False, not is_server)

        var_name = find_decl_ref_name(neg_target)
        if var_name and var_name in state.bool_prov:
            svar, op, _ = state.bool_prov[var_name]
            if not self._is_op_enabled_in_state(state, svar, op):
                return (None, False, False)
            if op in ("Write", "WriteLast", "WritesDone"):
                return (op, True, True)
            return (op, False, not is_server)

        return (None, False, False)

    def _walk_stmts(
        self,
        stmts: List[dict],
        cpp_file: Path,
        frontier: List[AbstractState],
        check_return_termination: bool = True,
        transport_abort_ctx: bool = False,
        allow_abort_finish_ctx: bool = False,
        known_stream_vars: Optional[Set[str]] = None,
        throw_collect: Optional[List[AbstractState]] = None,
    ) -> WalkResult:
        """
        Structured AST control-flow interpreter returning separated fallthrough,
        return, break, and continue state channels.
        """
        cur_frontier = dedup_states(frontier)
        return_exits: List[AbstractState] = []
        break_exits: List[AbstractState] = []
        continue_exits: List[AbstractState] = []

        for stmt in stmts:
            if not cur_frontier:
                return WalkResult([], True, return_exits, break_exits, continue_exits)

            kind = stmt.get("kind", "")
            line = self._get_line(stmt)

            if kind == "CompoundStmt":
                sub = self._walk_stmts(
                    stmt.get("inner", []), cpp_file, cur_frontier,
                    check_return_termination=check_return_termination,
                    transport_abort_ctx=transport_abort_ctx,
                    allow_abort_finish_ctx=allow_abort_finish_ctx,
                    known_stream_vars=known_stream_vars,
                    throw_collect=throw_collect,
                )
                return_exits.extend(sub.return_exits)
                break_exits.extend(sub.break_exits)
                continue_exits.extend(sub.continue_exits)
                if sub.terminated:
                    return WalkResult([], True, return_exits, break_exits, continue_exits)
                cur_frontier = sub.fallthrough
                continue

            if kind == "CXXTryStmt":
                try_body = next((ch for ch in stmt.get("inner", []) if ch.get("kind") == "CompoundStmt"), None)
                catches = [ch for ch in stmt.get("inner", []) if ch.get("kind") == "CXXCatchStmt"]
                entry_states = [s.clone() for s in cur_frontier]
                try_intermediate: List[AbstractState] = [s.clone() for s in entry_states]
                surviving: List[AbstractState] = []
                if try_body:
                    tb = self._walk_stmts(
                        try_body.get("inner", []), cpp_file, [s.clone() for s in entry_states],
                        check_return_termination=check_return_termination,
                        transport_abort_ctx=transport_abort_ctx,
                        allow_abort_finish_ctx=allow_abort_finish_ctx,
                        known_stream_vars=known_stream_vars,
                        throw_collect=try_intermediate,
                    )
                    return_exits.extend(tb.return_exits)
                    break_exits.extend(tb.break_exits)
                    continue_exits.extend(tb.continue_exits)
                    if not tb.terminated:
                        surviving.extend(tb.fallthrough)
                catch_entry = dedup_states(try_intermediate)
                for c_node in catches:
                    c_body = next((ch for ch in c_node.get("inner", []) if ch.get("kind") == "CompoundStmt"), None)
                    if c_body:
                        cb = self._walk_stmts(
                            c_body.get("inner", []), cpp_file, [s.clone() for s in catch_entry],
                            check_return_termination=check_return_termination,
                            transport_abort_ctx=transport_abort_ctx,
                            allow_abort_finish_ctx=allow_abort_finish_ctx,
                            known_stream_vars=known_stream_vars,
                            throw_collect=throw_collect,
                        )
                        return_exits.extend(cb.return_exits)
                        break_exits.extend(cb.break_exits)
                        continue_exits.extend(cb.continue_exits)
                        if not cb.terminated:
                            surviving.extend(cb.fallthrough)
                cur_frontier = dedup_states(surviving)
                if not cur_frontier:
                    return WalkResult([], True, return_exits, break_exits, continue_exits)
                continue

            if kind == "DoStmt":
                cur_frontier, d_rets = self._handle_do_stmt(
                    stmt, cpp_file, cur_frontier, check_return_termination, known_stream_vars, throw_collect
                )
                return_exits.extend(d_rets)
                continue

            if kind == "DeclStmt":
                if throw_collect is not None and has_any_call(stmt):
                    throw_collect.extend(s.clone() for s in cur_frontier)
                for st in cur_frontier:
                    self._handle_decl_stmt(
                        stmt, st, line, cpp_file,
                        allow_abort_finish=allow_abort_finish_ctx,
                        known_stream_vars=known_stream_vars,
                    )
                continue

            if kind == "IfStmt":
                if_res = self._handle_if_stmt(
                    stmt, cpp_file, cur_frontier, check_return_termination,
                    transport_abort_ctx, allow_abort_finish_ctx, known_stream_vars, throw_collect,
                )
                return_exits.extend(if_res.return_exits)
                break_exits.extend(if_res.break_exits)
                continue_exits.extend(if_res.continue_exits)
                if if_res.terminated:
                    return WalkResult([], True, return_exits, break_exits, continue_exits)
                cur_frontier = if_res.fallthrough
                continue

            if kind == "ReturnStmt":
                for st in cur_frontier:
                    self._scan_expr_calls(
                        stmt, cpp_file, st, line,
                        allow_abort_finish=allow_abort_finish_ctx,
                        known_stream_vars=known_stream_vars,
                    )
                if check_return_termination and not transport_abort_ctx:
                    if self.protocol.get("role") == "server":
                        for st in cur_frontier:
                            ret_status = _classify_status_expr(stmt, st.status_vars)
                            if ret_status == "non_ok":
                                self.server_app_err_returns += 1
                            elif ret_status == "Unknown":
                                self.unknown_status_returns += 1
                                self.emit_diagnostic(
                                    cpp_file.name, line, 9, "warning",
                                    "STATUS_UNKNOWN: Server handler returns unresolved grpc::Status "
                                    "(neither statically known Status::OK nor F_app_err); stream completion "
                                    "cannot be unconditionally classified as NormalCompletion or early error exit.",
                                )
                            else:
                                for svar in list(st.stream_states.keys()):
                                    cur_st = st.stream_states[svar]
                                    if cur_st not in self.terminal_states:
                                        self._check_simple_op(
                                            cpp_file, svar, cur_st, "ServerReturn", st, line,
                                            allow_abort_finish=False, outcome="OK",
                                        )
                    else:
                        self._check_terminal_frontier(cur_frontier, cpp_file, line, "on return")
                if not transport_abort_ctx:
                    return_exits.extend(s.clone() for s in cur_frontier)
                return WalkResult([], True, return_exits, break_exits, continue_exits)

            if kind == "BreakStmt":
                break_exits.extend(s.clone() for s in cur_frontier)
                return WalkResult([], True, return_exits, break_exits, continue_exits)

            if kind == "ContinueStmt":
                continue_exits.extend(s.clone() for s in cur_frontier)
                return WalkResult([], True, return_exits, break_exits, continue_exits)

            if kind in ("ForStmt", "WhileStmt", "CXXForRangeStmt"):
                cur_frontier, loop_rets = self._handle_loop_stmt(
                    stmt, kind, cpp_file, cur_frontier, check_return_termination, known_stream_vars, throw_collect
                )
                return_exits.extend(loop_rets)
                continue

            if throw_collect is not None and has_any_call(stmt):
                throw_collect.extend(s.clone() for s in cur_frontier)

            for st in cur_frontier:
                self._scan_expr_calls(
                    stmt, cpp_file, st, line,
                    allow_abort_finish=allow_abort_finish_ctx,
                    known_stream_vars=known_stream_vars,
                )
            cur_frontier = dedup_states(cur_frontier)

        return WalkResult(cur_frontier, False, return_exits, break_exits, continue_exits)

    def _is_server_non_ok_return(self, return_stmt: dict) -> bool:
        if self.protocol.get("role") != "server":
            return False
        return _classify_status_expr(return_stmt, {}) == "non_ok"

    def _handle_decl_stmt(self, decl_stmt: dict, state: AbstractState, line: int,
                          cpp_file: Path, allow_abort_finish: bool = False,
                          known_stream_vars: Optional[Set[str]] = None):
        for ch in decl_stmt.get("inner", []):
            if ch.get("kind") == "VarDecl":
                vname = ch.get("name")
                qtype = ch.get("type", {}).get("qualType", "")
                if vname and known_stream_vars is not None and _is_stream_qualtype(qtype):
                    known_stream_vars.add(vname)
                if vname and _has_nullptr_literal(ch):
                    state.ptr_null_streams.add(vname)
                if vname and "WriteOptions" in qtype:
                    if has_member_call(ch, "set_last_message"):
                        state.last_msg_options.add(vname)
                    elif ch.get("inner"):
                        state.last_msg_options.discard(vname)
                if vname and _is_status_qualtype(qtype):
                    init_expr = ch["inner"][0] if ch.get("inner") else None
                    state.status_vars[vname] = (
                        _classify_status_expr(init_expr, state.status_vars) if init_expr is not None else "OK"
                    )
                io_hit = find_member_call(ch, ("Write", "WriteLast", "Read", "WritesDone"))
                if vname and io_hit is not None:
                    site_id = str(ch.get("id") or f"decl:{line}:{vname}")
                    svar = io_hit[0] or next(iter(state.stream_states.keys()), "stream")
                    state.bool_prov[vname] = (svar, io_hit[1], site_id)
                elif vname and ch.get("inner"):
                    state.bool_prov.pop(vname, None)
        self._scan_expr_calls(
            decl_stmt, cpp_file, state, line,
            allow_abort_finish=allow_abort_finish,
            known_stream_vars=known_stream_vars,
        )

    def _handle_if_stmt(
        self,
        if_stmt: dict,
        cpp_file: Path,
        frontier: List[AbstractState],
        check_return_termination: bool,
        transport_abort_ctx: bool,
        allow_abort_finish_ctx: bool = False,
        known_stream_vars: Optional[Set[str]] = None,
        throw_collect: Optional[List[AbstractState]] = None,
    ) -> WalkResult:
        children = if_stmt.get("inner", [])
        if not children:
            return WalkResult(frontier, False)
        cond = children[0]
        then_node = children[1] if len(children) > 1 else None
        else_node = children[2] if len(children) > 2 else None
        line = self._get_line(if_stmt)
        is_server = (self.protocol.get("role") == "server")

        # Case A: Stream smart-pointer / pointer nullability check (`if (stream)` / `if (!stream)`).
        if (
            not has_member_call(cond, "Write")
            and not has_member_call(cond, "Read")
            and not has_member_call(cond, "WritesDone")
            and (has_member_call(cond, "operator bool") or "operator bool" in json.dumps(cond))
        ):
            cond_var = find_decl_ref_name(cond)
            negated = is_negated_expr(cond)
            if not negated and then_node:
                non_null = [st for st in frontier if not (cond_var and cond_var in st.ptr_null_streams)]
                already_null = [st for st in frontier if (cond_var and cond_var in st.ptr_null_streams)]
                then_stmts = [then_node] if then_node.get("kind") != "CompoundStmt" else then_node.get("inner", [])
                if non_null:
                    t_res = self._walk_stmts(
                        then_stmts, cpp_file, [s.clone() for s in non_null],
                        check_return_termination=check_return_termination,
                        transport_abort_ctx=transport_abort_ctx,
                        allow_abort_finish_ctx=allow_abort_finish_ctx,
                        known_stream_vars=known_stream_vars,
                        throw_collect=throw_collect,
                    )
                else:
                    t_res = WalkResult([], True)
                surviving = already_null + ([] if t_res.terminated else t_res.fallthrough)
                return WalkResult(
                    dedup_states(surviving) if surviving else [],
                    len(surviving) == 0,
                    t_res.return_exits,
                    t_res.break_exits,
                    t_res.continue_exits,
                )
            return WalkResult(frontier, False)

        # Determine whether cond checks IsCancelled() on server
        has_is_cancelled = is_server and has_member_call(cond, "IsCancelled")
        is_neg_cancel = has_is_cancelled and is_negated_expr(cond)
        is_pos_cancel = has_is_cancelled and not is_neg_cancel

        # Determine direct Read() vs negated !Read() in condition
        cond_read_hit = find_member_call(cond, ("Read",))
        neg_target = _extract_negated_target(cond)
        neg_read_hit = find_member_call(neg_target, ("Read",)) if neg_target is not None else None
        is_neg_read_cond = (neg_read_hit is not None)
        is_pos_read_cond = (cond_read_hit is not None and not is_neg_read_cond)

        then_write_abort: List[AbstractState] = []
        then_eos_finish: List[AbstractState] = []
        then_normal: List[AbstractState] = []
        else_states: List[AbstractState] = []
        else_eos_finish = allow_abort_finish_ctx

        for st in frontier:
            abort_op, is_write_abort, is_eos_finish = self._classify_io_abort(cond, st)

            # Evaluate condition for THEN branch
            st_then = st.clone()
            if is_neg_read_cond:
                self._apply_read_eos(st_then, neg_read_hit[0], cpp_file, line)
            else:
                self._scan_expr_calls(
                    cond, cpp_file, st_then, line,
                    allow_abort_finish=allow_abort_finish_ctx,
                    known_stream_vars=known_stream_vars,
                )
            if is_pos_cancel:
                self._apply_server_eos_refinement([st_then], "confirmed_cancel")
            elif is_neg_cancel:
                self._apply_server_eos_refinement([st_then], "confirmed_norm")

            if is_write_abort:
                then_write_abort.append(st_then)
            elif is_eos_finish:
                then_eos_finish.append(st_then)
            else:
                then_normal.append(st_then)

            # Evaluate condition for ELSE / fallthrough branch
            st_else = st.clone()
            if is_pos_read_cond:
                self._apply_read_eos(st_else, cond_read_hit[0], cpp_file, line)
                if not is_server:
                    else_eos_finish = True
            elif is_neg_read_cond:
                self._scan_expr_calls(
                    cond, cpp_file, st_else, line,
                    allow_abort_finish=allow_abort_finish_ctx,
                    known_stream_vars=known_stream_vars,
                )
            else:
                self._scan_expr_calls(
                    cond, cpp_file, st_else, line,
                    allow_abort_finish=allow_abort_finish_ctx,
                    known_stream_vars=known_stream_vars,
                )
            if is_pos_cancel:
                self._apply_server_eos_refinement([st_else], "confirmed_norm")
            elif is_neg_cancel:
                self._apply_server_eos_refinement([st_else], "confirmed_cancel")
            else_states.append(st_else)

        then_stmts = (
            [then_node] if (then_node and then_node.get("kind") != "CompoundStmt")
            else (then_node.get("inner", []) if then_node else [])
        )
        else_stmts = (
            [else_node] if (else_node and else_node.get("kind") != "CompoundStmt")
            else (else_node.get("inner", []) if else_node else [])
        )

        then_surviving: List[AbstractState] = []
        collected_rets: List[AbstractState] = []
        collected_breaks: List[AbstractState] = []
        collected_conts: List[AbstractState] = []
        then_all_term = bool(then_node)

        if then_node:
            sub_runs = []
            if then_write_abort:
                sub_runs.append(self._walk_stmts(
                    then_stmts, cpp_file, then_write_abort,
                    check_return_termination=check_return_termination,
                    transport_abort_ctx=True,
                    allow_abort_finish_ctx=True,
                    known_stream_vars=known_stream_vars,
                    throw_collect=throw_collect,
                ))
            if then_eos_finish:
                sub_runs.append(self._walk_stmts(
                    then_stmts, cpp_file, then_eos_finish,
                    check_return_termination=check_return_termination,
                    transport_abort_ctx=transport_abort_ctx,
                    allow_abort_finish_ctx=True,
                    known_stream_vars=known_stream_vars,
                    throw_collect=throw_collect,
                ))
            if then_normal:
                sub_runs.append(self._walk_stmts(
                    then_stmts, cpp_file, then_normal,
                    check_return_termination=check_return_termination,
                    transport_abort_ctx=transport_abort_ctx,
                    allow_abort_finish_ctx=allow_abort_finish_ctx,
                    known_stream_vars=known_stream_vars,
                    throw_collect=throw_collect,
                ))
            for sr in sub_runs:
                collected_rets.extend(sr.return_exits)
                collected_breaks.extend(sr.break_exits)
                collected_conts.extend(sr.continue_exits)
                if not sr.terminated:
                    then_all_term = False
                    then_surviving.extend(sr.fallthrough)
        else:
            then_all_term = False

        else_surviving: List[AbstractState] = []
        else_all_term = False
        if else_node:
            else_transport_abort = transport_abort_ctx or is_neg_cancel
            el_res = self._walk_stmts(
                else_stmts, cpp_file, [s.clone() for s in else_states],
                check_return_termination=check_return_termination,
                transport_abort_ctx=else_transport_abort,
                allow_abort_finish_ctx=else_eos_finish,
                known_stream_vars=known_stream_vars,
                throw_collect=throw_collect,
            )
            collected_rets.extend(el_res.return_exits)
            collected_breaks.extend(el_res.break_exits)
            collected_conts.extend(el_res.continue_exits)
            else_all_term = el_res.terminated
            if not el_res.terminated:
                else_surviving.extend(el_res.fallthrough)
        else:
            else_surviving.extend(s.clone() for s in else_states)

        merged = dedup_states(then_surviving + else_surviving)
        both_terminated = then_all_term and else_all_term and len(merged) == 0
        return WalkResult(merged, both_terminated, collected_rets, collected_breaks, collected_conts)

    def _handle_do_stmt(self, do_stmt: dict, cpp_file: Path,
                        frontier: List[AbstractState], check_return_termination: bool,
                        known_stream_vars: Optional[Set[str]] = None,
                        throw_collect: Optional[List[AbstractState]] = None) -> Tuple[List[AbstractState], List[AbstractState]]:
        inner = do_stmt.get("inner", [])
        body_node = inner[0] if inner else None
        cond_node = inner[1] if len(inner) > 1 else None
        is_macro_do_while_false = is_false_literal_expr(cond_node)

        if not body_node or body_node.get("kind") != "CompoundStmt":
            for st in frontier:
                for ch in inner:
                    self._scan_expr_calls(ch, cpp_file, st, self._get_line(do_stmt), known_stream_vars=known_stream_vars)
            return (dedup_states(frontier), [])

        loop_stmts = body_node.get("inner", [])
        if is_macro_do_while_false:
            res = self._walk_stmts(
                loop_stmts, cpp_file, frontier,
                check_return_termination=check_return_termination,
                known_stream_vars=known_stream_vars,
                throw_collect=throw_collect,
            )
            exits = res.break_exits + ([] if res.terminated else res.fallthrough)
            return (dedup_states(exits if exits else frontier), res.return_exits)

        return self._run_fixed_point_loop(
            loop_stmts=loop_stmts,
            cond_nodes=[cond_node] if cond_node else [],
            frontier=frontier,
            cpp_file=cpp_file,
            include_zero_trip=False,
            cond_before_body=False,
            check_return_termination=check_return_termination,
            loop_line=self._get_line(do_stmt),
            known_stream_vars=known_stream_vars,
            throw_collect=throw_collect,
        )

    def _handle_loop_stmt(self, loop_stmt: dict, kind: str, cpp_file: Path,
                          frontier: List[AbstractState], check_return_termination: bool,
                          known_stream_vars: Optional[Set[str]] = None,
                          throw_collect: Optional[List[AbstractState]] = None) -> Tuple[List[AbstractState], List[AbstractState]]:
        children = loop_stmt.get("inner", [])
        body_node = next((ch for ch in children if ch.get("kind") == "CompoundStmt"), None)
        cond_nodes = [ch for ch in children if ch.get("kind") != "CompoundStmt"]
        loop_stmts = body_node.get("inner", []) if body_node else []

        include_zero = kind in ("WhileStmt", "ForStmt", "CXXForRangeStmt")
        return self._run_fixed_point_loop(
            loop_stmts=loop_stmts,
            cond_nodes=cond_nodes,
            frontier=frontier,
            cpp_file=cpp_file,
            include_zero_trip=include_zero,
            cond_before_body=True,
            check_return_termination=check_return_termination,
            loop_line=self._get_line(loop_stmt),
            known_stream_vars=known_stream_vars,
            throw_collect=throw_collect,
        )

    def _run_fixed_point_loop(
        self,
        loop_stmts: List[dict],
        cond_nodes: List[dict],
        frontier: List[AbstractState],
        cpp_file: Path,
        include_zero_trip: bool,
        cond_before_body: bool,
        check_return_termination: bool,
        loop_line: int = 1,
        known_stream_vars: Optional[Set[str]] = None,
        throw_collect: Optional[List[AbstractState]] = None,
    ) -> Tuple[List[AbstractState], List[AbstractState]]:
        """
        Monotone worklist fixed-point over finite correlated AbstractStates.
        Connects `continue` paths back to loop header, merges `break` paths to exit
        (without falsely triggering Read(eos)), applies Read(eos) on condition-exhausted exit
        of `while (Read(...))` loops, and emits `ANALYSIS_INCOMPLETE` if MAX_LOOP_STATES is exhausted.
        """
        visited_headers: Set[tuple] = set()
        worklist: List[AbstractState] = [s.clone() for s in frontier]
        loop_read_hit = None
        for cn in cond_nodes:
            loop_read_hit = find_member_call(cn, ("Read",))
            if loop_read_hit is not None:
                break

        exit_states: List[AbstractState] = []
        if include_zero_trip:
            for s in frontier:
                s0 = s.clone()
                if cond_before_body and loop_read_hit is not None:
                    self._apply_read_eos(s0, loop_read_hit[0], cpp_file, loop_line)
                exit_states.append(s0)

        loop_returns: List[AbstractState] = []
        errs_start = len(self.diagnostics)

        while worklist and len(visited_headers) < self.MAX_LOOP_STATES:
            cur = worklist.pop(0)
            key = cur.freeze_key()
            if key in visited_headers:
                continue
            visited_headers.add(key)

            step_list = [cur.clone()]
            if cond_before_body and cond_nodes:
                for st in step_list:
                    for cn in cond_nodes:
                        self._scan_expr_calls(cn, cpp_file, st, self._get_line(cn), known_stream_vars=known_stream_vars)

            if loop_stmts:
                b_res = self._walk_stmts(
                    loop_stmts, cpp_file, step_list,
                    check_return_termination=check_return_termination,
                    known_stream_vars=known_stream_vars,
                    throw_collect=throw_collect,
                )
                loop_returns.extend(b_res.return_exits)
                # Early break exits do NOT represent Read() returning false (eos)
                exit_states.extend(b_res.break_exits)
                iter_survivors = ([] if b_res.terminated else b_res.fallthrough) + b_res.continue_exits
            else:
                iter_survivors = step_list

            if not cond_before_body and cond_nodes:
                for st in iter_survivors:
                    for cn in cond_nodes:
                        self._scan_expr_calls(cn, cpp_file, st, self._get_line(cn), known_stream_vars=known_stream_vars)

            for post_st in iter_survivors:
                if loop_read_hit is not None:
                    eos_exit = post_st.clone()
                    self._apply_read_eos(eos_exit, loop_read_hit[0], cpp_file, loop_line)
                    exit_states.append(eos_exit)
                else:
                    exit_states.append(post_st.clone())
                if len(self.diagnostics) == errs_start and post_st.freeze_key() not in visited_headers:
                    worklist.append(post_st.clone())

        self.max_loop_configs_explored = max(self.max_loop_configs_explored, len(visited_headers))
        if worklist and any(st.freeze_key() not in visited_headers for st in worklist):
            self.incomplete_loops += 1
            self.emit_diagnostic(
                cpp_file.name, loop_line, 9, "warning",
                f"ANALYSIS_INCOMPLETE: Loop state exploration exceeded MAX_LOOP_STATES ({self.MAX_LOOP_STATES}).",
            )

        if not exit_states:
            return (dedup_states(frontier), loop_returns)
        return (dedup_states(exit_states), loop_returns)

    def _scan_expr_calls(self, node: dict, cpp_file: Path, state: AbstractState,
                         line: int, allow_abort_finish: bool = False,
                         known_stream_vars: Optional[Set[str]] = None):
        if not isinstance(node, dict):
            return
        kind = node.get("kind", "")
        node_line = self._get_line(node, line)

        if kind == "LambdaExpr":
            lambda_owns_stream = (len(state.stream_states) == 0)
            body = next((ch for ch in node.get("inner", []) if ch.get("kind") == "CompoundStmt"), None)
            if body:
                lambda_init = AbstractState() if lambda_owns_stream else state
                l_res = self._walk_stmts(
                    body.get("inner", []), cpp_file, [lambda_init],
                    check_return_termination=lambda_owns_stream,
                    transport_abort_ctx=allow_abort_finish,
                    allow_abort_finish_ctx=allow_abort_finish,
                    known_stream_vars=known_stream_vars,
                )
                if lambda_owns_stream and not l_res.terminated:
                    self._check_terminal_frontier(l_res.fallthrough, cpp_file, node_line, "at end of lambda")
            return

        if kind == "CXXThrowExpr":
            for stream_var, current_state in list(state.stream_states.items()):
                if current_state not in self.terminal_states:
                    self.emit_diagnostic(
                        cpp_file.name, node_line, 9, "error",
                        f"Stream lifecycle leak (Level 1 exception unwinding): Stream '{stream_var}' destroyed by uncaught throw "
                        f"in non-terminal state '{current_state}'. Expected terminal state {sorted(self.terminal_states)} "
                        f"(missing Finish()). Server HTTP/2 resources will leak.",
                    )
            return

        if kind == "BinaryOperator" and node.get("opcode") == "=":
            ch = node.get("inner", [])
            if len(ch) == 2:
                lhs_var = find_decl_ref_name(ch[0])
                lhs_qt = ch[0].get("type", {}).get("qualType", "")
                io_hit = find_member_call(ch[1], ("Write", "WriteLast", "Read", "WritesDone"))
                if lhs_var:
                    if io_hit is not None:
                        site_id = str(node.get("id") or f"assign:{node_line}:{lhs_var}")
                        svar = io_hit[0] or next(iter(state.stream_states.keys()), "stream")
                        state.bool_prov[lhs_var] = (svar, io_hit[1], site_id)
                    else:
                        state.bool_prov.pop(lhs_var, None)
                    if _has_nullptr_literal(ch[1]):
                        state.ptr_null_streams.add(lhs_var)
                    elif find_member_call(ch[1], tuple(self.method_names)) is not None:
                        state.ptr_null_streams.discard(lhs_var)
                    if _is_status_qualtype(lhs_qt) or lhs_var in state.status_vars:
                        state.status_vars[lhs_var] = _classify_status_expr(ch[1], state.status_vars)

        if kind == "CXXOperatorCallExpr":
            ch = node.get("inner", [])
            if (
                len(ch) == 3
                and ch[0].get("kind") == "DeclRefExpr"
                and (ch[0].get("referencedDecl", {}).get("name") == "operator=" or ch[0].get("name") == "operator=")
            ):
                lhs_var = find_decl_ref_name(ch[1])
                lhs_qt = ch[1].get("type", {}).get("qualType", "")
                if lhs_var and (_is_status_qualtype(lhs_qt) or lhs_var in state.status_vars):
                    state.status_vars[lhs_var] = _classify_status_expr(ch[2], state.status_vars)
                elif lhs_var and "WriteOptions" in lhs_qt:
                    if has_member_call(ch[2], "set_last_message"):
                        state.last_msg_options.add(lhs_var)
                    else:
                        state.last_msg_options.discard(lhs_var)

        if kind == "CXXMemberCallExpr":
            self._handle_member_call(
                node, cpp_file, state, node_line,
                allow_abort_finish=allow_abort_finish,
                known_stream_vars=known_stream_vars,
            )
        elif kind == "CallExpr":
            self._handle_free_call(node, cpp_file, state, node_line, allow_abort_finish=allow_abort_finish)

        for child in node.get("inner", []):
            self._scan_expr_calls(
                child, cpp_file, state, node_line,
                allow_abort_finish=allow_abort_finish,
                known_stream_vars=known_stream_vars,
            )

    def _rebind_stream_var(self, state: AbstractState, stream_var: str):
        """Rebinds anonymous/alias handle key to the observed stream variable name."""
        if stream_var not in state.stream_states:
            old_key = None
            if "stream" in state.stream_states:
                old_key = "stream"
            elif len(state.stream_states) == 1:
                old_key = next(iter(state.stream_states.keys()))
            if old_key is not None:
                state.stream_states[stream_var] = state.stream_states.pop(old_key)
                if old_key in state.cancel_requested_streams:
                    state.cancel_requested_streams.remove(old_key)
                    state.cancel_requested_streams.add(stream_var)
                if old_key in state.ptr_null_streams:
                    state.ptr_null_streams.remove(old_key)
                    state.ptr_null_streams.add(stream_var)
                if old_key in state.eos_observed_streams:
                    state.eos_observed_streams.remove(old_key)
                    state.eos_observed_streams.add(stream_var)
                if old_key in state.server_eos_mode:
                    state.server_eos_mode[stream_var] = state.server_eos_mode.pop(old_key)
                if old_key in state.recv_complete:
                    state.recv_complete[stream_var] = state.recv_complete.pop(old_key)
                if old_key in state.write_last_streams:
                    state.write_last_streams.remove(old_key)
                    state.write_last_streams.add(stream_var)

    def _handle_free_call(self, call_node: dict, cpp_file: Path, state: AbstractState,
                          line: int, allow_abort_finish: bool = False):
        """Handles C++ template-dependent stream member calls (`CXXDependentScopeMemberExpr`)."""
        inner = call_node.get("inner", [])
        if inner and inner[0].get("kind") == "CXXDependentScopeMemberExpr":
            dep_member = inner[0].get("member")
            if dep_member in ("Write", "WriteLast", "WritesDone", "Read", "Finish"):
                stream_var = find_decl_ref_name(inner[0]) or next(iter(state.stream_states.keys()), "stream")
                self._rebind_stream_var(state, stream_var)
                cur = state.stream_states.get(stream_var, self.initial_state)
                if dep_member in ("Write", "WriteLast"):
                    ok = self._check_write(cpp_file, stream_var, cur, state, [], line)
                    if dep_member == "WriteLast" and ok:
                        if self.protocol.get("role", "client") == "server":
                            state.write_last_streams.add(stream_var)
                        else:
                            new_cur = state.stream_states.get(stream_var, cur)
                            self._check_simple_op(
                                cpp_file, stream_var, new_cur, "WritesDone", state, line,
                                allow_abort_finish=allow_abort_finish, outcome="ok",
                            )
                else:
                    outcome = "OK" if dep_member == "Finish" else "ok"
                    self._check_simple_op(
                        cpp_file, stream_var, cur, dep_member, state, line,
                        allow_abort_finish=allow_abort_finish,
                        outcome=outcome,
                    )

    def _handle_member_call(self, call_node: dict, cpp_file: Path, state: AbstractState,
                            line: int, allow_abort_finish: bool = False,
                            known_stream_vars: Optional[Set[str]] = None):
        inner = call_node.get("inner", [])
        if not inner:
            return
        m_expr = inner[0]
        if m_expr.get("kind") != "MemberExpr":
            return

        member_name = m_expr.get("name", "")
        caller_name = find_decl_ref_name(m_expr)
        call_qt = call_node.get("type", {}).get("qualType", "")
        args = [find_decl_ref_name(arg_node) for arg_node in inner[1:] if find_decl_ref_name(arg_node)]

        # Case A: Type-resolved stub RPC initialization (or dependent fallback when AST lacks resolved type)
        if member_name in self.method_names:
            is_io_name = member_name in ("Write", "WriteLast", "Read", "WritesDone", "Finish")
            returns_grpc_stream = _is_stream_qualtype(call_qt)
            is_known_stream_receiver = bool(caller_name and known_stream_vars and caller_name in known_stream_vars)
            if returns_grpc_stream and not is_known_stream_receiver:
                state.stream_states["stream"] = self.initial_state
                state.recv_complete["stream"] = self._default_recv_complete()
                state.ptr_null_streams.discard("stream")
                state.write_last_streams.discard("stream")
                return
            if not call_qt or "<dependent type>" in call_qt:
                is_stub_caller = caller_name and ("stub" in caller_name.lower() or "client" in caller_name.lower())
                if not is_io_name or not state.stream_states or is_stub_caller:
                    state.stream_states["stream"] = self.initial_state
                    state.recv_complete["stream"] = self._default_recv_complete()
                    state.write_last_streams.discard("stream")
                    return
            elif not is_io_name and not is_known_stream_receiver:
                state.stream_states["stream"] = self.initial_state
                state.recv_complete["stream"] = self._default_recv_complete()
                state.write_last_streams.discard("stream")
                return

        # Track smart-pointer `.reset()` separately from protocol completion
        if member_name == "reset" and caller_name:
            if caller_name in state.stream_states or (known_stream_vars and caller_name in known_stream_vars):
                state.ptr_null_streams.add(caller_name)
                return

        # Track WriteOptions `.set_last_message()` / `.clear_last_message()`
        if member_name == "set_last_message":
            if caller_name:
                state.last_msg_options.add(caller_name)
            return
        if member_name == "clear_last_message":
            if caller_name:
                state.last_msg_options.discard(caller_name)
            return

        # Case B: Protobuf Level 2 oneof / orthogonal field-presence mutations
        for setter_key, (group_key, case_num) in self.payload_groups.items():
            if setter_key in member_name:
                if caller_name:
                    state.payload_states.setdefault(caller_name, {})[group_key] = case_num
                return

        # Handle clearing a oneof member or whole oneof group (`clear_<member>()` / `clear_<oneof>()`)
        if member_name.startswith("clear_") and caller_name:
            target = member_name[len("clear_"):]
            if target in self._group_names:
                state.payload_states.get(caller_name, {}).pop(target, None)
                return
            if target in self.payload_groups:
                grp, _ = self.payload_groups[target]
                state.payload_states.get(caller_name, {}).pop(grp, None)
                return

        # Case C: Best-effort TryCancel() — ClientContext marks cancellation-requested; ServerContext is advisory only
        if member_name == "TryCancel":
            base_inner = m_expr.get("inner", [])
            base_qt = base_inner[0].get("type", {}).get("qualType", "") if base_inner else ""
            if self.protocol.get("role") == "server" or "ServerContext" in base_qt:
                self.emit_diagnostic(
                    cpp_file.name, line, 9, "warning",
                    "SERVER_TRY_CANCEL_ADVISORY: ServerContext::TryCancel() is excluded from normal termination; "
                    "gRPC 1.72.0 advises returning grpc::Status::CANCELLED from the handler instead.",
                )
                return
            for svar, st_name in state.stream_states.items():
                if st_name not in self.terminal_states:
                    state.cancel_requested_streams.add(svar)
            return

        # Case D: Stream operations
        if member_name in ("Write", "WriteLast", "WritesDone", "Read", "Finish"):
            stream_var = caller_name or "stream"
            self._rebind_stream_var(state, stream_var)
            current_state = state.stream_states.get(stream_var, self.initial_state)

            if member_name in ("Write", "WriteLast"):
                has_last_opt = (
                    member_name == "WriteLast"
                    or any(a in state.last_msg_options for a in args)
                    or any(has_member_call(arg_node, "set_last_message") for arg_node in inner[1:])
                )
                write_ok = self._check_write(cpp_file, stream_var, current_state, state, args, line)
                if has_last_opt and write_ok:
                    if self.protocol.get("role", "client") == "server":
                        state.write_last_streams.add(stream_var)
                    else:
                        new_state = state.stream_states.get(stream_var, current_state)
                        self._check_simple_op(
                            cpp_file, stream_var, new_state, "WritesDone", state, line,
                            allow_abort_finish=allow_abort_finish,
                            outcome="ok",
                        )
            else:
                outcome = "OK" if member_name == "Finish" else "ok"
                self._check_simple_op(
                    cpp_file, stream_var, current_state, member_name, state, line,
                    allow_abort_finish=allow_abort_finish,
                    outcome=outcome,
                )

    def _guard_matches(self, guard: Optional[dict], msg_props: Dict[str, int]) -> bool:
        if not guard:
            return True
        gtype = guard.get("type")
        if gtype == "all":
            return all(self._guard_matches(cl, msg_props) for cl in guard.get("clauses", []))
        if gtype == "oneof_case":
            grp = guard.get("field") or "payload"
            return msg_props.get(grp) == guard.get("expected_case")
        return True

    def _format_expected_guard(self, guard: dict) -> str:
        if guard.get("type") == "all":
            parts = [f"case {c.get('expected_case')} ({c.get('case_name')})" for c in guard.get("clauses", [])]
            return " and ".join(parts)
        return f"case {guard.get('expected_case')} ({guard.get('case_name')})"

    def _check_write(self, cpp_file: Path, stream_var: str, current_state: str,
                     state: AbstractState, args: List[str], line: int) -> bool:
        if stream_var in state.write_last_streams:
            self.emit_diagnostic(
                cpp_file.name, line, 9, "error",
                f"Protocol violation (Level 1 ordering): Illegal Write()/WriteLast() on stream '{stream_var}' "
                f"after WriteLast() / set_last_message() in state '{current_state}'.",
            )
            return False

        state_data = self.states.get(current_state, {})
        allowed_transitions = [t for t in state_data.get("transitions", []) if t["op"] == "Write"]

        if not allowed_transitions:
            self.emit_diagnostic(
                cpp_file.name, line, 9, "error",
                f"Protocol violation (Level 1 ordering): Illegal Write() in state '{current_state}'. "
                f"No write transitions permitted (stream is half-closed, response-EOS closed, uninitialized, or finished).",
            )
            return False

        written_msg_var = args[0] if args else None
        msg_props = state.payload_states.get(written_msg_var, {})

        matched = None
        for t in allowed_transitions:
            if self._guard_matches(t.get("guard"), msg_props):
                matched = t
                break

        if matched:
            state.stream_states[stream_var] = matched["target"]
            return True

        expected_names = [self._format_expected_guard(t["guard"]) for t in allowed_transitions if t.get("guard")]
        hints = [t["guard"].get("error_hint") for t in allowed_transitions if t.get("guard") and t["guard"].get("error_hint")]
        hint_str = hints[0] if hints else "Payload violates Level 2 contract guard."
        actual_desc = f"cases {dict(msg_props)}" if msg_props else "unknown/uninitialized case"
        self.emit_diagnostic(
            cpp_file.name, line, 9, "error",
            f"Protocol violation (Level 2 oneof payload progression): In state '{current_state}', "
            f"expected Write() with {', '.join(expected_names)}, but message '{written_msg_var}' has {actual_desc}. "
            f"{hint_str}",
        )
        return False

    def _check_simple_op(self, cpp_file: Path, stream_var: str, current_state: str,
                         op_name: str, state: AbstractState, line: int,
                         allow_abort_finish: bool = False, outcome: Optional[str] = None):
        state_data = self.states.get(current_state, {})
        transitions = state_data.get("transitions", [])
        matching = [
            t for t in transitions
            if t["op"] == op_name and (outcome is None or t.get("outcome") is None or t.get("outcome") == outcome)
        ]
        # Fallback for legacy JSON contracts omitting "outcome" on Finish
        if not matching and op_name == "Finish":
            matching = [t for t in transitions if t["op"] == "Finish"]

        if not matching:
            # Finish() along a post-Write/Read failure path, observed Read(eos), or after TryCancel() reaches cleanup
            if (
                op_name == "Finish"
                and (
                    allow_abort_finish
                    or stream_var in state.cancel_requested_streams
                    or stream_var in state.eos_observed_streams
                )
                and self.terminal_states
            ):
                state.stream_states[stream_var] = sorted(self.terminal_states)[0]
                state.cancel_requested_streams.discard(stream_var)
                state.eos_observed_streams.discard(stream_var)
                state.write_last_streams.discard(stream_var)
                return
            self.emit_diagnostic(
                cpp_file.name, line, 9, "error",
                f"Protocol violation (Level 1 sequencing): Illegal {op_name}() in state '{current_state}'. "
                f"Valid operations from this state: {[(t['op'], t.get('outcome')) for t in transitions]}.",
            )
            return

        if op_name == "Read" and outcome == "ok" and self.protocol.get("role", "client") == "client":
            state.recv_complete[stream_var] = "No"

        if (
            op_name == "Finish"
            and self.is_default_level1
            and self.protocol.get("role", "client") == "client"
            and not allow_abort_finish
            and stream_var not in state.cancel_requested_streams
            and stream_var not in state.eos_observed_streams
        ):
            rc_val = state.recv_complete.get(stream_var, self._default_recv_complete())
            if rc_val != "Yes":
                self.conditional_verifications += 1
                self.emit_diagnostic(
                    cpp_file.name, line, 9, "warning",
                    f"CONDITIONAL_VERIFICATION: Finish() called in state '{current_state}' with "
                    f"ReceiveComplete={rc_val} (FinishAllowed assumes no further server messages are expected).",
                )

        if op_name in ("Finish", "ServerReturn"):
            state.write_last_streams.discard(stream_var)

        state.stream_states[stream_var] = matching[0]["target"]

    def _get_line(self, node: dict, fallback: int = 1) -> int:
        for key in ("range", "loc"):
            sub = node.get(key, {})
            targets = [sub.get("begin", {})] if key == "range" else [sub]
            for base in targets:
                if "line" in base:
                    return base["line"]
                for loc_kind in ("expansionLoc", "spellingLoc"):
                    if loc_kind in base and "line" in base[loc_kind]:
                        return base[loc_kind]["line"]
        return fallback


def main():
    root_dir = Path(__file__).resolve().parent.parent
    demo_dir = Path(__file__).resolve().parent / "regression" / "wenet_demo"
    spec_file = root_dir / "datasets" / "02_unmodified_oss" / "contracts" / "recognize_client.session.json"
    if not spec_file.exists():
        print(f"Error: Specification file not found: {spec_file}")
        sys.exit(1)

    checker = ClangSessionChecker(spec_path=spec_file)
    all_passed = True
    for fname, expect in [("client_good.cpp", True), ("client_bad_ordering.cpp", False), ("client_bad_unfinalized.cpp", False)]:
        fpath = demo_dir / fname
        if not fpath.exists():
            continue
        res = checker.analyze_file(fpath)
        if res != expect:
            all_passed = False
    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
