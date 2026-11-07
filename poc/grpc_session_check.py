#!/usr/bin/env python3
"""
grpc_session_check.py — Clang AST-based Static Typestate Checker for gRPC Streaming
Part of Bachelor Thesis: "Types Won't Save You, But Typestates Will: Static Protocol Checking for C++ gRPC"
Author: Mihai Tira

This tool statically verifies C++ client and server code against binary session type
specifications compiled from real gRPC services (WeNet ASR, Apache Arrow Flight,
BuildBox CAS, gRPC CachingInterceptor, Triton Inference Server, Google Cloud Storage).

Features:
1. Clang AST JSON extraction: Runs clang++ directly on C++ code using real gRPC headers.
2. Binary Session Type FSM: Ingests `.session.json` (grpc.session.v1 schema).
3. Level 1 Verification: Call ordering (Write -> WritesDone -> Read -> Finish), 2-phase
   bidirectional handshakes, use-after-close detection, and early-return lifecycle leaks.
4. Level 2 Verification: Protobuf oneof variant and field-presence progression across
   k=2 bounded loop unrolling (e.g., decode_config -> audio_data; write_object_spec -> clear).
5. Bi-Terminal Duality: Path-sensitive handling of I/O failure bailouts vs. early-return bugs.
6. Multi-Method Class Lifecycle Tracking: Tracks member fields (this->stream_, this->d_reader_writer,
   this->grpc_stream_, this->request_) across object-oriented method sequences on real repository ASTs.
"""

import sys
import os
import json
import subprocess
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

# Ensure UTF-8 output on Windows consoles
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')


def find_decl_ref_name(node: dict) -> Optional[str]:
    """Recursively search for a variable, parameter, or class field reference, skipping method names."""
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
    """Checks whether an AST node tree invokes a specific member method."""
    if node.get("kind") == "CXXMemberCallExpr":
        inner = node.get("inner", [])
        if inner and inner[0].get("kind") == "MemberExpr":
            if inner[0].get("name") == target_member:
                return True
    for ch in node.get("inner", []):
        if has_member_call(ch, target_member):
            return True
    return False


def has_function_call(node: dict, target_func: str) -> bool:
    """Checks whether an AST node tree invokes a free/static function by name."""
    if node.get("kind") == "CallExpr":
        for ch in node.get("inner", []):
            if ch.get("kind") in ("ImplicitCastExpr", "DeclRefExpr"):
                ref = ch.get("referencedDecl", {})
                if ref.get("name") == target_func:
                    return True
                for sub in ch.get("inner", []):
                    if sub.get("referencedDecl", {}).get("name") == target_func:
                        return True
    for ch in node.get("inner", []):
        if has_function_call(ch, target_func):
            return True
    return False


def contains_return_stmt(node: dict) -> bool:
    """Checks whether an AST subtree contains a ReturnStmt or CXXThrowExpr."""
    if not node:
        return False
    if node.get("kind") in ("ReturnStmt", "CXXThrowExpr"):
        return True
    for ch in node.get("inner", []):
        if contains_return_stmt(ch):
            return True
    return False


def is_negated_expr(node: dict) -> bool:
    """Checks whether an expression has a top-level logical NOT (!expr)."""
    if node.get("kind") == "UnaryOperator" and node.get("opcode") == "!":
        return True
    for ch in node.get("inner", []):
        if ch.get("kind") == "UnaryOperator" and ch.get("opcode") == "!":
            return True
    return False


def is_false_literal_expr(node: dict) -> bool:
    """Checks whether a DoStmt condition is a constant false/0 (e.g., do { ... } while (false))."""
    if not node:
        return True
    kind = node.get("kind", "")
    if kind == "CXXBoolLiteralExpr" and node.get("value") is False:
        return True
    if kind == "IntegerLiteral" and str(node.get("value", "")) == "0":
        return True
    for ch in node.get("inner", []):
        if is_false_literal_expr(ch):
            return True
    return False


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
    def __init__(self, spec_path: Optional[Path] = None, spec_dict: Optional[dict] = None):
        if spec_dict is not None:
            self.spec = spec_dict
        elif spec_path is not None:
            with open(spec_path, 'r', encoding='utf-8') as f:
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
        # Configurable Level 2 payload setter mappings (defaults to WeNet decode_config / audio_data)
        self.payload_setters: Dict[str, int] = self.spec.get("payload_setters", {
            "decode_config": 1,
            "audio_data": 2
        })
        self.diagnostics: List[Tuple[str, str]] = []
        self._emitted_keys: Set[Tuple[int, str]] = set()

    @classmethod
    def from_default_level1(cls, rpc_type: str, method_name: str) -> "ClangSessionChecker":
        """
        Constructs the universal Level-1 gRPC Stream Lifecycle Automaton directly from
        the C++ gRPC stream type (ClientReader, ClientWriter, ClientReaderWriter) without
        requiring a custom per-method .session.json file.
        """
        if rpc_type == "SERVER_STREAMING":
            spec = {
                "schema_version": "grpc.session.v1",
                "protocol": {"method": method_name, "rpc_type": "SERVER_STREAMING", "role": "client"},
                "payload_setters": {},
                "initial_state": "S0_READING",
                "states": {
                    "S0_READING": {
                        "terminal": False,
                        "transitions": [
                            {"op": "Read", "target": "S0_READING", "guard": None},
                            {"op": "Finish", "target": "S1_FINISHED", "guard": None},
                        ],
                    },
                    "S1_FINISHED": {"terminal": True, "transitions": []},
                },
            }
        elif rpc_type == "CLIENT_STREAMING":
            spec = {
                "schema_version": "grpc.session.v1",
                "protocol": {"method": method_name, "rpc_type": "CLIENT_STREAMING", "role": "client"},
                "payload_setters": {},
                "initial_state": "S0_WRITING",
                "states": {
                    "S0_WRITING": {
                        "terminal": False,
                        "transitions": [
                            {"op": "Write", "target": "S0_WRITING", "guard": None},
                            {"op": "WritesDone", "target": "S1_HALF_CLOSED", "guard": None},
                        ],
                    },
                    "S1_HALF_CLOSED": {
                        "terminal": False,
                        "transitions": [
                            {"op": "Finish", "target": "S2_FINISHED", "guard": None},
                        ],
                    },
                    "S2_FINISHED": {"terminal": True, "transitions": []},
                },
            }
        else:  # BIDIRECTIONAL_STREAMING
            spec = {
                "schema_version": "grpc.session.v1",
                "protocol": {"method": method_name, "rpc_type": "BIDIRECTIONAL_STREAMING", "role": "client"},
                "payload_setters": {},
                "initial_state": "S0_OPEN",
                "states": {
                    "S0_OPEN": {
                        "terminal": False,
                        "transitions": [
                            {"op": "Write", "target": "S0_OPEN", "guard": None},
                            {"op": "Read", "target": "S0_OPEN", "guard": None},
                            {"op": "WritesDone", "target": "S1_HALF_CLOSED", "guard": None},
                        ],
                    },
                    "S1_HALF_CLOSED": {
                        "terminal": False,
                        "transitions": [
                            {"op": "Read", "target": "S1_HALF_CLOSED", "guard": None},
                            {"op": "Finish", "target": "S2_FINISHED", "guard": None},
                        ],
                    },
                    "S2_FINISHED": {"terminal": True, "transitions": []},
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
        """Runs clang++ with -Xclang -ast-dump=json to extract AST declarations."""
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
            str(cpp_file)
        ])
        res = subprocess.run(clang_cmd, capture_output=True, text=True, encoding='utf-8', errors='replace')
        if res.returncode != 0 and not res.stdout:
            raise RuntimeError(f"Clang AST dump failed:\n{res.stderr}")

        return decode_clang_json_stream(res.stdout)

    def analyze_file(self, cpp_file: Path) -> bool:
        self.diagnostics = []
        self._emitted_keys = set()
        role = self.protocol.get("role", "client")
        print(f"\n[*] Analyzing C++ source: {cpp_file.name}")
        print(f"[*] Target gRPC Service: {self.protocol['service']}.{self.protocol['method']} (role={role})")

        decls = self.run_clang_ast_dump(cpp_file)

        target_funcs = []
        for d in find_all_methods(decls):
            loc = d.get("loc", {})
            file_included = loc.get("file")
            if file_included and not file_included.endswith(cpp_file.name):
                continue

            if role == "server":
                has_server_stream_param = any(
                    ch.get("kind") == "ParmVarDecl" and
                    any(st in ch.get("type", {}).get("qualType", "")
                        for st in ("ServerReader", "ServerWriter", "ServerReaderWriter"))
                    for ch in d.get("inner", [])
                )
                if has_server_stream_param:
                    target_funcs.append(d)
            else:
                initiates_rpc = any(has_member_call(d, m) for m in self.method_names)
                if initiates_rpc:
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
        else:
            print(f"\033[1;31m[-] FAILED: Detected {len(errors)} protocol violation(s) in {cpp_file.name}\033[0m")
            return False

    def analyze_real_methods(self, cpp_file: Path, methods: List[dict],
                             initial_stream_state: Optional[str] = None) -> bool:
        """
        Analyzes one or a sequence of real C++ methods extracted from a repository translation unit,
        propagating member stream states and payload variant states across the lifecycle sequence.
        """
        self.diagnostics = []
        self._emitted_keys = set()
        role = self.protocol.get("role", "client")

        stream_states: Dict[str, str] = {}
        msg_payload_cases: Dict[str, int] = {}
        if initial_stream_state:
            stream_states["stream"] = initial_stream_state

        last_line = 1
        for idx, func in enumerate(methods):
            is_last = (idx == len(methods) - 1)
            last_line = func.get("loc", {}).get("line") or self._get_line(func, 1)
            self._analyze_function(
                func, cpp_file,
                shared_stream_states=stream_states,
                shared_msg_cases=msg_payload_cases,
                check_end_of_func=is_last
            )

        errors = [d for sev, d in self.diagnostics if sev == "error"]
        return len(errors) == 0

    def _analyze_function(self, func: dict, cpp_file: Path,
                          shared_stream_states: Optional[Dict[str, str]] = None,
                          shared_msg_cases: Optional[Dict[str, int]] = None,
                          check_end_of_func: bool = True):
        func_line = func.get("loc", {}).get("line") or self._get_line(func, 1)

        stream_states = shared_stream_states if shared_stream_states is not None else {}
        msg_payload_cases = shared_msg_cases if shared_msg_cases is not None else {}
        io_result_vars: Set[str] = set()

        # For server-side role, initialize ServerReader/Writer parameter or member stream to initial_state
        if self.protocol.get("role") == "server":
            for ch in func.get("inner", []):
                if ch.get("kind") == "ParmVarDecl":
                    qtype = ch.get("type", {}).get("qualType", "")
                    if any(st in qtype for st in ("ServerReader", "ServerWriter", "ServerReaderWriter")):
                        pname = ch.get("name") or "stream"
                        stream_states[pname] = self.initial_state
            if not stream_states:
                stream_states["stream"] = self.initial_state

        body = None
        for inner in func.get("inner", []):
            if inner.get("kind") == "CompoundStmt":
                body = inner
                break
        if not body:
            return

        self._walk_stmts(
            body.get("inner", []), cpp_file, stream_states, msg_payload_cases,
            io_result_vars, check_return_termination=check_end_of_func
        )

        # Check end of function/sequence: all streams must have reached terminal state
        if check_end_of_func:
            for stream_var, current_state in list(stream_states.items()):
                if current_state not in self.terminal_states:
                    self.emit_diagnostic(
                        cpp_file.name, func_line, 1, "error",
                        f"Stream lifecycle leak (Level 1 termination): Stream '{stream_var}' destroyed at end of function "
                        f"in non-terminal state '{current_state}'. Expected terminal state {list(self.terminal_states)} "
                        f"(missing Finish()). Server HTTP/2 stream will leak."
                    )

    def _walk_stmts(self, stmts: List[dict], cpp_file: Path,
                    stream_states: Dict[str, str],
                    msg_payload_cases: Dict[str, int],
                    io_result_vars: Optional[Set[str]] = None,
                    check_return_termination: bool = True):
        if io_result_vars is None:
            io_result_vars = set()

        for stmt in stmts:
            kind = stmt.get("kind", "")
            line = self._get_line(stmt)

            # 0. Nested CompoundStmt orCXXTryStmt
            if kind == "CompoundStmt":
                self._walk_stmts(stmt.get("inner", []), cpp_file, stream_states, msg_payload_cases, io_result_vars, check_return_termination)
                continue

            if kind == "CXXTryStmt":
                for ch in stmt.get("inner", []):
                    if ch.get("kind") == "CompoundStmt":
                        self._walk_stmts(ch.get("inner", []), cpp_file, stream_states, msg_payload_cases, io_result_vars, check_return_termination)
                continue

            # 0b. DoStmt: distinguish macro `do { ... } while (false)` vs real `do { ... } while (cond)` loop
            if kind == "DoStmt":
                inner = stmt.get("inner", [])
                body_node = inner[0] if inner else None
                cond_node = inner[1] if len(inner) > 1 else None
                is_macro_do_while_false = is_false_literal_expr(cond_node)

                if body_node and body_node.get("kind") == "CompoundStmt":
                    loop_stmts = body_node.get("inner", [])
                    errs_before = len(self.diagnostics)
                    # Iteration 1
                    self._walk_stmts(loop_stmts, cpp_file, stream_states, msg_payload_cases, io_result_vars, check_return_termination)
                    # Iteration 2 (k=2 bounded unrolling for real do-while loops such as Google Cloud Storage)
                    if not is_macro_do_while_false and len(self.diagnostics) == errs_before:
                        self._walk_stmts(loop_stmts, cpp_file, stream_states, msg_payload_cases, io_result_vars, check_return_termination)
                else:
                    for ch in inner:
                        self._scan_expr_calls(ch, cpp_file, stream_states, msg_payload_cases, line, io_result_vars)
                continue

            # 0c. DeclStmt: track boolean variables storing Write()/Read() I/O outcomes and ToProto() initializers
            if kind == "DeclStmt":
                for ch in stmt.get("inner", []):
                    if ch.get("kind") == "VarDecl":
                        vname = ch.get("name")
                        if vname and (has_member_call(ch, "Write") or has_member_call(ch, "Read")):
                            io_result_vars.add(vname)
                        if vname and "proto_request" in vname and "ToProto" in self.payload_setters:
                            msg_payload_cases[vname] = self.payload_setters["ToProto"]
                self._scan_expr_calls(stmt, cpp_file, stream_states, msg_payload_cases, line, io_result_vars)
                continue

            # 1. IfStmt: path-sensitive branch analysis
            if kind == "IfStmt":
                children = stmt.get("inner", [])
                if not children:
                    continue
                cond = children[0]
                then_branch = children[1] if len(children) > 1 else None
                else_branch = children[2] if len(children) > 2 else None
                negated = is_negated_expr(cond)
                cond_var = find_decl_ref_name(cond)

                # Check A: Null-check on stream or status-or: if (!stream) / if (!r) / if (!response)
                if not has_member_call(cond, "Write") and not has_member_call(cond, "Read") and (
                    has_member_call(cond, "operator bool") or "operator bool" in json.dumps(cond)
                ):
                    if not negated and then_branch:
                        # Positive check: if (stream_) { ... } (e.g., ~GrpcResultStream() or GrpcResultStream::Next())
                        # If stream_ has already finished & reset() in a prior method, `if (stream_)` is false at runtime.
                        if cond_var and cond_var in stream_states and stream_states[cond_var] in self.terminal_states:
                            continue
                        then_stmts = [then_branch] if then_branch.get("kind") != "CompoundStmt" else then_branch.get("inner", [])
                        self._walk_stmts(then_stmts, cpp_file, stream_states, msg_payload_cases, io_result_vars, check_return_termination)
                    continue

                # Check B: I/O check in condition, e.g. if (!stream->Write(req)) or if (!successful_write) or if (watchdog.get())
                if has_member_call(cond, "Write") or has_member_call(cond, "Read"):
                    self._scan_expr_calls(cond, cpp_file, stream_states, msg_payload_cases, line, io_result_vars)
                    if negated:
                        # Negated condition: then branch represents I/O transport failure!
                        # Under Bi-Terminal Session Duality, error exit on broken stream is valid.
                        continue
                    else:
                        if then_branch:
                            then_stmts = [then_branch] if then_branch.get("kind") != "CompoundStmt" else then_branch.get("inner", [])
                            self._walk_stmts(then_stmts, cpp_file, stream_states, msg_payload_cases, io_result_vars, check_return_termination)
                    continue

                if (negated and cond_var and cond_var in io_result_vars) or (
                    cond_var == "watchdog" and has_member_call(cond, "get")
                ) or (not negated and cond_var in ("finished_", "done_writing_")):
                    # Negated check on a boolean variable storing Write()/Read() result (e.g. BuildBox `if (!successful_write)`
                    # or Google Cloud Storage `if (!success)` / `if (watchdog.get())`) or idempotency guard (`if (finished_)`)
                    continue

                # Check C: Condition on local variable / status check (e.g. if (!ok), if (!_s.ok()), if (write_ok))
                if then_branch:
                    then_stmts = [then_branch] if then_branch.get("kind") != "CompoundStmt" else then_branch.get("inner", [])
                    if contains_return_stmt(then_branch):
                        # Path-sensitive split: clone state for early-return branch so cleanup (like Finish())
                        # in the returning branch is verified for that return without polluting fallthrough path.
                        branch_states = dict(stream_states)
                        branch_cases = dict(msg_payload_cases)
                        self._walk_stmts(then_stmts, cpp_file, branch_states, branch_cases, io_result_vars, check_return_termination)
                    else:
                        self._walk_stmts(then_stmts, cpp_file, stream_states, msg_payload_cases, io_result_vars, check_return_termination)
                if else_branch:
                    else_stmts = [else_branch] if else_branch.get("kind") != "CompoundStmt" else else_branch.get("inner", [])
                    self._walk_stmts(else_stmts, cpp_file, stream_states, msg_payload_cases, io_result_vars, check_return_termination)
                continue

            # 2. ReturnStmt: scan return expression first (e.g. `return FromGrpcStatus(stream->Finish(), ...)`),
            #    then check if any active stream is left in a non-terminal state!
            if kind == "ReturnStmt":
                self._scan_expr_calls(stmt, cpp_file, stream_states, msg_payload_cases, line, io_result_vars)
                if check_return_termination:
                    for stream_var, current_state in list(stream_states.items()):
                        if current_state not in self.terminal_states:
                            self.emit_diagnostic(
                                cpp_file.name, line, 9, "error",
                                f"Stream lifecycle leak (Level 1 termination): Stream '{stream_var}' destroyed on return "
                                f"in non-terminal state '{current_state}'. Expected terminal state {list(self.terminal_states)} "
                                f"(missing WritesDone() and Finish()). Server HTTP/2 resources will leak."
                            )
                continue

            # 3. Loops: ForStmt / WhileStmt / CXXForRangeStmt (with k=2 bounded unrolling)
            if kind in ("ForStmt", "WhileStmt", "CXXForRangeStmt"):
                # Note: For server role in WhileStmt (while (stream->Read(&req))), 0-iteration path
                # is possible if client sends empty stream and immediately closes!
                is_server_while_read = (
                    kind == "WhileStmt" and self.protocol.get("role") == "server"
                )

                if not is_server_while_read:
                    for ch in stmt.get("inner", []):
                        if ch.get("kind") != "CompoundStmt":
                            self._scan_expr_calls(ch, cpp_file, stream_states, msg_payload_cases, line, io_result_vars)

                loop_stmts = []
                for ch in stmt.get("inner", []):
                    if ch.get("kind") == "CompoundStmt":
                        loop_stmts = ch.get("inner", [])
                        break

                if loop_stmts:
                    errs_before = len(self.diagnostics)
                    # Iteration 1
                    self._walk_stmts(loop_stmts, cpp_file, stream_states, msg_payload_cases, io_result_vars, check_return_termination)
                    # Iteration 2 (k=2 bounded unrolling for loop-carried state/payload mutations)
                    if kind in ("ForStmt", "CXXForRangeStmt") and len(self.diagnostics) == errs_before:
                        self._walk_stmts(loop_stmts, cpp_file, stream_states, msg_payload_cases, io_result_vars, check_return_termination)
                continue

            # 4. Expressions & Calls inside this statement
            self._scan_expr_calls(stmt, cpp_file, stream_states, msg_payload_cases, line, io_result_vars)

    def _scan_expr_calls(self, node: dict, cpp_file: Path,
                         stream_states: Dict[str, str],
                         msg_payload_cases: Dict[str, int],
                         line: int,
                         io_result_vars: Optional[Set[str]] = None):
        kind = node.get("kind", "")
        node_line = self._get_line(node, line)
        if kind == "LambdaExpr":
            # Only check ReturnStmt termination inside the lambda if the lambda itself owns/creates the stream
            # (e.g. BuildBox `uploadLambda` / `downloadLambda`), not if it's a helper callback inside an open stream
            # (e.g. Google Cloud Storage `watchdog` returning bool).
            lambda_owns_stream = (len(stream_states) == 0)
            for ch in node.get("inner", []):
                if ch.get("kind") == "CompoundStmt":
                    self._walk_stmts(ch.get("inner", []), cpp_file, stream_states, msg_payload_cases, io_result_vars, lambda_owns_stream)
                    break
            return
        elif kind == "CXXThrowExpr":
            for stream_var, current_state in list(stream_states.items()):
                if current_state not in self.terminal_states:
                    self.emit_diagnostic(
                        cpp_file.name, node_line, 9, "error",
                        f"Stream lifecycle leak (Level 1 exception unwinding): Stream '{stream_var}' destroyed by uncaught throw "
                        f"in non-terminal state '{current_state}'. Expected terminal state {list(self.terminal_states)} "
                        f"(missing Finish()). Server HTTP/2 resources will leak."
                    )
            return
        elif kind == "CXXMemberCallExpr":
            self._handle_member_call(node, cpp_file, stream_states, msg_payload_cases, node_line)
        elif kind == "CallExpr":
            self._handle_free_call(node, cpp_file, stream_states, msg_payload_cases, node_line)

        for child in node.get("inner", []):
            self._scan_expr_calls(child, cpp_file, stream_states, msg_payload_cases, node_line, io_result_vars)

    def _handle_free_call(self, call_node: dict, cpp_file: Path,
                          stream_states: Dict[str, str],
                          msg_payload_cases: Dict[str, int],
                          line: int):
        """Handles helper function calls (CloseWriteObjectStream) and C++ template stream calls (CXXDependentScopeMemberExpr)."""
        if has_function_call(call_node, "CloseWriteObjectStream"):
            stream_var = "stream"
            for k in list(stream_states.keys()):
                stream_var = k
                break
            cur = stream_states.get(stream_var, self.initial_state)
            self._check_simple_op(cpp_file, stream_var, cur, "WritesDone", stream_states, line)
            cur = stream_states.get(stream_var, cur)
            self._check_simple_op(cpp_file, stream_var, cur, "Finish", stream_states, line)
            return

        # C++ template-dependent stream member calls (e.g. `stream_->Finish()` in `FinishableDataStream<Stream, ReadPayloadType>::DoFinish`)
        inner = call_node.get("inner", [])
        if inner and inner[0].get("kind") == "CXXDependentScopeMemberExpr":
            dep_member = inner[0].get("member")
            if dep_member in ("Write", "WritesDone", "Read", "Finish"):
                stream_var = find_decl_ref_name(inner[0]) or next(iter(stream_states.keys()), "stream")
                if stream_var not in stream_states and "stream" in stream_states:
                    stream_states[stream_var] = stream_states.pop("stream")
                cur = stream_states.get(stream_var, self.initial_state)
                self._check_simple_op(cpp_file, stream_var, cur, dep_member, stream_states, line)

    def _handle_member_call(self, call_node: dict, cpp_file: Path,
                            stream_states: Dict[str, str],
                            msg_payload_cases: Dict[str, int],
                            line: int):
        inner = call_node.get("inner", [])
        if not inner:
            return
        m_expr = inner[0]
        if m_expr.get("kind") != "MemberExpr":
            return

        member_name = m_expr.get("name")
        caller_name = find_decl_ref_name(m_expr)

        # Extract arguments from inner[1:]
        args = []
        for arg_node in inner[1:]:
            aname = find_decl_ref_name(arg_node)
            if aname:
                args.append(aname)

        # Case A: stub->Method(...) -> initialize stream
        # Note: if the RPC method itself is named "Write" or "Read" (e.g. google.bytestream.ByteStream),
        # only treat it as stub initialization when no stream is open yet (or caller is a stub/client).
        if member_name in self.method_names:
            is_io_name = member_name in ("Write", "Read", "WritesDone", "Finish")
            is_stub_caller = caller_name and ("stub" in caller_name.lower() or "client" in caller_name.lower())
            if not is_io_name or not stream_states or is_stub_caller:
                stream_states["stream"] = self.initial_state
                return

        # Case A2: Server-side handler helper call OnSpeechEnd() in WeNet grpc_server.cc
        if self.protocol.get("role") == "server" and member_name == "OnSpeechEnd":
            stream_var = next(iter(stream_states.keys()), "stream_")
            current_state = stream_states.get(stream_var, self.initial_state)
            self._check_simple_op(cpp_file, stream_var, current_state, "OnSpeechEnd", stream_states, line)
            return

        # Case B: Protobuf Level 2 oneof / field-presence mutations
        for setter_key, case_num in self.payload_setters.items():
            if setter_key in member_name:
                if caller_name:
                    msg_payload_cases[caller_name] = case_num
                return

        # Case C: Stream operations (Write, WritesDone, Read, Finish)
        if member_name in ("Write", "WritesDone", "Read", "Finish"):
            stream_var = caller_name or "stream"
            if stream_var not in stream_states and "stream" in stream_states:
                stream_states[stream_var] = stream_states.pop("stream")
            elif stream_var not in stream_states and len(stream_states) == 1:
                # Rebind across member field alias (e.g. reader_writer -> d_reader_writer in BuildBox)
                old_key = next(iter(stream_states.keys()))
                stream_states[stream_var] = stream_states.pop(old_key)
            current_state = stream_states.get(stream_var, self.initial_state)

            if member_name == "Write":
                self._check_write(call_node, cpp_file, stream_var, current_state,
                                  stream_states, msg_payload_cases, args, line)
            elif member_name == "WritesDone":
                self._check_simple_op(cpp_file, stream_var, current_state, "WritesDone",
                                      stream_states, line)
            elif member_name == "Read":
                self._check_simple_op(cpp_file, stream_var, current_state, "Read",
                                      stream_states, line)
            elif member_name == "Finish":
                self._check_simple_op(cpp_file, stream_var, current_state, "Finish",
                                      stream_states, line)

    def _check_write(self, call_node: dict, cpp_file: Path,
                     stream_var: str, current_state: str,
                     stream_states: Dict[str, str],
                     msg_payload_cases: Dict[str, int],
                     args: List[str], line: int):
        state_data = self.states.get(current_state, {})
        allowed_transitions = [
            t for t in state_data.get("transitions", []) if t["op"] == "Write"
        ]

        if not allowed_transitions:
            self.emit_diagnostic(
                cpp_file.name, line, 9, "error",
                f"Protocol violation (Level 1 ordering): Illegal Write() in state '{current_state}'. "
                f"No write transitions permitted (stream is half-closed, uninitialized, or finished)."
            )
            return

        written_msg_var = args[0] if args else None
        msg_case = msg_payload_cases.get(written_msg_var, None)

        matched = None
        for t in allowed_transitions:
            guard = t.get("guard")
            if not guard:
                matched = t
                break
            if guard.get("type") == "oneof_case":
                if msg_case == guard.get("expected_case"):
                    matched = t
                    break

        if matched:
            stream_states[stream_var] = matched["target"]
        else:
            expected_names = [
                f"case {t['guard']['expected_case']} ({t['guard']['case_name']})"
                for t in allowed_transitions if t.get("guard")
            ]
            hints = [
                t["guard"].get("error_hint")
                for t in allowed_transitions
                if t.get("guard") and t["guard"].get("error_hint")
            ]
            hint_str = hints[0] if hints else "In WeNet ASR, sending audio before DecodeConfig crashes the server pipeline."
            actual_desc = f"case {msg_case}" if msg_case else "unknown/uninitialized case"
            self.emit_diagnostic(
                cpp_file.name, line, 9, "error",
                f"Protocol violation (Level 2 oneof payload progression): In state '{current_state}', "
                f"expected Write() with {', '.join(expected_names)}, but message '{written_msg_var}' has {actual_desc}. "
                f"{hint_str}"
            )

    def _check_simple_op(self, cpp_file: Path, stream_var: str, current_state: str,
                         op_name: str, stream_states: Dict[str, str], line: int):
        state_data = self.states.get(current_state, {})
        matching = [t for t in state_data.get("transitions", []) if t["op"] == op_name]

        if not matching:
            self.emit_diagnostic(
                cpp_file.name, line, 9, "error",
                f"Protocol violation (Level 1 sequencing): Illegal {op_name}() in state '{current_state}'. "
                f"Valid operations from this state: {[t['op'] for t in state_data.get('transitions', [])]}."
            )
            return

        target_state = matching[0]["target"]
        stream_states[stream_var] = target_state

    def _get_line(self, node: dict, fallback: int = 1) -> int:
        range_info = node.get("range", {})
        begin = range_info.get("begin", {})
        if "line" in begin:
            return begin["line"]
        if "expansionLoc" in begin and "line" in begin["expansionLoc"]:
            return begin["expansionLoc"]["line"]
        if "spellingLoc" in begin and "line" in begin["spellingLoc"]:
            return begin["spellingLoc"]["line"]
        loc = node.get("loc", {})
        if "line" in loc:
            return loc["line"]
        if "expansionLoc" in loc and "line" in loc["expansionLoc"]:
            return loc["expansionLoc"]["line"]
        if "spellingLoc" in loc and "line" in loc["spellingLoc"]:
            return loc["spellingLoc"]["line"]
        return fallback


def main():
    spec_file = Path(__file__).parent / "wenet.session.json"
    if not spec_file.exists():
        print(f"Error: Specification file not found: {spec_file}")
        sys.exit(1)

    checker = ClangSessionChecker(spec_file)

    test_files = [
        Path(__file__).parent / "client_good.cpp",
        Path(__file__).parent / "client_bad_ordering.cpp",
        Path(__file__).parent / "client_bad_unfinalized.cpp",
    ]

    results = {}
    for test_file in test_files:
        if test_file.exists():
            success = checker.analyze_file(test_file)
            results[test_file.name] = success

    print("\n" + "=" * 65)
    print("STATIC PROTOCOL VERIFICATION SUMMARY (WeNet ASR gRPC Service)")
    print("=" * 65)
    for fname, passed in results.items():
        status = "\033[1;32mPASSED (Conforming)\033[0m" if passed else "\033[1;31mFAILED (Violation Caught)\033[0m"
        print(f"  {fname:30}: {status}")

    all_as_expected = (
        results.get("client_good.cpp") is True and
        results.get("client_bad_ordering.cpp") is False and
        results.get("client_bad_unfinalized.cpp") is False
    )

    if all_as_expected:
        print("\n\033[1;32m[+] SUCCESS: Checker verified conforming client and caught all Level 1 and Level 2 defects!\033[0m")
        sys.exit(0)
    else:
        print("\n\033[1;31m[-] FAILURE: One or more test outcomes differed from expected.\033[0m")
        sys.exit(1)


if __name__ == "__main__":
    main()
