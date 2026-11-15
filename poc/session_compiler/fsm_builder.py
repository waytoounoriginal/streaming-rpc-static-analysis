"""
fsm_builder.py — Synthesizes deterministic grpc.session.v1 FSMs from Session Type ASTs.

Transforms high-level session expressions into explicit state transitions:
  - Allocates numbered/named states
  - Binds recursion variables to loop header states
  - Lowers internal/external choices into multi-target state transitions with explicit outcome tags
  - Separates client surface actions (WritesDone, Finish, Read(eos), end) from server dual
    actions (PeerEndOfInput -> Read(eos) post-input state, ServerReturn(OK) -> terminal state)
  - Synthesizes Level-2 payload guards (Protobuf oneof cases and orthogonal field-presence groups)
  - Enforces transition determinism over (op, outcome, guard_signature)
"""

from typing import Dict, List, Optional, Any
from .ast_nodes import (
    SessionType,
    Send,
    Receive,
    WritesDoneOp,
    FinishOp,
    PeerEndOfInputOp,
    ServerReturnOp,
    EndOp,
    Recursion,
    RecVar,
    Choice,
    dual,
)


class FSMBuilder:
    def __init__(self,
                 role: str = "client",
                 payload_mappings: Optional[Dict[str, int]] = None,
                 payload_field_name: str = "payload",
                 payload_groups: Optional[Dict[str, Dict[str, Any]]] = None):
        self.role = role  # "client" or "server"
        self.payload_mappings = payload_mappings if payload_mappings is not None else {"decode_config": 1, "audio_data": 2}
        self.payload_field_name = payload_field_name
        self.payload_groups = payload_groups or {}
        self.states: Dict[str, Dict[str, Any]] = {}
        self.state_counter = 0
        self.rec_env: Dict[str, str] = {}  # var_name -> state_name
        self.initial_state: Optional[str] = None

    def _alloc_state_id(self, prefix: str = "S") -> str:
        s_id = f"{prefix}{self.state_counter}"
        self.state_counter += 1
        self.states[s_id] = {
            "description": f"State {s_id}",
            "terminal": False,
            "transitions": []
        }
        return s_id

    def build(self, session: SessionType) -> Dict[str, Any]:
        """Builds state dictionary from session AST."""
        start_id = self._alloc_state_id("S")
        self.initial_state = start_id
        self._lower(session, current_state=start_id)

        # Ensure unreachable dangling empty states are clean terminal only if marked
        has_terminal = any(s["terminal"] for s in self.states.values())
        if not has_terminal:
            for s_id, s_data in self.states.items():
                if len(s_data["transitions"]) == 0:
                    s_data["terminal"] = True

        self._validate_determinism()

        return {
            "initial_state": self.initial_state,
            "states": self.states
        }

    def _guard_signature(self, guard: Optional[Dict[str, Any]]) -> tuple:
        if not guard:
            return ()
        if guard.get("type") == "all":
            return ("all", tuple(self._guard_signature(c) for c in guard.get("clauses", [])))
        return (guard.get("type"), guard.get("field"), guard.get("expected_case"))

    def _validate_determinism(self):
        """Rejects ambiguous transitions sharing `(op, outcome, guard_signature)` to different targets."""
        for s_id, s_data in self.states.items():
            seen: Dict[tuple, str] = {}
            for tr in s_data.get("transitions", []):
                default_out = "OK" if tr["op"] == "ServerReturn" else "ok"
                outcome = tr.get("outcome") or default_out
                sig = (tr["op"], outcome, self._guard_signature(tr.get("guard")))
                tgt = tr["target"]
                if sig in seen and seen[sig] != tgt:
                    raise ValueError(
                        f"Non-deterministic transition in state {s_id} for {sig}: targets {seen[sig]} and {tgt}"
                    )
                seen[sig] = tgt

    def _make_single_guard(self, tag: Optional[str], case_num: Optional[int]) -> Optional[Dict[str, Any]]:
        if not tag and case_num is None:
            return None
        group_name = self.payload_field_name
        if case_num is not None:
            c_num = case_num
        else:
            c_num = 1
            for candidate in (tag, f"mutable_{tag}", f"set_{tag}"):
                if candidate and candidate in self.payload_mappings:
                    c_num = self.payload_mappings[candidate]
                    if candidate in self.payload_groups:
                        group_name = self.payload_groups[candidate]["group"]
                    break
        name = tag or f"case_{c_num}"
        return {
            "type": "oneof_case",
            "field": group_name,
            "expected_case": c_num,
            "case_name": name,
            "error_hint": f"Expected payload variant '{name}' (case {c_num})"
        }

    def _make_guard(self, tag: Optional[str], case_num: Optional[int],
                    guard_tags: Optional[List[str]] = None) -> Optional[Dict[str, Any]]:
        if guard_tags and len(guard_tags) > 1:
            clauses = [self._make_single_guard(t, None) for t in guard_tags]
            clauses = [c for c in clauses if c is not None]
            return {
                "type": "all",
                "clauses": clauses,
                "error_hint": "Expected conjunctive payload constraints: " + ", ".join(guard_tags),
            }
        return self._make_single_guard(tag, case_num)

    def _append_transition(self, state_id: str, op: str, target: str,
                           outcome: str = "ok", guard: Optional[Dict[str, Any]] = None,
                           label: Optional[str] = None):
        tr: Dict[str, Any] = {
            "op": op,
            "outcome": outcome,
        }
        if label is not None:
            tr["label"] = label
        tr["target"] = target
        tr["guard"] = guard
        self.states[state_id]["transitions"].append(tr)

    def _lower_terminal_action(self, current_state: str, label: Optional[str] = None):
        """Lowers EndOp / FinishOp / ServerReturnOp into an explicit transition to an accepting state."""
        op_name = "Finish" if self.role == "client" else "ServerReturn"
        outcome_name = "OK"
        existing = next(
            (t for t in self.states[current_state]["transitions"] if t["op"] == op_name),
            None,
        )
        if existing is None:
            term_state = self._alloc_state_id()
            self.states[term_state]["terminal"] = True
            self._append_transition(
                current_state, op_name, term_state, outcome=outcome_name, guard=None, label=label
            )

    def _lower(self, session: SessionType, current_state: str, branch_label: Optional[str] = None):
        """Lowers an endpoint-projected SessionType AST into deterministic FSM states."""
        if isinstance(session, (Send, Receive)):
            op_name = "Write" if isinstance(session, Send) else "Read"
            guard = self._make_guard(session.guard_tag, session.guard_case, getattr(session, "guard_tags", None))
            if isinstance(session.next_session, RecVar):
                target = self.rec_env.get(session.next_session.var_name)
                if not target:
                    raise ValueError(f"Unbound recursive variable '{session.next_session.var_name}'")
                self._append_transition(current_state, op_name, target, outcome="ok", guard=guard, label=branch_label)
            else:
                next_state = self._alloc_state_id()
                self._append_transition(current_state, op_name, next_state, outcome="ok", guard=guard, label=branch_label)
                if session.next_session:
                    self._lower(session.next_session, next_state)
                else:
                    self._lower(EndOp(), next_state)

        elif isinstance(session, WritesDoneOp):
            next_state = self._alloc_state_id()
            op_name = "WritesDone" if self.role == "client" else "Read"
            outcome = "ok" if self.role == "client" else "eos"
            self._append_transition(current_state, op_name, next_state, outcome=outcome, guard=None, label=branch_label)
            if session.next_session:
                self._lower(session.next_session, next_state)
            else:
                self._lower(EndOp(), next_state)

        elif isinstance(session, PeerEndOfInputOp):
            # Server observes explicit Read(eos) before entering post-input response phase
            next_state = self._alloc_state_id()
            self._append_transition(
                current_state, "Read", next_state, outcome="eos", guard=None, label=branch_label or "eof"
            )
            if session.next_session:
                self._lower(session.next_session, next_state)
            else:
                self._lower(EndOp(), next_state)

        elif isinstance(session, (FinishOp, ServerReturnOp, EndOp)):
            self._lower_terminal_action(current_state, label=branch_label)

        elif isinstance(session, Recursion):
            self.rec_env[session.var_name] = current_state
            self._lower(session.body, current_state)

        elif isinstance(session, RecVar):
            target_header = self.rec_env.get(session.var_name)
            if not target_header:
                raise ValueError(f"Unbound recursive variable '{session.var_name}'")

        elif isinstance(session, Choice):
            has_read_sibling = any(isinstance(b, Receive) for b in session.branches.values())
            for label, branch_session in session.branches.items():
                if isinstance(branch_session, EndOp):
                    if self.role == "client" and has_read_sibling:
                        # Explicit Read(eos) branch separates incoming stream exhaustion from Finish()
                        eos_state = self._alloc_state_id()
                        self._append_transition(
                            current_state, "Read", eos_state, outcome="eos", guard=None, label=label
                        )
                        self._lower_terminal_action(eos_state)
                    else:
                        self._lower_terminal_action(current_state, label=label)
                elif isinstance(branch_session, (Send, Receive, WritesDoneOp, PeerEndOfInputOp, FinishOp, ServerReturnOp)):
                    self._lower(branch_session, current_state, branch_label=label)
                else:
                    self._lower(branch_session, current_state)


def compile_protocol_fsm(session_ast: SessionType,
                         package: str,
                         service: str,
                         method: str,
                         rpc_type: str,
                         client_stream_type: str,
                         server_stream_type: str,
                         role: str = "client",
                         payload_mappings: Optional[Dict[str, int]] = None,
                         payload_field_name: str = "payload",
                         method_aliases: Optional[List[str]] = None,
                         payload_groups: Optional[Dict[str, Dict[str, Any]]] = None) -> Dict[str, Any]:
    """
    Compiles a SessionType AST into a full grpc.session.v1 JSON contract dictionary.
    Computes endpoint duality automatically if role == 'server'.
    """
    ast_to_compile = session_ast if role == "client" else dual(session_ast)
    builder = FSMBuilder(
        role=role,
        payload_mappings=payload_mappings,
        payload_field_name=payload_field_name,
        payload_groups=payload_groups,
    )
    fsm_data = builder.build(ast_to_compile)

    contract = {
        "schema_version": "grpc.session.v1",
        "protocol": {
            "package": package,
            "service": service,
            "method": method,
            "role": role,
            "rpc_type": rpc_type,
            "client_stream_type": client_stream_type,
            "server_stream_type": server_stream_type
        },
        "initial_state": fsm_data["initial_state"],
        "states": fsm_data["states"]
    }
    if method_aliases:
        contract["protocol"]["method_aliases"] = method_aliases
    if payload_mappings:
        contract["payload_setters"] = payload_mappings
    if payload_groups:
        contract["payload_groups"] = payload_groups

    return contract
