"""
fsm_builder.py — Synthesizes deterministic grpc.session.v1 FSMs from Session Type ASTs.

Transforms high-level session expressions into explicit state transitions:
  - Allocates numbered/named states
  - Binds recursion variables to loop header states
  - Lowers internal/external choices into multi-target state transitions
  - Synthesizes Level-2 payload guards (Protobuf oneof case discriminators)
  - Emits both Client FSM and Server Dual FSM conforming to grpc.session.v1 schema.
"""

from typing import Dict, List, Optional, Any, Set
from .ast_nodes import (
    SessionType,
    Send,
    Receive,
    WritesDoneOp,
    FinishOp,
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
                 payload_field_name: str = "payload"):
        self.role = role  # "client" or "server"
        self.payload_mappings = payload_mappings or {"decode_config": 1, "audio_data": 2}
        self.payload_field_name = payload_field_name
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

        # Cleanup: Ensure at least one terminal state exists
        has_terminal = any(s["terminal"] for s in self.states.values())
        if not has_terminal:
            # If end state wasn't explicitly set, mark leaf states as terminal
            for s_id, s_data in self.states.items():
                if len(s_data["transitions"]) == 0:
                    s_data["terminal"] = True

        return {
            "initial_state": self.initial_state,
            "states": self.states
        }

    def _make_guard(self, tag: Optional[str], case_num: Optional[int]) -> Optional[Dict[str, Any]]:
        if not tag and case_num is None:
            return None
        c_num = case_num if case_num is not None else self.payload_mappings.get(tag, 1)
        name = tag or f"case_{c_num}"
        return {
            "type": "oneof_case",
            "field": self.payload_field_name,
            "expected_case": c_num,
            "case_name": name,
            "error_hint": f"Expected payload variant '{name}' (case {c_num})"
        }

    def _lower(self, session: SessionType, current_state: str):
        if isinstance(session, Send):
            op_name = "Write" if self.role == "client" else "Read"
            guard = self._make_guard(session.guard_tag, session.guard_case)
            next_state = self._alloc_state_id()
            self.states[current_state]["transitions"].append({
                "op": op_name,
                "target": next_state,
                "guard": guard
            })
            if session.next_session:
                self._lower(session.next_session, next_state)
            else:
                self._lower(EndOp(), next_state)

        elif isinstance(session, Receive):
            op_name = "Read" if self.role == "client" else "Write"
            guard = self._make_guard(session.guard_tag, session.guard_case)
            next_state = self._alloc_state_id()
            self.states[current_state]["transitions"].append({
                "op": op_name,
                "target": next_state,
                "guard": guard
            })
            if session.next_session:
                self._lower(session.next_session, next_state)
            else:
                self._lower(EndOp(), next_state)

        elif isinstance(session, WritesDoneOp):
            if self.role == "client":
                next_state = self._alloc_state_id()
                self.states[current_state]["transitions"].append({
                    "op": "WritesDone",
                    "target": next_state,
                    "guard": None
                })
                if session.next_session:
                    self._lower(session.next_session, next_state)
                else:
                    self._lower(EndOp(), next_state)
            else:
                # Dual: server observes EOF on read loop
                if session.next_session:
                    self._lower(session.next_session, current_state)
                else:
                    self._lower(EndOp(), current_state)

        elif isinstance(session, FinishOp):
            next_state = self._alloc_state_id()
            self.states[current_state]["transitions"].append({
                "op": "Finish",
                "target": next_state,
                "guard": None
            })
            self.states[next_state]["terminal"] = True
            if session.next_session:
                self._lower(session.next_session, next_state)

        elif isinstance(session, EndOp):
            if self.role == "client":
                # In client gRPC, reaching 'end' after writes_done requires Finish()
                # Check if this state has a Finish transition
                has_finish = any(t["op"] == "Finish" for t in self.states[current_state]["transitions"])
                if not has_finish:
                    term_state = self._alloc_state_id()
                    self.states[term_state]["terminal"] = True
                    self.states[current_state]["transitions"].append({
                        "op": "Finish",
                        "target": term_state,
                        "guard": None
                    })
                else:
                    self.states[current_state]["terminal"] = True
            else:
                self.states[current_state]["terminal"] = True

        elif isinstance(session, Recursion):
            # The current state becomes the loop header
            self.rec_env[session.var_name] = current_state
            self._lower(session.body, current_state)

        elif isinstance(session, RecVar):
            target_header = self.rec_env.get(session.var_name)
            if not target_header:
                raise ValueError(f"Unbound recursive variable '{session.var_name}'")
            # If current_state is empty and different from target_header, alias or jump
            if current_state != target_header:
                # Merge or add epsilon transition / link
                pass

        elif isinstance(session, Choice):
            for label, branch_session in session.branches.items():
                if isinstance(branch_session, EndOp):
                    self._lower(EndOp(), current_state)
                elif isinstance(branch_session, (Send, Receive)):
                    # Extract op directly
                    is_send = isinstance(branch_session, Send)
                    op_name = ("Write" if is_send else "Read") if self.role == "client" else ("Read" if is_send else "Write")
                    guard = self._make_guard(branch_session.guard_tag, branch_session.guard_case)

                    # Look ahead to see if target is recursive loopback
                    if isinstance(branch_session.next_session, RecVar):
                        target = self.rec_env[branch_session.next_session.var_name]
                        self.states[current_state]["transitions"].append({
                            "op": op_name,
                            "target": target,
                            "guard": guard
                        })
                    else:
                        branch_target = self._alloc_state_id()
                        self.states[current_state]["transitions"].append({
                            "op": op_name,
                            "target": branch_target,
                            "guard": guard
                        })
                        if branch_session.next_session:
                            self._lower(branch_session.next_session, branch_target)
                        else:
                            self._lower(EndOp(), branch_target)
                else:
                    branch_target = self._alloc_state_id()
                    self._lower(branch_session, branch_target)


def compile_protocol_fsm(session_ast: SessionType,
                         package: str,
                         service: str,
                         method: str,
                         rpc_type: str,
                         client_stream_type: str,
                         server_stream_type: str,
                         role: str = "client",
                         payload_mappings: Optional[Dict[str, int]] = None,
                         payload_field_name: str = "payload") -> Dict[str, Any]:
    """
    Compiles a SessionType AST into a full grpc.session.v1 JSON contract dictionary.
    Computes endpoint duality automatically if role == 'server'.
    """
    ast_to_compile = session_ast if role == "client" else dual(session_ast)
    builder = FSMBuilder(role=role, payload_mappings=payload_mappings, payload_field_name=payload_field_name)
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
    if payload_mappings:
        contract["payload_setters"] = payload_mappings

    return contract
