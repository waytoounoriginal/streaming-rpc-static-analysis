"""
ast_nodes.py — Abstract Syntax Tree for Binary Session Types & Endpoint Duality

Formal Grammar:
  S ::= !T[guard]; S          (Send message with optional Level-2 payload guard)
      | ?T[guard]; S          (Receive message with optional Level-2 payload guard)
      | WritesDone; S         (Half-close write direction on client stream)
      | Finish; S             (RPC finalization / status collection)
      | end                   (Successful protocol termination)
      | rec X. S | mu X. S    (Recursive session type definition)
      | X                     (Recursive variable reference / loop back)
      | +{l1: S1, ..., ln: Sn} (Internal choice / select)
      | &{l1: S1, ..., ln: Sn} (External choice / branch)
"""

from dataclasses import dataclass
from typing import Dict, Optional, Any


class SessionType:
    """Base class for all session type AST nodes."""
    def to_string(self) -> str:
        raise NotImplementedError


@dataclass
class Send(SessionType):
    msg_type: str
    guard_tag: Optional[str] = None
    guard_case: Optional[int] = None
    next_session: Optional[SessionType] = None

    def to_string(self) -> str:
        tag_str = f"[{self.guard_tag}]" if self.guard_tag else ""
        next_str = f"; {self.next_session.to_string()}" if self.next_session else ""
        return f"!{self.msg_type}{tag_str}{next_str}"


@dataclass
class Receive(SessionType):
    msg_type: str
    guard_tag: Optional[str] = None
    guard_case: Optional[int] = None
    next_session: Optional[SessionType] = None

    def to_string(self) -> str:
        tag_str = f"[{self.guard_tag}]" if self.guard_tag else ""
        next_str = f"; {self.next_session.to_string()}" if self.next_session else ""
        return f"?{self.msg_type}{tag_str}{next_str}"


@dataclass
class WritesDoneOp(SessionType):
    next_session: Optional[SessionType] = None

    def to_string(self) -> str:
        next_str = f"; {self.next_session.to_string()}" if self.next_session else ""
        return f"WritesDone{next_str}"


@dataclass
class FinishOp(SessionType):
    next_session: Optional[SessionType] = None

    def to_string(self) -> str:
        next_str = f"; {self.next_session.to_string()}" if self.next_session else ""
        return f"Finish{next_str}"


@dataclass
class EndOp(SessionType):
    def to_string(self) -> str:
        return "end"


@dataclass
class Recursion(SessionType):
    var_name: str
    body: SessionType

    def to_string(self) -> str:
        return f"rec {self.var_name}. ({self.body.to_string()})"


@dataclass
class RecVar(SessionType):
    var_name: str

    def to_string(self) -> str:
        return self.var_name


@dataclass
class Choice(SessionType):
    """
    Choice between alternatives.
    is_external = True represents external choice (&) / branch
    is_external = False represents internal choice (+) / select
    """
    is_external: bool
    branches: Dict[str, SessionType]

    def to_string(self) -> str:
        op = "&" if self.is_external else "+"
        branch_strs = [f"{label}: {st.to_string()}" for label, st in self.branches.items()]
        return f"{op}{{{', '.join(branch_strs)}}}"


def dual(session: SessionType) -> SessionType:
    """
    Computes the dual session type (S̄) according to Honda et al. / Yoshida & Vasconcelos:
      - !(T); S    => ?(T); S̄
      - ?(T); S    => !(T); S̄
      - WritesDone; S => ReadEOF; S̄ (on server: reads EOF when client calls WritesDone)
      - Finish; S  => S̄ (server returns status upon completion)
      - +{li: Si}  => &{li: S̄i}  (Internal choice becomes External choice)
      - &{li: Si}  => +{li: S̄i}  (External choice becomes Internal choice)
      - rec X. S   => rec X. S̄
      - X          => X
      - end        => end
    """
    if isinstance(session, Send):
        return Receive(
            msg_type=session.msg_type,
            guard_tag=session.guard_tag,
            guard_case=session.guard_case,
            next_session=dual(session.next_session) if session.next_session else None,
        )
    elif isinstance(session, Receive):
        return Send(
            msg_type=session.msg_type,
            guard_tag=session.guard_tag,
            guard_case=session.guard_case,
            next_session=dual(session.next_session) if session.next_session else None,
        )
    elif isinstance(session, WritesDoneOp):
        # Client writes done -> Server observes Read returning EOF
        return WritesDoneOp(next_session=dual(session.next_session) if session.next_session else None)
    elif isinstance(session, FinishOp):
        return FinishOp(next_session=dual(session.next_session) if session.next_session else None)
    elif isinstance(session, EndOp):
        return EndOp()
    elif isinstance(session, Recursion):
        return Recursion(var_name=session.var_name, body=dual(session.body))
    elif isinstance(session, RecVar):
        return RecVar(var_name=session.var_name)
    elif isinstance(session, Choice):
        # Invert choice type: internal (+) becomes external (&) and vice versa
        dual_branches = {label: dual(target) for label, target in session.branches.items()}
        return Choice(is_external=not session.is_external, branches=dual_branches)
    else:
        raise TypeError(f"Unknown session type node: {type(session)}")
