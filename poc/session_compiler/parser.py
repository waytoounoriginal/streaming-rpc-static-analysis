"""
parser.py — Lexer and Recursive-Descent Parser for Binary Session Types

Supports:
  - Sequence: !Msg; ?Msg; end
  - Payload guards: !Request[decode_config]; !Request[audio_data]
  - Recursion: rec X. (&{recv_info: ?FlightInfo; X, recv_eof: end})
  - Internal Choice: +{ok: !Chunk; S1, abort: WritesDone; end}
  - External Choice: &{recv_info: ?FlightInfo; X, recv_eof: end}
  - Half-close & Lifecycle: WritesDone, Finish, end
"""

import re
from typing import List, Tuple, Optional
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
)


class Lexer:
    TOKEN_SPEC = [
        ("WS", r"\s+"),
        ("BANG", r"!"),
        ("QUESTION", r"\?"),
        ("SEMICOLON", r";"),
        ("DOT", r"\."),
        ("COMMA", r","),
        ("COLON", r":"),
        ("LPAREN", r"\("),
        ("RPAREN", r"\)"),
        ("LBRACKET", r"\["),
        ("RBRACKET", r"\]"),
        ("LBRACE", r"\{"),
        ("RBRACE", r"\}"),
        ("PLUS", r"\+"),
        ("AMPERSAND", r"&"),
        ("IDENT", r"[a-zA-Z_][a-zA-Z0-9_]*"),
    ]
    TOKEN_REGEX = re.compile("|".join(f"(?P<{name}>{pattern})" for name, pattern in TOKEN_SPEC))

    @classmethod
    def tokenize(cls, text: str) -> List[Tuple[str, str, int]]:
        tokens = []
        for match in cls.TOKEN_REGEX.finditer(text):
            kind = match.lastgroup
            val = match.group()
            pos = match.start()
            if kind == "WS":
                continue
            tokens.append((kind, val, pos))
        tokens.append(("EOF", "", len(text)))
        return tokens


class Parser:
    def __init__(self, text: str):
        self.text = text
        self.tokens = Lexer.tokenize(text)
        self.pos = 0

    def current(self) -> Tuple[str, str, int]:
        return self.tokens[self.pos]

    def peek_kind(self) -> str:
        return self.tokens[self.pos][0]

    def peek_val(self) -> str:
        return self.tokens[self.pos][1]

    def advance(self) -> Tuple[str, str, int]:
        tok = self.current()
        if tok[0] != "EOF":
            self.pos += 1
        return tok

    def expect(self, kind: str, val: Optional[str] = None) -> Tuple[str, str, int]:
        curr = self.current()
        if curr[0] != kind or (val is not None and curr[1] != val):
            expected = f"{kind}('{val}')" if val else kind
            got = f"{curr[0]}('{curr[1]}')"
            raise SyntaxError(
                f"Syntax error at position {curr[2]}: Expected {expected}, got {got}\n"
                f"Context: {self.text[max(0, curr[2]-15):curr[2]+20]}"
            )
        return self.advance()

    def parse(self) -> SessionType:
        res = self.parse_session()
        self.expect("EOF")
        return res

    def parse_session(self) -> SessionType:
        left = self.parse_atomic_session()
        if self.peek_kind() == "SEMICOLON":
            self.advance()  # consume ';'
            if self.peek_kind() == "EOF" or self.peek_kind() in ("RPAREN", "RBRACE", "COMMA"):
                return left
            right = self.parse_session()
            return self._attach_continuation(left, right)
        return left

    def _attach_continuation(self, prefix: SessionType, continuation: SessionType) -> SessionType:
        """Attaches a right-hand continuation session to the tail of a sequential prefix."""
        if isinstance(prefix, (Send, Receive, WritesDoneOp, FinishOp)):
            if prefix.next_session is None:
                prefix.next_session = continuation
                return prefix
            else:
                prefix.next_session = self._attach_continuation(prefix.next_session, continuation)
                return prefix
        elif isinstance(prefix, EndOp):
            raise SyntaxError("Cannot sequence operations after 'end'")
        elif isinstance(prefix, RecVar):
            raise SyntaxError(f"Cannot sequence operations after recursive variable '{prefix.var_name}'")
        elif isinstance(prefix, Recursion):
            # Sequence continuation onto the body
            prefix.body = self._attach_continuation(prefix.body, continuation)
            return prefix
        elif isinstance(prefix, Choice):
            for k in prefix.branches:
                prefix.branches[k] = self._attach_continuation(prefix.branches[k], continuation)
            return prefix
        return prefix

    def parse_atomic_session(self) -> SessionType:
        kind = self.peek_kind()
        val = self.peek_val()

        # 1. Send: !MsgType or !MsgType[guard]
        if kind == "BANG":
            self.advance()
            msg_type = self.expect("IDENT")[1]
            guard_tag = None
            if self.peek_kind() == "LBRACKET":
                self.advance()
                guard_tag = self.expect("IDENT")[1]
                self.expect("RBRACKET")
            return Send(msg_type=msg_type, guard_tag=guard_tag)

        # 2. Receive: ?MsgType or ?MsgType[guard]
        elif kind == "QUESTION":
            self.advance()
            msg_type = self.expect("IDENT")[1]
            guard_tag = None
            if self.peek_kind() == "LBRACKET":
                self.advance()
                guard_tag = self.expect("IDENT")[1]
                self.expect("RBRACKET")
            return Receive(msg_type=msg_type, guard_tag=guard_tag)

        # 3. Recursion: rec X. S or mu X. S
        elif kind == "IDENT" and val in ("rec", "mu"):
            self.advance()
            var_name = self.expect("IDENT")[1]
            self.expect("DOT")
            body = self.parse_session()
            return Recursion(var_name=var_name, body=body)

        # 4. Lifecycle primitives: WritesDone, Finish, end
        elif kind == "IDENT" and val == "WritesDone":
            self.advance()
            return WritesDoneOp()
        elif kind == "IDENT" and val == "Finish":
            self.advance()
            return FinishOp()
        elif kind == "IDENT" and val == "end":
            self.advance()
            return EndOp()

        # 5. Choices: +{...} (internal) or &{...} (external)
        elif kind in ("PLUS", "AMPERSAND"):
            is_external = (kind == "AMPERSAND")
            self.advance()
            self.expect("LBRACE")
            branches = {}
            while self.peek_kind() != "RBRACE":
                label = self.expect("IDENT")[1]
                self.expect("COLON")
                branch_session = self.parse_session()
                branches[label] = branch_session
                if self.peek_kind() == "COMMA":
                    self.advance()
                else:
                    break
            self.expect("RBRACE")
            return Choice(is_external=is_external, branches=branches)

        # 6. Grouping parentheses: (S)
        elif kind == "LPAREN":
            self.advance()
            inner = self.parse_session()
            self.expect("RPAREN")
            return inner

        # 7. Recursive variable reference: X
        elif kind == "IDENT":
            self.advance()
            return RecVar(var_name=val)

        else:
            raise SyntaxError(
                f"Unexpected token {kind}('{val}') at pos {self.current()[2]} while parsing session type."
            )


def parse_session_type(spec_string: str) -> SessionType:
    """Parses a binary session type expression into an AST."""
    parser = Parser(spec_string.strip())
    return parser.parse()
