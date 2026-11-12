"""
test_compiler.py — Unit & Integration Test Suite for Protobuf Session Type Compiler
Part of Bachelor Thesis: "Static Analysis of Streaming RPC Protocols in C++: A Behavioural Types Approach"
Author: Mihai Tira
"""

import sys
import unittest
from pathlib import Path

POC_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(POC_DIR))

from session_compiler.ast_nodes import (
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
from session_compiler.parser import parse_session_type
from session_compiler.fsm_builder import compile_protocol_fsm
from session_compiler.proto_extractor import extract_session_specs_from_proto
from grpc_session_check import ClangSessionChecker


class TestSessionCompiler(unittest.TestCase):
    def test_lexer_and_parser_sequences(self):
        spec = "!HandshakeRequest; ?HandshakeResponse; WritesDone; end"
        ast = parse_session_type(spec)
        self.assertIsInstance(ast, Send)
        self.assertEqual(ast.msg_type, "HandshakeRequest")
        self.assertIsInstance(ast.next_session, Receive)
        self.assertEqual(ast.next_session.msg_type, "HandshakeResponse")
        self.assertIsInstance(ast.next_session.next_session, WritesDoneOp)

    def test_payload_guards_parsing(self):
        spec = "!Request[decode_config]; !Request[audio_data]; end"
        ast = parse_session_type(spec)
        self.assertEqual(ast.guard_tag, "decode_config")
        self.assertEqual(ast.next_session.guard_tag, "audio_data")

    def test_recursion_and_choices(self):
        spec = "rec X. (&{recv_info: ?FlightInfo; X, recv_eof: end})"
        ast = parse_session_type(spec)
        self.assertIsInstance(ast, Recursion)
        self.assertEqual(ast.var_name, "X")
        self.assertIsInstance(ast.body, Choice)
        self.assertTrue(ast.body.is_external)
        self.assertIn("recv_info", ast.body.branches)
        self.assertIn("recv_eof", ast.body.branches)

    def test_duality_involution(self):
        """Formal Duality Identity: dual(dual(S)) == S."""
        specs = [
            "!Req; ?Resp; WritesDone; end",
            "rec X. (&{recv: ?Data; X, done: end})",
            "!Config[p1]; rec X. (+{chunk: !Chunk[p2]; X, stop: WritesDone; ?Ack; end})",
        ]
        for spec in specs:
            ast = parse_session_type(spec)
            d1 = dual(ast)
            d2 = dual(d1)
            self.assertEqual(ast.to_string(), d2.to_string())

    def test_proto_extraction(self):
        proto_file = POC_DIR / "real_repo_bugs" / "real_repos.proto"
        specs = extract_session_specs_from_proto(proto_file)
        self.assertGreaterEqual(len(specs), 6)
        methods = {s["method"] for s in specs}
        expected = {"ListFlights", "Handshake", "StageTree", "SayHelloStream", "ModelStreamInfer", "BidiWriteObject"}
        self.assertTrue(expected.issubset(methods))

    def test_clang_analyzer_integration(self):
        """Validates that compiler-generated FSM JSON works 100% with ClangSessionChecker."""
        proto_file = POC_DIR / "real_repo_bugs" / "real_repos.proto"
        specs = extract_session_specs_from_proto(proto_file)
        list_flights_spec = next(s for s in specs if s["method"] == "ListFlights")

        ast = parse_session_type(list_flights_spec["session_spec"])
        contract = compile_protocol_fsm(
            session_ast=ast,
            package=list_flights_spec["package"],
            service=list_flights_spec["service"],
            method="ListFlights",
            rpc_type=list_flights_spec["rpc_type"],
            client_stream_type=list_flights_spec["client_stream_type"],
            server_stream_type=list_flights_spec["server_stream_type"],
            role="client"
        )

        checker = ClangSessionChecker(spec_dict=contract)
        # Test 1: Conforming implementation passes
        good_file = POC_DIR / "real_repo_bugs" / "arrow_result_stream_good.cpp"
        p_good = checker.analyze_file(good_file)
        self.assertTrue(p_good)

        # Test 2: Buggy implementation caught
        bad_file = POC_DIR / "real_repo_bugs" / "arrow_list_flights_buggy.cpp"
        p_bad = checker.analyze_file(bad_file)
        self.assertFalse(p_bad)
        self.assertGreater(len(checker.diagnostics), 0)


if __name__ == "__main__":
    unittest.main()
