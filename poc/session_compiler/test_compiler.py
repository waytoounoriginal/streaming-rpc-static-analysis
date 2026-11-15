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
        proto_file = POC_DIR / "regression" / "real_repo_bugs" / "real_repos.proto"
        specs = extract_session_specs_from_proto(proto_file)
        self.assertGreaterEqual(len(specs), 6)
        methods = {s["method"] for s in specs}
        expected = {"ListFlights", "Handshake", "StageTree", "SayHelloStream", "ModelStreamInfer", "BidiWriteObject"}
        self.assertTrue(expected.issubset(methods))

    def test_clang_analyzer_integration(self):
        """Validates that compiler-generated FSM JSON works 100% with ClangSessionChecker."""
        proto_file = POC_DIR / "regression" / "real_repo_bugs" / "real_repos.proto"
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
        good_file = POC_DIR / "regression" / "real_repo_bugs" / "arrow_result_stream_good.cpp"
        p_good = checker.analyze_file(good_file)
        self.assertTrue(p_good)

        # Test 2: Buggy implementation caught
        bad_file = POC_DIR / "regression" / "real_repo_bugs" / "arrow_list_flights_buggy.cpp"
        p_bad = checker.analyze_file(bad_file)
        self.assertFalse(p_bad)
        self.assertGreater(len(checker.diagnostics), 0)

    def test_dual_server_fsm_no_double_flip(self):
        """Client !Req; ?Ack; rec X. (+{next: !Req; X, stop: WritesDone; ?Ack; end}) dualizes cleanly for server."""
        spec = "!Req; ?Ack; rec X. (+{next: !Req; X, stop: WritesDone; ?Ack; end})"
        ast = parse_session_type(spec)
        client_fsm = compile_protocol_fsm(
            ast, "demo", "Handshake", "Exchange", "BIDIRECTIONAL_STREAMING",
            "demo.Req", "demo.Ack", role="client"
        )
        server_fsm = compile_protocol_fsm(
            ast, "demo", "Handshake", "Exchange", "BIDIRECTIONAL_STREAMING",
            "demo.Req", "demo.Ack", role="server"
        )
        # Client S0 writes, Server S0 reads (no double-flip), and PeerEndOfInput separates pre/post half-close
        self.assertEqual([t["op"] for t in client_fsm["states"][client_fsm["initial_state"]]["transitions"]], ["Write"])
        self.assertEqual([t["op"] for t in server_fsm["states"][server_fsm["initial_state"]]["transitions"]], ["Read"])
        s1 = server_fsm["states"][server_fsm["initial_state"]]["transitions"][0]["target"]
        self.assertEqual([t["op"] for t in server_fsm["states"][s1]["transitions"]], ["Write"])
        s2 = server_fsm["states"][s1]["transitions"][0]["target"]
        s2_trans = server_fsm["states"][s2]["transitions"]
        self.assertEqual([(t["op"], t.get("outcome")) for t in s2_trans], [("Read", "ok"), ("Read", "eos")])
        s3 = next(t["target"] for t in s2_trans if t.get("outcome") == "eos")
        self.assertFalse(server_fsm["states"][s3]["terminal"])
        self.assertEqual([t["op"] for t in server_fsm["states"][s3]["transitions"]], ["Write"])
        s4 = server_fsm["states"][s3]["transitions"][0]["target"]
        self.assertFalse(server_fsm["states"][s4]["terminal"])
        self.assertEqual([t["op"] for t in server_fsm["states"][s4]["transitions"]], ["ServerReturn"])
        s5 = server_fsm["states"][s4]["transitions"][0]["target"]
        self.assertTrue(server_fsm["states"][s5]["terminal"])
        term_states = [k for k, v in server_fsm["states"].items() if v["terminal"]]
        self.assertEqual(term_states, [s5])

    def test_determinism_allows_read_ok_vs_eos(self):
        """Compiler distinguishes Read(ok) from Read(eos) on the same state while rejecting ambiguous duplicates."""
        ok_spec = parse_session_type("rec X. (&{ack: ?Resp; X, eof: end})")
        fsm = compile_protocol_fsm(ok_spec, "d", "S", "M", "BIDIRECTIONAL_STREAMING", "d.Req", "d.Resp", role="client")
        self.assertEqual([(t["op"], t.get("outcome"), t["target"]) for t in fsm["states"]["S0"]["transitions"]],
                         [("Read", "ok", "S0"), ("Read", "eos", "S1")])
        bad_spec = parse_session_type("&{a: ?Resp; ?Resp; end, b: ?Resp; end}")
        with self.assertRaises(ValueError):
            compile_protocol_fsm(bad_spec, "d", "S", "M", "BIDIRECTIONAL_STREAMING", "d.Req", "d.Resp", role="client")


    def test_presence_aliases_and_multistep_recvar(self):
        """Covers multi-step RecVar loopbacks, optional clear_* presence guards, and method_aliases."""
        spec_str = "rec X. (+{infer: !Req; ?Resp; X, stop: WritesDone; end})"
        ast = parse_session_type(spec_str)
        fsm = compile_protocol_fsm(
            ast, "t", "Svc", "Call", "BIDIRECTIONAL_STREAMING", "t.Req", "t.Resp",
            role="client", method_aliases=["CallAlias"]
        )
        self.assertEqual(fsm["protocol"].get("method_aliases"), ["CallAlias"])
        # In S0: Write -> S1, WritesDone -> S2; in S1: Read must loop directly back to S0 (no dead-end)
        s0_write_tgt = next(t["target"] for t in fsm["states"]["S0"]["transitions"] if t["op"] == "Write")
        s1_trans = fsm["states"][s0_write_tgt]["transitions"]
        self.assertEqual([(t["op"], t["target"]) for t in s1_trans], [("Read", "S0")])

        real_proto = POC_DIR / "regression" / "real_repo_bugs" / "real_repos.proto"
        specs = extract_session_specs_from_proto(real_proto)
        storage = next(s for s in specs if s["method"] == "BidiWriteObject")
        self.assertEqual(storage.get("method_aliases"), ["WriteObject"])
        pm = storage.get("payload_mappings") or {}
        self.assertIn("mutable_write_object_spec", pm)
        self.assertIn("clear_write_object_spec", pm)
        self.assertNotIn("write_object_spec", pm)

if __name__ == "__main__":
    unittest.main()
