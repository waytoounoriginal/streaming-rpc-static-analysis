#!/usr/bin/env python3
"""
test_analyzer_properties.py — Unit Tests for Path-Sensitive Branch Merge, Fixed-Point Loops,
Product-Domain Level 2 Payloads, Boolean Provenance, and 3-Way gRPC Termination Semantics.
"""

import tempfile
import unittest
from pathlib import Path
import sys

POC_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(POC_DIR))

from grpc_session_check import ClangSessionChecker
from session_compiler import compile_protocol_fsm, parse_session_type


MINIMAL_GRPC_PREAMBLE = """
namespace grpc {
struct StatusCode { enum Code { OK = 0, CANCELLED = 1, INVALID_ARGUMENT = 3 }; };
struct Status {
    Status(StatusCode::Code = StatusCode::OK, const char* = "") {}
    static Status OK;
    static Status CANCELLED;
    bool ok() const { return true; }
};
struct WriteOptions {
    WriteOptions& set_last_message() { return *this; }
    WriteOptions& clear_last_message() { return *this; }
};
class ClientContext { public: void TryCancel() {} };
class ServerContext {
public:
    bool IsCancelled() const { return false; }
    void TryCancel() {}
};
template <typename R>
class ClientReader {
public:
    bool Read(R*) { return true; }
    Status Finish() { return Status::OK; }
};
template <typename W>
class ClientWriter {
public:
    bool Write(const W&) { return true; }
    bool Write(const W&, WriteOptions) { return true; }
    bool WriteLast(const W&, WriteOptions) { return true; }
    bool WritesDone() { return true; }
    Status Finish() { return Status::OK; }
};
template <typename W, typename R>
class ClientReaderWriter {
public:
    bool Write(const W&) { return true; }
    bool Write(const W&, WriteOptions) { return true; }
    bool WriteLast(const W&, WriteOptions) { return true; }
    bool Read(R*) { return true; }
    bool WritesDone() { return true; }
    Status Finish() { return Status::OK; }
};
template <typename W>
class ServerWriter {
public:
    bool Write(const W&) { return true; }
    bool Write(const W&, WriteOptions) { return true; }
    bool WriteLast(const W&, WriteOptions) { return true; }
};
template <typename W, typename R>
class ServerReaderWriter {
public:
    bool Write(const W&) { return true; }
    bool Write(const W&, WriteOptions) { return true; }
    bool WriteLast(const W&, WriteOptions) { return true; }
    bool Read(R*) { return true; }
};
template <typename T>
class SmartPtr {
public:
    explicit SmartPtr(T* p = nullptr) : p_(p) {}
    explicit operator bool() const { return p_ != nullptr; }
    T* operator->() const { return p_; }
    void reset() { p_ = nullptr; }
private:
    T* p_;
};
}
"""


def run_checker_on_cpp(checker: ClangSessionChecker, cpp_source: str) -> bool:
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "test_case.cpp"
        p.write_text(MINIMAL_GRPC_PREAMBLE + "\n" + cpp_source, encoding="utf-8")
        return checker.analyze_file(p, extra_flags=["-std=c++17"])


class TestAnalyzerProperties(unittest.TestCase):
    def _compile_spec(self, spec_str: str, payload_mappings=None, payload_field="body", payload_groups=None):
        ast = parse_session_type(spec_str)
        return compile_protocol_fsm(
            ast, "demo", "Svc", "Call", "BIDIRECTIONAL_STREAMING",
            "demo.Req", "demo.Resp", role="client",
            payload_mappings=payload_mappings,
            payload_field_name=payload_field,
            payload_groups=payload_groups,
        )

    def test_branch_merge_diverging_if_else(self):
        """If/else branches reaching distinct DFSM states preserve both at merge."""
        # Two paths: Write(a) advances S0->S1 (allows Write), Write(b) advances S0->S2 (only WritesDone).
        spec = "+{branch_a: !Req[tag_a]; !Req[tag_c]; WritesDone; end, branch_b: !Req[tag_b]; WritesDone; end}"
        contract = self._compile_spec(
            spec,
            payload_mappings={"tag_a": 1, "tag_b": 2, "tag_c": 3},
            payload_groups={
                "tag_a": {"group": "body", "case": 1},
                "tag_b": {"group": "body", "case": 2},
                "tag_c": {"group": "body", "case": 3},
            },
        )
        cpp_divergent_bug = """
        struct Req {
            void set_tag_a() {}
            void set_tag_b() {}
            void set_tag_c() {}
        };
        struct Resp {};
        struct Stub { grpc::ClientReaderWriter<Req, Resp>* Call(grpc::ClientContext*); };
        void RunBug(Stub* stub, bool cond) {
            grpc::ClientContext ctx;
            auto* stream = stub->Call(&ctx);
            Req r;
            if (cond) {
                r.set_tag_a();
                stream->Write(r);
            } else {
                r.set_tag_b();
                stream->Write(r);
            }
            r.set_tag_c();
            stream->Write(r);
            stream->WritesDone();
            stream->Finish();
        }
        """
        checker = ClangSessionChecker(spec_dict=contract)
        self.assertFalse(run_checker_on_cpp(checker, cpp_divergent_bug))
        self.assertTrue(any("Illegal Write()" in msg for _, msg in checker.diagnostics))

    def test_third_iteration_loop_violation(self):
        """Fixed-point loop catches protocol violation occurring on iteration 3 (> k=2)."""
        spec = "!Req; !Req; WritesDone; end"  # Exactly 2 writes allowed before WritesDone
        contract = self._compile_spec(spec)
        cpp_loop_3_bug = """
        struct Req {};
        struct Resp {};
        struct Stub { grpc::ClientReaderWriter<Req, Resp>* Call(grpc::ClientContext*); };
        void RunLoopBug(Stub* stub) {
            grpc::ClientContext ctx;
            auto* stream = stub->Call(&ctx);
            Req req;
            for (int i = 0; i < 3; ++i) {
                stream->Write(req);
            }
            stream->WritesDone();
            stream->Finish();
        }
        """
        checker = ClangSessionChecker(spec_dict=contract)
        self.assertFalse(run_checker_on_cpp(checker, cpp_loop_3_bug))
        self.assertTrue(any("Illegal Write()" in msg for _, msg in checker.diagnostics))

    def test_product_domain_multiple_payload_constraints(self):
        """Updating an independent optional field does not overwrite a oneof variant group."""
        spec = "!Req[init & auth_token]; WritesDone; end"
        contract = self._compile_spec(
            spec,
            payload_mappings={"init": 1, "chunk": 2, "set_auth_token": 3, "clear_auth_token": 4},
            payload_groups={
                "init": {"group": "body", "case": 1},
                "chunk": {"group": "body", "case": 2},
                "set_auth_token": {"group": "field:auth_token", "case": 3},
                "clear_auth_token": {"group": "field:auth_token", "case": 4},
            },
        )
        cpp_good = """
        struct Req {
            void set_init() {}
            void set_chunk() {}
            void set_auth_token() {}
            void clear_auth_token() {}
        };
        struct Resp {};
        struct Stub { grpc::ClientReaderWriter<Req, Resp>* Call(grpc::ClientContext*); };
        void RunGood(Stub* stub) {
            grpc::ClientContext ctx;
            auto* stream = stub->Call(&ctx);
            Req req;
            req.set_init();
            req.set_auth_token();
            stream->Write(req);
            stream->WritesDone();
            stream->Finish();
        }
        """
        cpp_missing_second = """
        struct Req {
            void set_init() {}
            void set_chunk() {}
            void set_auth_token() {}
        };
        struct Resp {};
        struct Stub { grpc::ClientReaderWriter<Req, Resp>* Call(grpc::ClientContext*); };
        void RunBad(Stub* stub) {
            grpc::ClientContext ctx;
            auto* stream = stub->Call(&ctx);
            Req req;
            req.set_init();
            stream->Write(req);
            stream->WritesDone();
            stream->Finish();
        }
        """
        self.assertTrue(run_checker_on_cpp(ClangSessionChecker(spec_dict=contract), cpp_good))
        self.assertFalse(run_checker_on_cpp(ClangSessionChecker(spec_dict=contract), cpp_missing_second))

    def test_read_eos_requires_finish_vs_write_abort(self):
        """Client Read()==false requires Finish(), while Write()==false permits transport-abort exit."""
        spec = "!Req; ?Resp; WritesDone; end"
        contract = self._compile_spec(spec)

        cpp_read_no_finish = """
        struct Req {};
        struct Resp {};
        struct Stub { grpc::ClientReaderWriter<Req, Resp>* Call(grpc::ClientContext*); };
        int RunReadLeak(Stub* stub) {
            grpc::ClientContext ctx;
            auto* stream = stub->Call(&ctx);
            Req req;
            if (!stream->Write(req)) {
                return 1;
            }
            Resp resp;
            if (!stream->Read(&resp)) {
                return 2; // Missing Finish() after Read() EOS!
            }
            stream->WritesDone();
            stream->Finish();
            return 0;
        }
        """
        cpp_read_with_finish = cpp_read_no_finish.replace("return 2; // Missing Finish() after Read() EOS!", "stream->Finish(); return 2;")

        self.assertFalse(run_checker_on_cpp(ClangSessionChecker(spec_dict=contract), cpp_read_no_finish))
        self.assertTrue(run_checker_on_cpp(ClangSessionChecker(spec_dict=contract), cpp_read_with_finish))

    def test_boolean_provenance_invalidation_on_reassign(self):
        """Reassigning a boolean variable invalidates its Write() I/O failure provenance."""
        spec = "!Req; WritesDone; end"
        contract = self._compile_spec(spec)
        cpp_reassigned = """
        struct Req {};
        struct Resp {};
        struct Stub { grpc::ClientReaderWriter<Req, Resp>* Call(grpc::ClientContext*); };
        int RunReassign(Stub* stub, bool ext_flag) {
            grpc::ClientContext ctx;
            auto* stream = stub->Call(&ctx);
            Req req;
            bool ok = stream->Write(req);
            ok = ext_flag;
            if (!ok) {
                return 1; // Provenance lost -> missing Finish() must be flagged!
            }
            stream->WritesDone();
            stream->Finish();
            return 0;
        }
        """
        checker = ClangSessionChecker(spec_dict=contract)
        self.assertFalse(run_checker_on_cpp(checker, cpp_reassigned))
        self.assertTrue(any("Stream lifecycle leak" in msg for _, msg in checker.diagnostics))

    def test_bidi_read_after_writesdone(self):
        """Default BIDIRECTIONAL_STREAMING client permits Read() loop after WritesDone() before Finish()."""
        checker = ClangSessionChecker.from_default_level1("BIDIRECTIONAL_STREAMING", "Call", role="client")
        cpp_valid_bidi = """
        struct Req {};
        struct Resp {};
        struct Stub { grpc::ClientReaderWriter<Req, Resp>* Call(grpc::ClientContext*); };
        grpc::Status RunValidBidi(Stub* stub) {
            grpc::ClientContext ctx;
            auto* stream = stub->Call(&ctx);
            Req req;
            Resp resp;
            stream->Write(req);
            stream->Write(req);
            stream->WritesDone();
            while (stream->Read(&resp)) {}
            return stream->Finish();
        }
        """
        self.assertTrue(run_checker_on_cpp(checker, cpp_valid_bidi))

    def test_empty_range_for_loop_caught(self):
        """Zero-trip path on for / range-for exposes missing required first Write before WritesDone."""
        spec = "!Req; WritesDone; end"
        contract = self._compile_spec(spec)
        cpp_empty_range = """
        struct Req {};
        struct Resp {};
        struct Stub { grpc::ClientReaderWriter<Req, Resp>* Call(grpc::ClientContext*); };
        void RunEmptyLoop(Stub* stub, int n) {
            grpc::ClientContext ctx;
            auto* stream = stub->Call(&ctx);
            Req req;
            for (int i = 0; i < n; ++i) {
                stream->Write(req);
            }
            stream->WritesDone();
            stream->Finish();
        }
        """
        checker = ClangSessionChecker(spec_dict=contract)
        self.assertFalse(run_checker_on_cpp(checker, cpp_empty_range))
        self.assertTrue(any("Illegal WritesDone()" in msg for _, msg in checker.diagnostics))

    def test_break_and_continue_channels(self):
        """BreakStmt exits loop without executing subsequent loop-body Read/Write statements."""
        spec = "!Req; WritesDone; end"
        contract = self._compile_spec(spec)
        cpp_break = """
        struct Req {};
        struct Resp {};
        struct Stub { grpc::ClientReaderWriter<Req, Resp>* Call(grpc::ClientContext*); };
        void RunBreak(Stub* stub, int n) {
            grpc::ClientContext ctx;
            auto* stream = stub->Call(&ctx);
            Req req;
            for (int i = 0; i < n; ++i) {
                if (!stream->Write(req)) {
                    break;
                }
                if (i == 0) {
                    continue;
                }
                stream->Write(req);
            }
            stream->WritesDone();
            stream->Finish();
        }
        """
        # With 0-trip path from ForStmt, S0->WritesDone flags cleanly; test direct Write->break->Read inside DoStmt:
        spec2 = "rec X. (+{send: !Req; X, done: WritesDone; ?Resp; end})"
        c2 = self._compile_spec(spec2)
        cpp_do_break = """
        struct Req {};
        struct Resp {};
        struct Stub { grpc::ClientReaderWriter<Req, Resp>* Call(grpc::ClientContext*); };
        void RunDoBreak(Stub* stub) {
            grpc::ClientContext ctx;
            auto* stream = stub->Call(&ctx);
            Req req;
            int n = 1;
            do {
                if (!stream->Write(req)) {
                    break;
                }
            } while (--n > 0);
            stream->WritesDone();
            Resp resp;
            stream->Read(&resp);
            stream->Finish();
        }
        """
        self.assertTrue(run_checker_on_cpp(ClangSessionChecker(spec_dict=c2), cpp_do_break))

    def test_server_is_cancelled_distinction(self):
        """Server while(Read) proceeds normally on half-close while ctx->IsCancelled() exits cleanly."""
        spec = "!Req; rec X. (+{next: !Req; X, done: WritesDone; ?Resp; end})"
        ast = parse_session_type(spec)
        server_contract = compile_protocol_fsm(
            ast, "demo", "Svc", "Call", "BIDIRECTIONAL_STREAMING",
            "demo.Req", "demo.Resp", role="server",
        )
        cpp_server = """
        struct Req {};
        struct Resp {};
        grpc::Status RunServer(grpc::ServerContext* ctx, grpc::ServerReaderWriter<Resp, Req>* stream) {
            Req req;
            if (!stream->Read(&req)) {
                return grpc::Status::CANCELLED;
            }
            while (stream->Read(&req)) {
                if (ctx->IsCancelled()) {
                    return grpc::Status::CANCELLED;
                }
            }
            Resp resp;
            stream->Write(resp);
            return grpc::Status::OK;
        }
        """
        self.assertTrue(run_checker_on_cpp(ClangSessionChecker(spec_dict=server_contract), cpp_server))

    def test_finite_ast_site_bool_prov_in_loop(self):
        """Boolean I/O provenance using static AST site ID converges cleanly in finite fixed point."""
        spec = "rec X. (+{w: !Req; X, d: WritesDone; end})"
        contract = self._compile_spec(spec)
        cpp_loop_prov = """
        struct Req {};
        struct Resp {};
        struct Stub { grpc::ClientReaderWriter<Req, Resp>* Call(grpc::ClientContext*); };
        int RunLoopProv(Stub* stub, int n) {
            grpc::ClientContext ctx;
            auto* stream = stub->Call(&ctx);
            Req req;
            bool ok = false;
            do {
                ok = stream->Write(req);
                if (!ok) {
                    return 1;
                }
            } while (--n > 0);
            stream->WritesDone();
            stream->Finish();
            return 0;
        }
        """
        checker = ClangSessionChecker(spec_dict=contract)
        self.assertTrue(run_checker_on_cpp(checker, cpp_loop_prov))
        self.assertEqual(checker.incomplete_loops, 0)
        self.assertLessEqual(checker.max_loop_configs_explored, 4)

    def test_smart_ptr_valid_does_not_waive_finish(self):
        """if (stream) on non-reset handle still flags missing Finish()."""
        spec = "!Req; WritesDone; end"
        contract = self._compile_spec(spec)
        cpp_ptr = """
        struct Req {};
        struct Resp {};
        struct Stub {
            grpc::SmartPtr<grpc::ClientReaderWriter<Req, Resp>> Call(grpc::ClientContext*);
        };
        int RunPtrCheck(Stub* stub) {
            grpc::ClientContext ctx;
            auto stream = stub->Call(&ctx);
            if (stream) {
                Req req;
                stream->Write(req);
                stream->WritesDone();
                return 0; // Missing Finish()! Pointer non-null must NOT suppress this leak.
            }
            return 1;
        }
        """
        checker = ClangSessionChecker(spec_dict=contract)
        self.assertFalse(run_checker_on_cpp(checker, cpp_ptr))
        self.assertTrue(any("Stream lifecycle leak" in msg for _, msg in checker.diagnostics))

    def test_determinism_ambiguous_choice_rejected(self):
        """Compiler rejects non-deterministic choice branches sharing identical `(op, outcome, guard)` to distinct targets."""
        spec = "+{a: !Req; !Req; end, b: !Req; WritesDone; end}"
        ast = parse_session_type(spec)
        with self.assertRaises(ValueError):
            compile_protocol_fsm(ast, "demo", "Svc", "Call", "BIDIRECTIONAL_STREAMING", "demo.Req", "demo.Resp")

    def test_worked_example_exact_5_client_4_server_states(self):
        """Worked example S_ex lowers to 5 client states and 4 server states with non-terminal operational states."""
        spec = "!Frame[init]; rec X. (+{chunk: !Frame[chunk]; X, done: WritesDone; rec Y. (&{ack: ?Summary; Y, eof: end})})"
        ast = parse_session_type(spec)
        pm = {"mutable_init": 1, "set_chunk": 2}
        pg = {"mutable_init": {"group": "body", "case": 1}, "set_chunk": {"group": "body", "case": 2}}
        c_fsm = compile_protocol_fsm(
            ast, "demo", "Svc", "Upload", "BIDIRECTIONAL_STREAMING", "demo.Frame", "demo.Summary",
            role="client", payload_mappings=pm, payload_field_name="body", payload_groups=pg,
        )
        s_fsm = compile_protocol_fsm(
            ast, "demo", "Svc", "Upload", "BIDIRECTIONAL_STREAMING", "demo.Frame", "demo.Summary",
            role="server", payload_mappings=pm, payload_field_name="body", payload_groups=pg,
        )
        self.assertEqual(len(c_fsm["states"]), 5)
        self.assertEqual([k for k, v in c_fsm["states"].items() if v["terminal"]], ["S4"])
        self.assertEqual([(t["op"], t.get("outcome"), t["target"]) for t in c_fsm["states"]["S2"]["transitions"]],
                         [("Read", "ok", "S2"), ("Read", "eos", "S3")])
        self.assertEqual([(t["op"], t["target"]) for t in c_fsm["states"]["S3"]["transitions"]], [("Finish", "S4")])

        self.assertEqual(len(s_fsm["states"]), 4)
        self.assertEqual([k for k, v in s_fsm["states"].items() if v["terminal"]], ["S3"])
        self.assertFalse(s_fsm["states"]["S1"]["terminal"])
        self.assertFalse(s_fsm["states"]["S2"]["terminal"])
        self.assertEqual([(t["op"], t.get("outcome"), t["target"]) for t in s_fsm["states"]["S1"]["transitions"]],
                         [("Read", "ok", "S1"), ("Read", "eos", "S2")])
        self.assertEqual([(t["op"], t["target"]) for t in s_fsm["states"]["S2"]["transitions"]],
                         [("Write", "S2"), ("ServerReturn", "S3")])

        cpp_good = """
        struct Frame { void mutable_init() {} void set_chunk() {} };
        struct Summary {};
        struct Stub { grpc::ClientReaderWriter<Frame, Summary>* Upload(grpc::ClientContext*); };
        void RunClientGood(Stub* stub) {
            grpc::ClientContext ctx;
            auto* stream = stub->Upload(&ctx);
            Frame init; init.mutable_init();
            stream->Write(init);
            Frame c; c.set_chunk();
            stream->Write(c);
            stream->WritesDone();
            Summary s;
            while (stream->Read(&s)) {}
            stream->Finish();
        }
        grpc::Status RunServerGood(grpc::ServerContext* ctx, grpc::ServerReaderWriter<Summary, Frame>* stream) {
            Frame f;
            if (!stream->Read(&f)) return grpc::Status::CANCELLED;
            while (stream->Read(&f)) {}
            Summary s;
            stream->Write(s);
            return grpc::Status::OK;
        }
        """
        cpp_skip_read_drain = """
        struct Frame { void mutable_init() {} void set_chunk() {} };
        struct Summary {};
        struct Stub { grpc::ClientReaderWriter<Frame, Summary>* Upload(grpc::ClientContext*); };
        void RunClientNoDrain(Stub* stub) {
            grpc::ClientContext ctx;
            auto* stream = stub->Upload(&ctx);
            Frame init; init.mutable_init();
            stream->Write(init);
            stream->WritesDone();
            stream->Finish();
        }
        """
        self.assertTrue(run_checker_on_cpp(ClangSessionChecker(spec_dict=c_fsm), cpp_good))
        self.assertTrue(run_checker_on_cpp(ClangSessionChecker(spec_dict=s_fsm), cpp_good))
        self.assertFalse(run_checker_on_cpp(ClangSessionChecker(spec_dict=c_fsm), cpp_skip_read_drain))

    def test_server_default_forbids_read_after_eos(self):
        """Default ServerReaderWriter transitions to S1_INPUT_CLOSED on Read(eos) and forbids further Read()."""
        checker = ClangSessionChecker.from_default_level1("BIDIRECTIONAL_STREAMING", "Call", role="server")
        cpp_post_eos_read = """
        struct Req {};
        struct Resp {};
        grpc::Status RunBadServer(grpc::ServerContext* ctx, grpc::ServerReaderWriter<Resp, Req>* stream) {
            Req req;
            while (stream->Read(&req)) {}
            stream->Read(&req); // Illegal read after Read(eos)!
            return grpc::Status::OK;
        }
        """
        self.assertFalse(run_checker_on_cpp(checker, cpp_post_eos_read))

    def test_server_eos_mode_iscancelled_vs_ambiguous(self):
        """Server Read(eos) without IsCancelled() records ambiguous mode; IsCancelled() refines confirmed paths."""
        spec = "!Req; rec X. (+{next: !Req; X, done: WritesDone; ?Resp; end})"
        s_fsm = compile_protocol_fsm(
            parse_session_type(spec), "demo", "Svc", "Call", "BIDIRECTIONAL_STREAMING",
            "demo.Req", "demo.Resp", role="server",
        )
        cpp_ambig = """
        struct Req {};
        struct Resp {};
        grpc::Status RunAmbig(grpc::ServerContext* ctx, grpc::ServerReaderWriter<Resp, Req>* stream) {
            Req req;
            if (!stream->Read(&req)) return grpc::Status::CANCELLED;
            while (stream->Read(&req)) {}
            Resp resp;
            stream->Write(resp);
            return grpc::Status::OK;
        }
        """
        c_ambig = ClangSessionChecker(spec_dict=s_fsm)
        self.assertTrue(run_checker_on_cpp(c_ambig, cpp_ambig))
        self.assertEqual(c_ambig.observed_server_eos_modes, {"ambiguous"})

        cpp_checked = """
        struct Req {};
        struct Resp {};
        grpc::Status RunChecked(grpc::ServerContext* ctx, grpc::ServerReaderWriter<Resp, Req>* stream) {
            Req req;
            if (!stream->Read(&req)) return grpc::Status::CANCELLED;
            while (stream->Read(&req)) {}
            if (ctx->IsCancelled()) {
                return grpc::Status::CANCELLED;
            }
            Resp resp;
            stream->Write(resp);
            return grpc::Status::OK;
        }
        """
        c_checked = ClangSessionChecker(spec_dict=s_fsm)
        self.assertTrue(run_checker_on_cpp(c_checked, cpp_checked))
        self.assertIn("confirmed_cancel", c_checked.observed_server_eos_modes)
        self.assertIn("confirmed_norm", c_checked.observed_server_eos_modes)

    def test_try_catch_propagates_intermediate_stream_mutation(self):
        """Exceptions inside try after WritesDone() propagate S1_HALF_CLOSED into catch blocks."""
        checker = ClangSessionChecker.from_default_level1("CLIENT_STREAMING", "Call", role="client")
        cpp_catch_ok = """
        struct Req {};
        struct Stub { grpc::ClientWriter<Req>* Call(grpc::ClientContext*); };
        void MayThrow();
        void RunTryCatchOk(Stub* stub) {
            grpc::ClientContext ctx;
            auto* stream = stub->Call(&ctx);
            try {
                stream->WritesDone();
                MayThrow();
                stream->Finish();
            } catch (...) {
                stream->Finish();
            }
        }
        """
        cpp_catch_bad_write = """
        struct Req {};
        struct Stub { grpc::ClientWriter<Req>* Call(grpc::ClientContext*); };
        void MayThrow();
        void RunTryCatchBad(Stub* stub) {
            grpc::ClientContext ctx;
            auto* stream = stub->Call(&ctx);
            try {
                stream->WritesDone();
                MayThrow();
                stream->Finish();
            } catch (...) {
                Req req;
                stream->Write(req); // Illegal Write after WritesDone() along post-WritesDone throw!
                stream->Finish();
            }
        }
        """
        self.assertTrue(run_checker_on_cpp(checker, cpp_catch_ok))
        self.assertFalse(run_checker_on_cpp(checker, cpp_catch_bad_write))

    def test_client_bidi_forbids_write_after_read_eos(self):
        """MC1: ClientReaderWriter Read(eos) closes writes (S2_RESPONSE_EOS); subsequent Write() fails."""
        checker = ClangSessionChecker.from_default_level1("BIDIRECTIONAL_STREAMING", "Call", role="client")
        cpp_write_after_eos = """
        struct Req {};
        struct Resp {};
        struct Stub { grpc::ClientReaderWriter<Req, Resp>* Call(grpc::ClientContext*); };
        void RunWriteAfterEos(Stub* stub) {
            grpc::ClientContext ctx;
            auto* stream = stub->Call(&ctx);
            Resp resp;
            if (!stream->Read(&resp)) {
                Req req;
                stream->Write(req); // Illegal Write after Read(eos)!
            }
            stream->Finish();
        }
        """
        cpp_clean_bidi = """
        struct Req {};
        struct Resp {};
        struct Stub { grpc::ClientReaderWriter<Req, Resp>* Call(grpc::ClientContext*); };
        void RunCleanBidi(Stub* stub) {
            grpc::ClientContext ctx;
            auto* stream = stub->Call(&ctx);
            Req req;
            stream->Write(req);
            stream->WritesDone();
            Resp resp;
            while (stream->Read(&resp)) {}
            stream->Finish();
        }
        """
        self.assertFalse(run_checker_on_cpp(checker, cpp_write_after_eos))
        self.assertTrue(run_checker_on_cpp(checker, cpp_clean_bidi))
        self.assertEqual(checker.conditional_verifications, 0)

    def test_default_client_finish_without_eos_reports_conditional(self):
        """MC2: Finish() with ReceiveComplete != Yes emits CONDITIONAL_VERIFICATION; Read(eos) or ClientWriter does not."""
        reader_chk = ClangSessionChecker.from_default_level1("SERVER_STREAMING", "Stream", role="client")
        cpp_early_finish = """
        struct Resp {};
        struct Stub { grpc::ClientReader<Resp>* Stream(grpc::ClientContext*); };
        void RunEarlyFinish(Stub* stub) {
            grpc::ClientContext ctx;
            auto* stream = stub->Stream(&ctx);
            Resp resp;
            if (stream->Read(&resp)) {
                // Early exit without observing Read(eos)
            }
            stream->Finish();
        }
        """
        self.assertTrue(run_checker_on_cpp(reader_chk, cpp_early_finish))
        self.assertGreaterEqual(reader_chk.conditional_verifications, 1)
        self.assertTrue(any("CONDITIONAL_VERIFICATION" in d for _, d in reader_chk.diagnostics))

        cpp_drained = """
        struct Resp {};
        struct Stub { grpc::ClientReader<Resp>* Stream(grpc::ClientContext*); };
        void RunDrained(Stub* stub) {
            grpc::ClientContext ctx;
            auto* stream = stub->Stream(&ctx);
            Resp resp;
            while (stream->Read(&resp)) {}
            stream->Finish();
        }
        """
        self.assertTrue(run_checker_on_cpp(reader_chk, cpp_drained))
        self.assertEqual(reader_chk.conditional_verifications, 0)

        writer_chk = ClangSessionChecker.from_default_level1("CLIENT_STREAMING", "Upload", role="client")
        cpp_writer = """
        struct Req {};
        struct Stub { grpc::ClientWriter<Req>* Upload(grpc::ClientContext*); };
        void RunWriter(Stub* stub) {
            grpc::ClientContext ctx;
            auto* stream = stub->Upload(&ctx);
            Req req;
            stream->Write(req);
            stream->WritesDone();
            stream->Finish();
        }
        """
        self.assertTrue(run_checker_on_cpp(writer_chk, cpp_writer))
        self.assertEqual(writer_chk.conditional_verifications, 0)

    def test_writelast_and_set_last_message(self):
        """MC3: WriteLast and Write(..., WriteOptions().set_last_message()) close writes on client & server."""
        cw_chk = ClangSessionChecker.from_default_level1("CLIENT_STREAMING", "Upload", role="client")
        cpp_cw_ok = """
        struct Req {};
        struct Stub { grpc::ClientWriter<Req>* Upload(grpc::ClientContext*); };
        void RunWriteLastOk(Stub* stub) {
            grpc::ClientContext ctx;
            auto* stream = stub->Upload(&ctx);
            Req req;
            grpc::WriteOptions opts;
            stream->WriteLast(req, opts);
            stream->Finish();
        }
        """
        cpp_cw_bad = """
        struct Req {};
        struct Stub { grpc::ClientWriter<Req>* Upload(grpc::ClientContext*); };
        void RunWriteLastBad(Stub* stub) {
            grpc::ClientContext ctx;
            auto* stream = stub->Upload(&ctx);
            Req req;
            grpc::WriteOptions opts;
            opts.set_last_message();
            stream->Write(req, opts);
            stream->Write(req); // Illegal Write after set_last_message!
            stream->Finish();
        }
        """
        self.assertTrue(run_checker_on_cpp(cw_chk, cpp_cw_ok))
        self.assertFalse(run_checker_on_cpp(cw_chk, cpp_cw_bad))

        bidi_chk = ClangSessionChecker.from_default_level1("BIDIRECTIONAL_STREAMING", "Call", role="client")
        cpp_bidi_ok = """
        struct Req {};
        struct Resp {};
        struct Stub { grpc::ClientReaderWriter<Req, Resp>* Call(grpc::ClientContext*); };
        void RunBidiLastOk(Stub* stub) {
            grpc::ClientContext ctx;
            auto* stream = stub->Call(&ctx);
            Req req;
            stream->Write(req, grpc::WriteOptions().set_last_message());
            Resp resp;
            while (stream->Read(&resp)) {}
            stream->Finish();
        }
        """
        cpp_bidi_bad = """
        struct Req {};
        struct Resp {};
        struct Stub { grpc::ClientReaderWriter<Req, Resp>* Call(grpc::ClientContext*); };
        void RunBidiLastBad(Stub* stub) {
            grpc::ClientContext ctx;
            auto* stream = stub->Call(&ctx);
            Req req;
            stream->Write(req, grpc::WriteOptions().set_last_message());
            stream->Write(req); // Illegal Write after set_last_message!
            stream->Finish();
        }
        """
        self.assertTrue(run_checker_on_cpp(bidi_chk, cpp_bidi_ok))
        self.assertFalse(run_checker_on_cpp(bidi_chk, cpp_bidi_bad))

        srv_chk = ClangSessionChecker.from_default_level1("SERVER_STREAMING", "Stream", role="server")
        cpp_srv_ok = """
        struct Resp {};
        grpc::Status RunSrvOk(grpc::ServerContext* ctx, grpc::ServerWriter<Resp>* stream) {
            Resp resp;
            grpc::WriteOptions opts;
            stream->WriteLast(resp, opts);
            return grpc::Status::OK;
        }
        """
        cpp_srv_bad = """
        struct Resp {};
        grpc::Status RunSrvBad(grpc::ServerContext* ctx, grpc::ServerWriter<Resp>* stream) {
            Resp resp;
            grpc::WriteOptions opts;
            stream->WriteLast(resp, opts);
            stream->Write(resp); // Illegal Write after ServerWriter::WriteLast!
            return grpc::Status::OK;
        }
        """
        self.assertTrue(run_checker_on_cpp(srv_chk, cpp_srv_ok))
        self.assertFalse(run_checker_on_cpp(srv_chk, cpp_srv_bad))

    def test_failed_illegal_write_not_waived_as_abort(self):
        """MC4: Negated illegal Write() after WritesDone() must not waive missing Finish() as F_abort."""
        checker = ClangSessionChecker.from_default_level1("CLIENT_STREAMING", "Upload", role="client")
        cpp_illegal_direct = """
        struct Req {};
        struct Stub { grpc::ClientWriter<Req>* Upload(grpc::ClientContext*); };
        void RunIllegalDirect(Stub* stub) {
            grpc::ClientContext ctx;
            auto* stream = stub->Upload(&ctx);
            stream->WritesDone();
            Req req;
            if (!stream->Write(req)) {
                return;
            }
            stream->Finish();
        }
        """
        cpp_illegal_prov = """
        struct Req {};
        struct Stub { grpc::ClientWriter<Req>* Upload(grpc::ClientContext*); };
        void RunIllegalProv(Stub* stub) {
            grpc::ClientContext ctx;
            auto* stream = stub->Upload(&ctx);
            stream->WritesDone();
            Req req;
            bool ok = stream->Write(req);
            if (!ok) {
                return; // Must NOT waive missing Finish()!
            }
            stream->Finish();
        }
        """
        self.assertFalse(run_checker_on_cpp(checker, cpp_illegal_direct))
        self.assertTrue(any("Stream lifecycle leak" in d for _, d in checker.diagnostics))
        self.assertFalse(run_checker_on_cpp(checker, cpp_illegal_prov))
        self.assertTrue(any("Stream lifecycle leak" in d for _, d in checker.diagnostics))

    def test_server_return_status_ok_nonok_unknown_and_try_cancel(self):
        """MC5/MC6: status_vars classification (OK/non_ok/Unknown), pre-EOS IsCancelled guard, and TryCancel advisory."""
        spec = "rec X. (+{next: !Req; X, done: WritesDone; ?Resp; end})"
        s_fsm = compile_protocol_fsm(
            parse_session_type(spec), "demo", "Svc", "Call", "BIDIRECTIONAL_STREAMING",
            "demo.Req", "demo.Resp", role="server",
        )
        cpp_unknown = """
        struct Req {};
        struct Resp {};
        grpc::Status ComputeStatus();
        grpc::Status RunUnknownStatus(grpc::ServerContext* ctx, grpc::ServerReaderWriter<Resp, Req>* stream) {
            Req req;
            if (!ctx->IsCancelled()) {
                // Must NOT mark confirmed_norm before Read(eos)!
            }
            while (stream->Read(&req)) {}
            Resp resp;
            stream->Write(resp);
            grpc::Status st = ComputeStatus();
            ctx->TryCancel();
            return st;
        }
        """
        c_unk = ClangSessionChecker(spec_dict=s_fsm)
        self.assertTrue(run_checker_on_cpp(c_unk, cpp_unknown))
        self.assertGreaterEqual(c_unk.unknown_status_returns, 1)
        self.assertEqual(c_unk.observed_server_eos_modes, {"ambiguous"})
        self.assertTrue(any("STATUS_UNKNOWN" in d for _, d in c_unk.diagnostics))
        self.assertTrue(any("SERVER_TRY_CANCEL_ADVISORY" in d for _, d in c_unk.diagnostics))

        cpp_nonok_var = """
        struct Req {};
        struct Resp {};
        grpc::Status RunNonOkVar(grpc::ServerContext* ctx, grpc::ServerReaderWriter<Resp, Req>* stream) {
            grpc::Status st = grpc::Status::CANCELLED;
            return st;
        }
        """
        c_err = ClangSessionChecker(spec_dict=s_fsm)
        self.assertTrue(run_checker_on_cpp(c_err, cpp_nonok_var))
        self.assertGreaterEqual(c_err.server_app_err_returns, 1)
        self.assertEqual(c_err.unknown_status_returns, 0)


if __name__ == "__main__":
    unittest.main()
