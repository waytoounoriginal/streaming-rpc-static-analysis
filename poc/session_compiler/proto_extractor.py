"""
proto_extractor.py — Extracts Session Type specifications and RPC definitions from .proto files.

Scans Protobuf IDL files for service definitions with custom method options:
  option (<ext_name>.session_type) = "<session_type_string>";
"""

import re
from pathlib import Path
from typing import List, Dict, Any, Optional


PACKAGE_REGEX = re.compile(r"package\s+(?P<package>[a-zA-Z0-9_.]+)\s*;")
SERVICE_START_REGEX = re.compile(r"service\s+(?P<service>[a-zA-Z0-9_]+)\s*\{")
RPC_START_REGEX = re.compile(
    r"rpc\s+(?P<method>[a-zA-Z0-9_]+)\s*\(\s*(?P<client_stream>stream\s+)?(?P<client_type>[a-zA-Z0-9_.]+)\s*\)\s*"
    r"returns\s*\(\s*(?P<server_stream>stream\s+)?(?P<server_type>[a-zA-Z0-9_.]+)\s*\)\s*\{"
)
OPTION_REGEX = re.compile(
    r"option\s*\([^)]*session_type\)\s*=\s*\"(?P<session_spec>[^\"]+)\"\s*;",
    re.DOTALL
)


def _find_matching_brace(text: str, start_brace_pos: int) -> int:
    """Finds the index of the matching closing brace, respecting nesting."""
    depth = 0
    in_string = False
    escape = False

    for i in range(start_brace_pos, len(text)):
        ch = text[i]
        if escape:
            escape = False
            continue
        if ch == '\\':
            escape = True
            continue
        if ch == '"':
            in_string = not in_string
            continue
        if not in_string:
            if ch == '{':
                depth += 1
            elif ch == '}':
                depth -= 1
                if depth == 0:
                    return i
    return -1


def extract_session_specs_from_proto(proto_path: Path) -> List[Dict[str, Any]]:
    """
    Parses a .proto file and returns a list of streaming RPC specifications
    that declare a custom (session_type) option.
    """
    content = proto_path.read_text(encoding="utf-8")

    pkg_match = PACKAGE_REGEX.search(content)
    package = pkg_match.group("package") if pkg_match else "default"

    specs = []
    idx = 0

    while True:
        svc_match = SERVICE_START_REGEX.search(content, idx)
        if not svc_match:
            break

        service_name = svc_match.group("service")
        svc_start = svc_match.end() - 1
        svc_end = _find_matching_brace(content, svc_start)
        if svc_end == -1:
            break

        svc_body = content[svc_start + 1:svc_end]
        rpc_idx = 0

        while True:
            rpc_match = RPC_START_REGEX.search(svc_body, rpc_idx)
            if not rpc_match:
                break

            method_name = rpc_match.group("method")
            is_client_stream = bool(rpc_match.group("client_stream"))
            client_type = rpc_match.group("client_type").strip()
            is_server_stream = bool(rpc_match.group("server_stream"))
            server_type = rpc_match.group("server_type").strip()

            rpc_start = rpc_match.end() - 1
            rpc_end = _find_matching_brace(svc_body, rpc_start)
            if rpc_end == -1:
                break

            rpc_body = svc_body[rpc_start + 1:rpc_end]
            opt_match = OPTION_REGEX.search(rpc_body)

            if opt_match:
                session_spec = opt_match.group("session_spec").strip()

                if is_client_stream and is_server_stream:
                    rpc_type = "BIDIRECTIONAL_STREAMING"
                elif is_server_stream:
                    rpc_type = "SERVER_STREAMING"
                elif is_client_stream:
                    rpc_type = "CLIENT_STREAMING"
                else:
                    rpc_type = "UNARY"

                full_client_type = client_type if "." in client_type else f"{package}.{client_type}"
                full_server_type = server_type if "." in server_type else f"{package}.{server_type}"

                specs.append({
                    "package": package,
                    "service": service_name,
                    "method": method_name,
                    "rpc_type": rpc_type,
                    "client_stream_type": full_client_type,
                    "server_stream_type": full_server_type,
                    "session_spec": session_spec,
                    "proto_file": str(proto_path)
                })

            rpc_idx = rpc_end + 1

        idx = svc_end + 1

    return specs
