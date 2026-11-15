"""
proto_extractor.py — Extracts Session Type specifications and RPC definitions from .proto files.

Scans Protobuf IDL files for service definitions with custom method options:
  option (<ext_name>.session_type) = "<session_type_string>";
  option (<ext_name>.method_aliases) = "AliasA, AliasB";   // optional
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
    re.DOTALL,
)
ALIASES_REGEX = re.compile(
    r"option\s*\([^)]*method_aliases\)\s*=\s*\"(?P<aliases>[^\"]+)\"\s*;",
    re.DOTALL,
)
GUARD_TAG_REGEX = re.compile(r"(?:!|\?)[a-zA-Z_][a-zA-Z0-9_]*\[([^\]]+)\]")
MSG_START_REGEX = re.compile(r"message\s+(?P<msg>[a-zA-Z0-9_]+)\s*\{")
ONEOF_START_REGEX = re.compile(r"oneof\s+(?P<oneof_name>[a-zA-Z0-9_]+)\s*\{")
FIELD_REGEX = re.compile(
    r"(?:optional\s+|repeated\s+)?[a-zA-Z0-9_.]+\s+(?P<field>[a-zA-Z0-9_]+)\s*=\s*(?P<num>\d+)\s*(?:\[[^\]]*\])?\s*;"
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
        if ch == "\\":
            escape = True
            continue
        if ch == '"':
            in_string = not in_string
            continue
        if not in_string:
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    return i
    return -1


def _extract_message_schemas(content: str) -> Dict[str, Dict[str, Any]]:
    """Extracts oneof field-number mappings and top-level field names per Protobuf message."""
    stripped = re.sub(r"//.*", "", content)
    schemas: Dict[str, Dict[str, Any]] = {}
    idx = 0
    while True:
        m = MSG_START_REGEX.search(stripped, idx)
        if not m:
            break
        msg_name = m.group("msg")
        m_start = m.end() - 1
        m_end = _find_matching_brace(stripped, m_start)
        if m_end == -1:
            break
        body = stripped[m_start + 1:m_end]

        oneof_field_name = None
        oneof_mappings: Dict[str, int] = {}
        o_match = ONEOF_START_REGEX.search(body)
        if o_match:
            o_start = o_match.end() - 1
            o_end = _find_matching_brace(body, o_start)
            if o_end != -1:
                o_body = body[o_start + 1:o_end]
                oneof_mappings = {
                    fm.group("field"): int(fm.group("num"))
                    for fm in FIELD_REGEX.finditer(o_body)
                }
                if oneof_mappings:
                    oneof_field_name = o_match.group("oneof_name")

        all_fields = {fm.group("field"): int(fm.group("num")) for fm in FIELD_REGEX.finditer(body)}
        schemas[msg_name] = {
            "field_name": oneof_field_name,
            "oneof_mappings": oneof_mappings,
            "all_fields": all_fields,
        }
        idx = m_end + 1
    return schemas


def _extract_payload_spec(
    session_spec: str,
    client_schema: Dict[str, Any],
    server_schema: Dict[str, Any],
) -> tuple[Optional[Dict[str, int]], str, Optional[Dict[str, Dict[str, Any]]]]:
    """
    Synthesizes Level-2 payload setter mappings and orthogonal property groups.
    - oneof variants share their oneof group key (mutually exclusive).
    - non-oneof explicit/submessage fields get independent `field:<name>` groups
      so multiple fields on the same message can be tracked orthogonally.
    """
    oneof_mappings = dict(client_schema.get("oneof_mappings") or server_schema.get("oneof_mappings") or {})
    field_name = client_schema.get("field_name") or server_schema.get("field_name") or "payload"

    all_fields: Dict[str, int] = {}
    all_fields.update(server_schema.get("all_fields") or {})
    all_fields.update(client_schema.get("all_fields") or {})

    tags: List[str] = []
    for raw_grp in GUARD_TAG_REGEX.findall(session_spec):
        for tok in re.split(r"[&,]", raw_grp):
            t = tok.strip()
            if t:
                tags.append(t)

    presence_mappings: Dict[str, int] = {}
    groups: Dict[str, Dict[str, Any]] = {}
    for k, v in oneof_mappings.items():
        groups[k] = {"group": field_name, "case": v}

    presence_field_name: Optional[str] = None
    next_case = max(oneof_mappings.values(), default=0) + 1

    for tag in tags:
        if tag in oneof_mappings:
            continue
        if tag.startswith("clear_") and tag[len("clear_"):] in all_fields:
            base = tag[len("clear_"):]
            if tag not in presence_mappings:
                presence_mappings[tag] = next_case
                next_case += 1
            groups[tag] = {"group": f"field:{base}", "case": presence_mappings[tag]}
            if presence_field_name is None:
                presence_field_name = base
        elif tag in all_fields:
            if f"mutable_{tag}" not in presence_mappings:
                case_id = next_case
                next_case += 1
                presence_mappings[tag] = case_id
                presence_mappings[f"mutable_{tag}"] = case_id
                presence_mappings[f"set_{tag}"] = case_id
            else:
                case_id = presence_mappings[f"mutable_{tag}"]
            groups[f"mutable_{tag}"] = {"group": f"field:{tag}", "case": case_id}
            groups[f"set_{tag}"] = {"group": f"field:{tag}", "case": case_id}
            if presence_field_name is None:
                presence_field_name = tag

    if oneof_mappings:
        combined = dict(oneof_mappings)
        for k, v in presence_mappings.items():
            if f"clear_{k}" not in presence_mappings:
                combined[k] = v
                if k not in groups:
                    base = k[len("mutable_"):] if k.startswith("mutable_") else (k[len("set_"):] if k.startswith("set_") else k)
                    groups[k] = {"group": f"field:{base}", "case": v}
        return (combined or None), field_name, (groups or None)
    if presence_mappings:
        filtered = {
            k: v for k, v in presence_mappings.items()
            if f"clear_{k}" not in presence_mappings
        }
        return (filtered or None), (presence_field_name or "payload"), (groups or None)
    return None, field_name, (groups or None)


def _build_payload_mappings(
    session_spec: str,
    client_schema: Dict[str, Any],
    server_schema: Dict[str, Any],
) -> tuple[Optional[Dict[str, int]], str]:
    mappings, fname, _ = _extract_payload_spec(session_spec, client_schema, server_schema)
    return mappings, fname


def extract_session_specs_from_proto(proto_path: Path) -> List[Dict[str, Any]]:
    """
    Parses a .proto file and returns a list of streaming RPC specifications
    that declare a custom (session_type) option.
    """
    content = proto_path.read_text(encoding="utf-8")
    msg_schemas = _extract_message_schemas(content)

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
                alias_match = ALIASES_REGEX.search(rpc_body)
                method_aliases = (
                    [a.strip() for a in alias_match.group("aliases").split(",") if a.strip()]
                    if alias_match
                    else None
                )

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
                short_client = client_type.split(".")[-1]
                short_server = server_type.split(".")[-1]

                payload_mappings, payload_field_name, payload_groups = _extract_payload_spec(
                    session_spec,
                    msg_schemas.get(short_client, {}),
                    msg_schemas.get(short_server, {}),
                )

                specs.append({
                    "package": package,
                    "service": service_name,
                    "method": method_name,
                    "method_aliases": method_aliases,
                    "rpc_type": rpc_type,
                    "client_stream_type": full_client_type,
                    "server_stream_type": full_server_type,
                    "session_spec": session_spec,
                    "payload_mappings": payload_mappings,
                    "payload_field_name": payload_field_name,
                    "payload_groups": payload_groups,
                    "proto_file": str(proto_path),
                })

            rpc_idx = rpc_end + 1

        idx = svc_end + 1

    return specs
