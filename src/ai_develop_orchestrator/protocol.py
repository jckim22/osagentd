from __future__ import annotations

import json
from typing import Any, Dict


PROTOCOL_VERSION = 1


def ok(**payload: Any) -> Dict[str, Any]:
    body = {"ok": True, "protocol_version": PROTOCOL_VERSION}
    body.update(payload)
    return body


def error(message: str, *, code: str = "error", **payload: Any) -> Dict[str, Any]:
    body = {
        "ok": False,
        "protocol_version": PROTOCOL_VERSION,
        "error": {"code": code, "message": message},
    }
    body.update(payload)
    return body


def encode_message(message: Dict[str, Any]) -> bytes:
    return (json.dumps(message, ensure_ascii=True) + "\n").encode("utf-8")


def decode_message(raw: bytes) -> Dict[str, Any]:
    return json.loads(raw.decode("utf-8"))

