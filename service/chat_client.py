from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass
from typing import Any

import requests

from model.response.chat_completion_response import ChatCompletionResponse
from service.config_loader import ApiConfig


@dataclass(frozen=True)
class ChatCallResult:
    ok: bool
    status_code: int | None
    elapsed_ms: int
    response_json: dict[str, Any] | None
    response_id: str | None
    response_model: str | None
    assistant_content: str | None
    assistant_reasoning: str | None
    error: str | None


def _extract_final_content_from_response_text(raw_text: str) -> tuple[str, dict[str, Any] | None]:
    """
    Implements user's logic:
    - Response may contain multiple lines like: "data: {...json...}"
    - We take the *last* chunk's `choices[0].message.content` as final_content.

    Returns: (final_content, last_chunk_dict)
    """
    if not raw_text:
        return "", None

    final_content = ""
    last_chunk: dict[str, Any] | None = None
    for ln in raw_text.splitlines():
        ln = ln.strip()
        if not ln.startswith("data: "):
            continue
        json_str = ln[6:]  # bỏ prefix "data: "
        if not json_str or json_str.strip() == "[DONE]":
            continue
        try:
            chunk = json.loads(json_str)
            last_chunk = chunk if isinstance(chunk, dict) else last_chunk
            final_content = chunk["choices"][0]["message"]["content"]
        except json.JSONDecodeError:
            pass
        except Exception:
            # keep scanning; we only care final content from valid chunks
            pass

    return final_content, last_chunk


def _normalize_response(data: Any) -> dict[str, Any] | None:
    if data is None:
        return None
    if isinstance(data, dict):
        # if we captured raw text because JSON parse failed earlier
        if "raw_text" in data and isinstance(data.get("raw_text"), str):
            _, last_chunk = _extract_final_content_from_response_text(data["raw_text"])
            return last_chunk or data
        return data
    if isinstance(data, str):
        _, last_chunk = _extract_final_content_from_response_text(data)
        return last_chunk or {"raw_text": data}
    return {"raw": data}


def build_request_payload(
    cfg: ApiConfig,
    user_input: str,
    user_id: str,
    user_name: str,
    conv_uid: str,
    overrides: dict[str, Any] | None,
) -> tuple[dict[str, str], dict[str, Any]]:
    headers = dict(cfg.default_headers)
    headers["user-id"] = user_id

    body = json.loads(json.dumps(cfg.default_body))  # deep copy
    body["user_input"] = user_input
    body["user_name"] = user_name
    body["conv_uid"] = conv_uid

    if overrides:
        # shallow merge for top-level keys
        for k, v in overrides.items():
            body[k] = v

    # Special flow: when dashboard mode is requested, keep chat_mode/app_code consistent.
    # Users often set only one field in overrides; the API expects both to match.
    chat_mode = str(body.get("chat_mode") or "").strip()
    app_code = str(body.get("app_code") or "").strip()
    if chat_mode == "chat_dashboard" or app_code == "chat_dashboard":
        body["chat_mode"] = "chat_dashboard"
        body["app_code"] = "chat_dashboard"

    return headers, body


def call_chat_api(
    cfg: ApiConfig,
    user_input: str,
    user_id: str,
    user_name: str,
    conv_uid: str,
    overrides: dict[str, Any] | None = None,
    session: requests.Session | None = None,
) -> ChatCallResult:
    url = f"{cfg.base_url}{cfg.endpoint}"
    headers, body = build_request_payload(
        cfg=cfg,
        user_input=user_input,
        user_id=user_id,
        user_name=user_name,
        conv_uid=conv_uid,
        overrides=overrides,
    )

    s = session or requests.Session()
    start = time.perf_counter()
    try:
        resp = s.post(url, headers=headers, json=body, timeout=cfg.timeout_seconds)
        elapsed_ms = int((time.perf_counter() - start) * 1000)

        # Always parse SSE from UTF-8 bytes to avoid mojibake like: "DÆ°á»i ÄÃ¢y..."
        raw_bytes = resp.content or b""
        raw_text = raw_bytes.decode("utf-8", errors="replace")
        final_content, last_chunk = _extract_final_content_from_response_text(raw_text)

        try:
            data: Any = resp.json()
        except Exception:
            data = {"raw_text": raw_text}
        normalized = _normalize_response(data)

        parsed: ChatCompletionResponse | None = None
        try:
            if isinstance(normalized, dict) and "raw_text" not in normalized:
                parsed = ChatCompletionResponse.from_any(normalized)
        except Exception:
            parsed = None

        # Prefer user's final content; fallback to parsed model
        assistant_content = final_content or (parsed.first_content() if parsed else None) or None

        # Try to get id/model from the last SSE chunk if present
        response_id = None
        response_model = None
        if isinstance(last_chunk, dict):
            response_id = last_chunk.get("id") if isinstance(last_chunk.get("id"), str) else None
            response_model = last_chunk.get("model") if isinstance(last_chunk.get("model"), str) else None

        ok = 200 <= resp.status_code < 300
        return ChatCallResult(
            ok=ok,
            status_code=resp.status_code,
            elapsed_ms=elapsed_ms,
            response_json=normalized if isinstance(normalized, dict) else None,
            response_id=response_id or (parsed.id if parsed else None),
            response_model=response_model or (parsed.model if parsed else None),
            assistant_content=assistant_content,
            assistant_reasoning=parsed.first_reasoning() if parsed else None,
            error=(
                None
                if ok
                else (normalized.get("error") if isinstance(normalized, dict) else None)
                or "HTTP error"
            ),
        )
    except Exception as e:
        elapsed_ms = int((time.perf_counter() - start) * 1000)
        return ChatCallResult(
            ok=False,
            status_code=None,
            elapsed_ms=elapsed_ms,
            response_json=None,
            response_id=None,
            response_model=None,
            assistant_content=None,
            assistant_reasoning=None,
            error=str(e),
        )


def make_conv_uid() -> str:
    # matches typical uuid string format expected by APIs
    return str(uuid.uuid4())
