from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ApiConfig:
    base_url: str
    endpoint: str
    timeout_seconds: int
    max_retries: int
    max_user_threads: int
    max_prompt_threads_per_user: int
    max_workers: int
    user_count: int
    model_names: list[str]
    chat_modes: dict[str, dict[str, Any]]
    default_user_id: str
    default_user_name: str
    default_headers: dict[str, str]
    default_body: dict[str, Any]

    def to_public_dict(self) -> dict[str, Any]:
        return {
            "base_url": self.base_url,
            "endpoint": self.endpoint,
            "timeout_seconds": self.timeout_seconds,
            "max_retries": self.max_retries,
            "max_user_threads": self.max_user_threads,
            "max_prompt_threads_per_user": self.max_prompt_threads_per_user,
            "max_workers": self.max_workers,
            "user_count": self.user_count,
            "model_names": self.model_names,
            "chat_modes": self.chat_modes,
            "default_user_id": self.default_user_id,
            "default_user_name": self.default_user_name,
            "default_headers": self.default_headers,
            "default_body": self.default_body,
        }


def load_api_config(path: Path) -> ApiConfig:
    raw = json.loads(path.read_text(encoding="utf-8"))
    return ApiConfig(
        base_url=str(raw["base_url"]).rstrip("/"),
        endpoint=str(raw["endpoint"]),
        timeout_seconds=int(raw.get("timeout_seconds", 120)),
        max_retries=max(0, int(raw.get("max_retries", 2))),
        max_user_threads=min(8, max(1, int(raw.get("max_user_threads", 8)))),
        max_prompt_threads_per_user=max(1, int(raw.get("max_prompt_threads_per_user", 1))),
        max_workers=int(raw.get("max_workers", 10)),
        user_count=max(1, int(raw.get("user_count", 3))),
        model_names=list(raw.get("model_names", [])),
        chat_modes=dict(raw.get("chat_modes", {})),
        default_user_id=str(raw.get("default_user_id", "")).strip(),
        default_user_name=str(raw.get("default_user_name", "")).strip(),
        default_headers=dict(raw.get("default_headers", {})),
        default_body=dict(raw.get("default_body", {})),
    )

