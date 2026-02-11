from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, Field


class ChatMessage(BaseModel):
    role: Optional[str] = None
    content: Optional[str] = None
    reasoning_content: Optional[str] = None


class ChatChoice(BaseModel):
    index: Optional[int] = None
    message: Optional[ChatMessage] = None


class ChatCompletionUsage(BaseModel):
    # API may return empty object {}
    raw: dict[str, Any] = Field(default_factory=dict)


class ChatCompletionResponse(BaseModel):
    """
    Supports payload like:
      {
        "id": "...",
        "model": "...",
        "choices": [{"index": 0, "message": {"role": "assistant", "content": "...", "reasoning_content": null}}],
        "usage": {}
      }

    Also supports wrappers like:
      { "data": { ...payload... } }
    """

    id: Optional[str] = None
    model: Optional[str] = None
    choices: list[ChatChoice] = Field(default_factory=list)
    usage: dict[str, Any] = Field(default_factory=dict)

    @staticmethod
    def from_any(payload: Any) -> "ChatCompletionResponse":
        if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
            payload = payload["data"]
        return ChatCompletionResponse.model_validate(payload)

    def first_content(self) -> str:
        if not self.choices:
            return ""
        msg = self.choices[0].message
        if not msg or not msg.content:
            return ""
        return str(msg.content)

    def first_reasoning(self) -> str:
        if not self.choices:
            return ""
        msg = self.choices[0].message
        if not msg or not msg.reasoning_content:
            return ""
        return str(msg.reasoning_content)

