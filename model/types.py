from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PromptItem:
    sheet: str
    row_index: int
    user_input: str
    prompt_id: str = ""


@dataclass(frozen=True)
class UserItem:
    user_id: str
    user_name: str

    def to_dict(self) -> dict:
        return {"user_id": self.user_id, "user_name": self.user_name}

