from __future__ import annotations

from model.types import UserItem


def generate_users(count: int, start: int = 1) -> list[UserItem]:
    """
    Generate a list of users starting from a given offset.

    Examples:
        generate_users(3)           -> user-001, user-002, user-003
        generate_users(3, start=4)  -> user-004, user-005, user-006
    """
    count = max(0, count)
    return [
        UserItem(
            user_id=f"userid-{i:03d}",
            user_name=f"user-{i:03d}",
        )
        for i in range(start, start + count)
    ]
