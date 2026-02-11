from __future__ import annotations

import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from model.types import PromptItem, UserItem
from service.config_loader import ApiConfig
from service.export_runner import ExportJobResult, run_export_job
from service.prompt_reader import read_prompts_from_file
from service.user_generator import generate_users


@dataclass(frozen=True)
class BatchRunResult:
    run_id: str
    export_root: Path
    users_total: int
    models_total: int
    chat_modes_total: int
    jobs_total: int
    jobs_ok: int
    jobs_error: int


def run_for_all_users(
    cfg: ApiConfig,
    project_root: Path,
    export_root: Path,
    users: list[UserItem] | None = None,
) -> BatchRunResult:
    """
    - Accept a user list OR generate from cfg.user_count
    - Read model_names, chat_modes (with prompt files) from config
    - Each chat_mode maps to its own prompt file
    - Run all (user x model_name x chat_mode) combinations in parallel
    - Export output under export/<user_name>/<model_name>/<chat_mode>/...
    """
    if users is None:
        users = generate_users(cfg.user_count)
    if not users:
        raise ValueError(f"user list is empty – nothing to do")

    # model_names from config; fallback to default_body
    model_names = cfg.model_names
    if not model_names:
        fallback = str(
            cfg.default_body.get("model_name") or "unknown_model"
        ).strip()
        model_names = [fallback]

    # chat_modes from config (dict: chat_mode -> {prompt_file, sheet_name?})
    chat_modes = cfg.chat_modes
    if not chat_modes:
        fallback_mode = str(
            cfg.default_body.get("chat_mode") or "unknown_mode"
        ).strip()
        chat_modes = {fallback_mode: {"prompt_file": "csv/prompt_txt_to_sql.xlsx"}}

    # Pre-load prompts for each chat_mode
    prompts_by_mode: dict[str, list[PromptItem]] = {}
    for mode, mode_cfg in chat_modes.items():
        prompt_file = str(mode_cfg.get("prompt_file", ""))
        sheet_name = mode_cfg.get("sheet_name") or None
        prompt_path = (project_root / prompt_file).resolve()
        if not prompt_path.exists():
            raise ValueError(
                f"Prompt file not found for chat_mode '{mode}': {prompt_path}"
            )
        prompts = read_prompts_from_file(prompt_path, sheet_name=sheet_name)
        if not prompts:
            raise ValueError(
                f"No prompts found in file for chat_mode '{mode}': {prompt_path}"
                + (f" (sheet: {sheet_name})" if sheet_name else "")
            )
        prompts_by_mode[mode] = prompts

    run_id = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

    # Build all (user x model x chat_mode) combinations
    jobs: list[tuple[UserItem, str, str]] = [
        (u, m, c) for u in users for m in model_names for c in chat_modes
    ]

    max_threads = min(cfg.max_user_threads, 8, len(jobs))
    prompt_workers = max(1, int(cfg.max_prompt_threads_per_user))

    jobs_ok = 0
    jobs_error = 0

    def _run_one(
        u: UserItem, model_name: str, chat_mode: str,
    ) -> ExportJobResult:
        job_overrides: dict[str, Any] = {
            "model_name": model_name,
            "chat_mode": chat_mode,
            "app_code": chat_mode,
        }
        return run_export_job(
            prompts=prompts_by_mode[chat_mode],
            cfg=cfg,
            run_id=run_id,
            user_id=u.user_id,
            user_name=u.user_name,
            overrides=job_overrides,
            max_workers=prompt_workers,
            export_root=export_root,
        )

    with ThreadPoolExecutor(max_workers=max_threads) as ex:
        futs = {ex.submit(_run_one, u, m, c): (u, m, c) for u, m, c in jobs}
        for fut in as_completed(futs):
            try:
                fut.result()
                jobs_ok += 1
            except Exception:
                jobs_error += 1

    export_root.mkdir(parents=True, exist_ok=True)

    return BatchRunResult(
        run_id=run_id,
        export_root=export_root,
        users_total=len(users),
        models_total=len(model_names),
        chat_modes_total=len(chat_modes),
        jobs_total=len(jobs),
        jobs_ok=jobs_ok,
        jobs_error=jobs_error,
    )

