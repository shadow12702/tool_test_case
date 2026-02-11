from __future__ import annotations

import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests
from openpyxl import Workbook

from model.types import PromptItem
from service.chat_client import ChatCallResult, call_chat_api, make_conv_uid
from service.config_loader import ApiConfig
from service.raw_chart_exporter import export_raw_chart_html_for_dashboard


@dataclass(frozen=True)
class ExportJobResult:
    export_dir: Path
    counts: dict[str, int]


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _sanitize_folder_name(name: str) -> str:
    bad = '<>:"/\\|?*'
    out = "".join("_" if c in bad else c for c in name.strip())
    return out or "user"


def run_export_job(
    prompts: list[PromptItem],
    cfg: ApiConfig,
    run_id: str,
    user_id: str,
    user_name: str,
    overrides: dict[str, Any] | None,
    max_workers: int,
    export_root: Path,
) -> ExportJobResult:
    """
    - Multi-thread POST calls in parallel
    - Export output under: export/<user_name>/<model_name>/<chat_mode>/
    - Export CSV grouped by source sheet
    """
    overrides_dict = overrides or {}

    # Determine effective model_name and chat_mode for directory structure
    effective_model_name = str(
        overrides_dict.get("model_name")
        or cfg.default_body.get("model_name")
        or "unknown_model"
    ).strip()
    effective_chat_mode = str(
        overrides_dict.get("chat_mode")
        or cfg.default_body.get("chat_mode")
        or "unknown_mode"
    ).strip()
    effective_app_code = str(
        overrides_dict.get("app_code")
        or cfg.default_body.get("app_code")
        or ""
    ).strip()
    # If app_code is dashboard but chat_mode isn't, reflect that
    if effective_app_code == "chat_dashboard" and effective_chat_mode != "chat_dashboard":
        effective_chat_mode = "chat_dashboard"

    user_dir = export_root / _sanitize_folder_name(user_name)
    export_dir = (
        user_dir
        / _sanitize_folder_name(effective_model_name)
        / _sanitize_folder_name(effective_chat_mode)
    )
    # Clean previous results before writing new ones
    if export_dir.exists():
        shutil.rmtree(export_dir)
    _ensure_dir(export_dir)

    # Thread-local-ish session per worker (simple pool of sessions)
    sessions = [requests.Session() for _ in range(max_workers)]

    def _task(i: int, item: PromptItem) -> tuple[PromptItem, ChatCallResult]:
        sess = sessions[i % max_workers]
        # Each prompt gets its own conv_uid so they can run in parallel
        # without conversation-context conflicts on the server side.
        res = call_chat_api(
            cfg=cfg,
            user_input=item.user_input,
            user_id=user_id,
            user_name=user_name,
            conv_uid=make_conv_uid(),
            overrides=overrides,
            session=sess,
        )
        return item, res

    results: list[tuple[PromptItem, ChatCallResult]] = []
    ok_count = 0
    err_count = 0

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = [ex.submit(_task, idx, item) for idx, item in enumerate(prompts)]
        for fut in as_completed(futs):
            item, res = fut.result()
            results.append((item, res))
            if res.ok:
                ok_count += 1
            else:
                err_count += 1

    # Group results by sheet
    by_sheet: dict[str, list[tuple[PromptItem, ChatCallResult]]] = {}
    for item, res in results:
        by_sheet.setdefault(item.sheet, []).append((item, res))

    # For non-dashboard modes, include prompt_id and user_input columns
    is_dashboard = (
        effective_chat_mode == "chat_dashboard"
        or effective_app_code == "chat_dashboard"
    )

    written_files: list[str] = []
    for sheet, items in sorted(by_sheet.items(), key=lambda kv: kv[0]):
        safe_sheet = _sanitize_folder_name(sheet.replace(":", " - "))
        out_xlsx = export_dir / f"{safe_sheet}.xlsx"

        wb = Workbook()
        ws = wb.active
        ws.title = "Result"

        if is_dashboard:
            ws.append(["content"])
        else:
            ws.append(["prompt_id", "prompt", "content"])

        for item, res in sorted(items, key=lambda t: (t[0].sheet, t[0].row_index)):
            if is_dashboard:
                ws.append([res.assistant_content or ""])
            else:
                ws.append([
                    item.prompt_id,
                    item.user_input,
                    res.assistant_content or "",
                ])

        wb.save(str(out_xlsx))
        written_files.append(str(out_xlsx))

    # Special flow: chat_dashboard -> export raw chart HTML from the *last usable* response
    raw_chart_html: str | None = None
    raw_chart_count: int | None = None
    raw_chart_error: str | None = None
    if is_dashboard:
        # Keep prompt order stable for "last response" logic
        ordered = sorted(results, key=lambda t: (t[0].sheet, t[0].row_index))
        responses_in_order = [r for _, r in ordered]
        try:
            raw_res = export_raw_chart_html_for_dashboard(
                responses_in_prompt_order=responses_in_order,
                export_dir=export_dir,
                user_name=user_name,
            )
            if raw_res:
                raw_chart_html = str(raw_res.html_path)
                raw_chart_count = raw_res.chart_count
        except Exception as e:
            raw_chart_error = str(e)

    return ExportJobResult(
        export_dir=export_dir,
        counts={"total": len(prompts), "ok": ok_count, "error": err_count},
    )
