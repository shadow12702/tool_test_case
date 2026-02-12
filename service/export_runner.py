from __future__ import annotations

import logging
import shutil
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests
from openpyxl import Workbook

from model.types import PromptItem
from service.chat_client import ChatCallResult, call_chat_api, make_conv_uid
from service.config_loader import ApiConfig
from service.raw_chart_exporter import render_single_dashboard_html

log = logging.getLogger("batch_runner")


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


def _extract_prompt_id_prefix(prompt_id: str) -> str:
    """Extract grouping prefix from Prompt_ID.

    Examples:
        "P01-1-03" -> "P01-1"
        "P09-3-20" -> "P09-3"
        "unknown"  -> "unknown"
    """
    parts = prompt_id.strip().split("-")
    if len(parts) >= 2:
        return f"{parts[0]}-{parts[1]}"
    return prompt_id.strip() or "unknown"


def _write_prefix_xlsx(
    export_dir: Path,
    prefix: str,
    items: list[tuple[PromptItem, ChatCallResult]],
) -> str:
    """Write (or overwrite) the prefix xlsx with all results collected so far.

    Called every time a new prompt result arrives for this prefix, so the
    file on disk always reflects the latest state.  If the process crashes,
    all previously written rows are preserved.
    """
    safe_name = _sanitize_folder_name(prefix)
    out_xlsx = export_dir / f"{safe_name}.xlsx"

    wb = Workbook()
    ws = wb.active
    ws.title = "Result"
    ws.append(["prompt_id", "prompt", "content"])

    for item, res in sorted(items, key=lambda t: (t[0].sheet, t[0].row_index)):
        ws.append([
            item.prompt_id,
            item.user_input,
            res.assistant_content or "",
        ])

    wb.save(str(out_xlsx))
    return str(out_xlsx)


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
    - Non-dashboard: export each Prompt_ID prefix group IMMEDIATELY when complete
    - Dashboard: export after all prompts finish (group by sheet)
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
    if effective_app_code == "chat_dashboard" and effective_chat_mode != "chat_dashboard":
        effective_chat_mode = "chat_dashboard"

    is_dashboard = (
        effective_chat_mode == "chat_dashboard"
        or effective_app_code == "chat_dashboard"
    )

    user_dir = export_root / _sanitize_folder_name(user_name)
    export_dir = (
        user_dir
        / _sanitize_folder_name(effective_model_name)
        / _sanitize_folder_name(effective_chat_mode)
    )
    if export_dir.exists():
        shutil.rmtree(export_dir)
    _ensure_dir(export_dir)

    job_label = f"{user_name}/{effective_model_name}/{effective_chat_mode}"

    # --- Pre-compute expected count per prefix (for progress tracking) ---
    prefix_expected: dict[str, int] = {}
    if not is_dashboard:
        for item in prompts:
            prefix = _extract_prompt_id_prefix(item.prompt_id)
            prefix_expected[prefix] = prefix_expected.get(prefix, 0) + 1
    total_prefixes = len(prefix_expected)

    # Thread-local-ish session per worker
    sessions = [requests.Session() for _ in range(max_workers)]

    max_retries = cfg.max_retries
    # Backoff delays: 5s after 1st fail, 15s after 2nd, 30s after 3rd, ...
    _RETRY_DELAYS = [5, 15, 30, 60]

    def _task(i: int, item: PromptItem) -> tuple[PromptItem, ChatCallResult]:
        sess = sessions[i % max_workers]
        prompt_label = item.prompt_id or f"row_{item.row_index}"
        last_res: ChatCallResult | None = None

        for attempt in range(1, max_retries + 2):  # attempt 1 = first try, +retries
            res = call_chat_api(
                cfg=cfg,
                user_input=item.user_input,
                user_id=user_id,
                user_name=user_name,
                conv_uid=make_conv_uid(),
                overrides=overrides,
                session=sess,
            )
            last_res = res

            if res.ok and res.assistant_content:
                # Success with content - no need to retry
                return item, res

            if attempt <= max_retries:
                is_timeout = res.status_code is None  # timeout/connection error
                reason = "no content" if res.ok else res.error or "ERROR"
                delay = _RETRY_DELAYS[min(attempt - 1, len(_RETRY_DELAYS) - 1)]

                log.info(
                    f"    [RETRY {attempt}/{max_retries}] {job_label} | "
                    f"{prompt_label} - {reason} ({res.elapsed_ms}ms) "
                    f"-> waiting {delay}s before retry"
                )
                time.sleep(delay)

        # All attempts exhausted, return last result
        return item, last_res  # type: ignore[return-value]

    results: list[tuple[PromptItem, ChatCallResult]] = []
    ok_count = 0
    err_count = 0
    total = len(prompts)

    # Incremental export state
    by_prefix: dict[str, list[tuple[PromptItem, ChatCallResult]]] = {}
    completed_prefixes: set[str] = set()
    written_files: list[str] = []
    dashboard_html_count = 0

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = [ex.submit(_task, idx, item) for idx, item in enumerate(prompts)]
        for fut in as_completed(futs):
            item, res = fut.result()
            results.append((item, res))

            if res.ok:
                ok_count += 1
            else:
                err_count += 1

            completed = ok_count + err_count

            # --- Progress logging ---
            status = "OK" if res.ok else "ERROR"
            log.info(
                f"    [{completed}/{total}] {job_label} | "
                f"{item.prompt_id} - {status} ({res.elapsed_ms}ms)"
            )

            if is_dashboard:
                # --- Dashboard: export each prompt as individual HTML immediately ---
                try:
                    html_res = render_single_dashboard_html(
                        res=res,
                        prompt_id=item.prompt_id,
                        export_dir=export_dir,
                        user_name=user_name,
                    )
                    if html_res:
                        dashboard_html_count += 1
                        written_files.append(str(html_res.html_path))
                        log.info(
                            f"    >> Exported {item.prompt_id}.html "
                            f"({html_res.chart_count} charts) "
                            f"[{dashboard_html_count}/{total}]"
                        )
                    else:
                        log.info(
                            f"    >> {item.prompt_id}: no chart payload, skipped HTML"
                        )
                except Exception as e:
                    log.warning(
                        f"    >> {item.prompt_id}: HTML export failed: {e}"
                    )
            else:
                # --- Non-dashboard: write prefix xlsx EVERY time a prompt completes ---
                # File is overwritten with all rows collected so far, ensuring
                # crash-safe progress (no data lost if process dies mid-run).
                prefix = _extract_prompt_id_prefix(item.prompt_id)
                by_prefix.setdefault(prefix, []).append((item, res))
                got = len(by_prefix[prefix])
                expected = prefix_expected[prefix]

                _write_prefix_xlsx(export_dir, prefix, by_prefix[prefix])

                if got == expected and prefix not in completed_prefixes:
                    completed_prefixes.add(prefix)
                    written_files.append(
                        str(export_dir / f"{_sanitize_folder_name(prefix)}.xlsx")
                    )
                    log.info(
                        f"    >> Completed {prefix}.xlsx "
                        f"({got}/{expected} rows) "
                        f"[{len(completed_prefixes)}/{total_prefixes} groups done]"
                    )
                else:
                    log.info(
                        f"    >> Updated {prefix}.xlsx "
                        f"({got}/{expected} rows)"
                    )

    return ExportJobResult(
        export_dir=export_dir,
        counts={"total": len(prompts), "ok": ok_count, "error": err_count},
    )
