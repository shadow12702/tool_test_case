from __future__ import annotations

import logging
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
    """Write a single prefix group to an xlsx file. Returns the file path."""
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

    # --- Pre-compute expected count per prefix (for incremental export) ---
    prefix_expected: dict[str, int] = {}
    if not is_dashboard:
        for item in prompts:
            prefix = _extract_prompt_id_prefix(item.prompt_id)
            prefix_expected[prefix] = prefix_expected.get(prefix, 0) + 1

    # Thread-local-ish session per worker
    sessions = [requests.Session() for _ in range(max_workers)]

    def _task(i: int, item: PromptItem) -> tuple[PromptItem, ChatCallResult]:
        sess = sessions[i % max_workers]
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
    total = len(prompts)

    # Incremental export state
    by_prefix: dict[str, list[tuple[PromptItem, ChatCallResult]]] = {}
    written_prefixes: set[str] = set()
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
                # --- Non-dashboard: group by prefix, export when group complete ---
                prefix = _extract_prompt_id_prefix(item.prompt_id)
                by_prefix.setdefault(prefix, []).append((item, res))

                if (
                    prefix not in written_prefixes
                    and len(by_prefix[prefix]) == prefix_expected[prefix]
                ):
                    path = _write_prefix_xlsx(export_dir, prefix, by_prefix[prefix])
                    written_prefixes.add(prefix)
                    written_files.append(path)
                    log.info(
                        f"    >> Exported {prefix}.xlsx "
                        f"({len(by_prefix[prefix])} rows) "
                        f"[{len(written_prefixes)}/{len(prefix_expected)} groups]"
                    )

    return ExportJobResult(
        export_dir=export_dir,
        counts={"total": len(prompts), "ok": ok_count, "error": err_count},
    )
