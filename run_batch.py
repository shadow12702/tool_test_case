from __future__ import annotations

import logging
import math
import sys
import time
from datetime import datetime
from pathlib import Path

from service.batch_runner import run_for_all_users
from service.config_loader import load_api_config
from service.user_generator import generate_users

APP_ROOT = Path(__file__).resolve().parent
LOG_DIR = APP_ROOT / "logs"


def _setup_logging() -> logging.Logger:
    """Log cả ra console lẫn file logs/batch_YYYYMMDD_HHMMSS.log"""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    logger = logging.getLogger("batch_runner")
    logger.setLevel(logging.INFO)

    fmt = logging.Formatter("%(asctime)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    # File handler - luôn ghi log
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    # Console handler - chỉ khi có terminal
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    logger.info(f"Log file: {log_file}")
    return logger


def main() -> int:
    log = _setup_logging()
    cfg = load_api_config(APP_ROOT / "config" / "api_config.json")

    total_users = cfg.total_users
    batch_size = cfg.user_count
    total_batches = math.ceil(total_users / batch_size)

    log.info("=" * 60)
    log.info(f"  BATCH RUNNER")
    log.info(f"  Total users  : {total_users}")
    log.info(f"  Batch size   : {batch_size} users/batch")
    log.info(f"  Total batches: {total_batches}")
    log.info(f"  Models       : {cfg.model_names}")
    log.info(f"  Chat modes   : {list(cfg.chat_modes.keys())}")
    log.info(f"  Max threads  : {cfg.max_user_threads}")
    log.info(f"  Prompt workers/job: {cfg.max_prompt_threads_per_user}")
    log.info("=" * 60)

    global_start = time.perf_counter()
    total_jobs_ok = 0
    total_jobs_err = 0
    total_jobs_count = 0

    for batch_idx in range(total_batches):
        start_user = batch_idx * batch_size + 1
        count = min(batch_size, total_users - batch_idx * batch_size)
        users = generate_users(count, start=start_user)

        log.info(
            f"--- Batch {batch_idx + 1}/{total_batches} "
            f"| users {start_user}-{start_user + count - 1} "
            f"({count} users) ---"
        )

        batch_start = time.perf_counter()
        try:
            result = run_for_all_users(
                cfg=cfg,
                project_root=APP_ROOT,
                export_root=APP_ROOT / "export",
                users=users,
            )
            total_jobs_ok += result.jobs_ok
            total_jobs_err += result.jobs_error
            total_jobs_count += result.jobs_total

            batch_elapsed = time.perf_counter() - batch_start
            log.info(f"    run_id : {result.run_id}")
            log.info(
                f"    jobs   : {result.jobs_total} "
                f"(ok: {result.jobs_ok}, error: {result.jobs_error})"
            )
            log.info(f"    time   : {batch_elapsed:.1f}s")
        except Exception as e:
            batch_elapsed = time.perf_counter() - batch_start
            log.error(f"    BATCH FAILED: {e}")
            log.info(f"    time   : {batch_elapsed:.1f}s")
            total_jobs_err += 1

    total_elapsed = time.perf_counter() - global_start

    log.info("=" * 60)
    log.info(f"  ALL DONE")
    log.info(f"  Batches     : {total_batches}")
    log.info(
        f"  Total jobs  : {total_jobs_count} "
        f"(ok: {total_jobs_ok}, error: {total_jobs_err})"
    )
    log.info(
        f"  Total time  : {total_elapsed:.1f}s "
        f"({total_elapsed / 60:.1f} min)"
    )
    log.info(f"  Avg per batch: {total_elapsed / total_batches:.1f}s")
    log.info("=" * 60)

    return 0 if total_jobs_err == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
