from __future__ import annotations

import logging
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

    users = generate_users(cfg.user_count)

    log.info("=" * 60)
    log.info(f"  BATCH RUNNER")
    log.info(f"  Users        : {cfg.user_count}")
    log.info(f"  Models       : {cfg.model_names}")
    log.info(f"  Chat modes   : {list(cfg.chat_modes.keys())}")
    log.info(f"  Max threads  : {cfg.max_user_threads}")
    log.info(f"  Prompt workers/job: {cfg.max_prompt_threads_per_user}")
    log.info("=" * 60)

    start = time.perf_counter()
    try:
        result = run_for_all_users(
            cfg=cfg,
            project_root=APP_ROOT,
            export_root=APP_ROOT / "export",
            users=users,
        )
    except Exception as e:
        elapsed = time.perf_counter() - start
        log.error(f"  BATCH FAILED: {e}")
        log.info(f"  Time: {elapsed:.1f}s")
        return 1

    elapsed = time.perf_counter() - start

    log.info("=" * 60)
    log.info(f"  ALL DONE")
    log.info(f"  run_id : {result.run_id}")
    log.info(
        f"  Jobs   : {result.jobs_total} "
        f"(ok: {result.jobs_ok}, error: {result.jobs_error})"
    )
    log.info(
        f"  Time   : {elapsed:.1f}s "
        f"({elapsed / 60:.1f} min)"
    )
    log.info("=" * 60)

    return 0 if result.jobs_error == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
