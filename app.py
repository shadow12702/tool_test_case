from __future__ import annotations

from pathlib import Path

from flask import Flask, jsonify, send_from_directory

from service.config_loader import load_api_config
from service.batch_runner import run_for_all_users


APP_ROOT = Path(__file__).resolve().parent
STATIC_DIR = APP_ROOT / "static"


def create_app() -> Flask:
    app = Flask(__name__, static_folder=None)

    @app.get("/")
    def index():
        return send_from_directory(STATIC_DIR, "index.html")

    @app.get("/static/<path:filename>")
    def static_files(filename: str):
        return send_from_directory(STATIC_DIR, filename)

    @app.get("/api/config")
    def api_config():
        cfg = load_api_config(APP_ROOT / "config" / "api_config.json")
        return jsonify(cfg.to_public_dict())

    @app.post("/api/run_batch")
    def api_run_batch():
        """
        All config (user_count, model_names, chat_modes, prompt files, etc.)
        is read from config/api_config.json. No form data needed.
        """
        cfg = load_api_config(APP_ROOT / "config" / "api_config.json")

        try:
            result = run_for_all_users(
                cfg=cfg,
                project_root=APP_ROOT,
                export_root=APP_ROOT / "export",
            )
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 400

        return jsonify(
            {
                "ok": True,
                "run_id": result.run_id,
                "export_root": str(result.export_root),
                "users_total": result.users_total,
                "models_total": result.models_total,
                "chat_modes_total": result.chat_modes_total,
                "jobs_total": result.jobs_total,
                "jobs_ok": result.jobs_ok,
                "jobs_error": result.jobs_error,
            }
        )

    return app


app = create_app()


if __name__ == "__main__":
    # For local dev without `flask run`
    app.run(host="0.0.0.0", port=5001, debug=True)
