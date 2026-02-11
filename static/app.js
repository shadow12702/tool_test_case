function setStatus(text) {
	document.getElementById("status").textContent = text;
}

async function runExport() {
	const runBtn = document.getElementById("runBtn");
	runBtn.disabled = true;
	try {
		setStatus("Running...");

		const res = await fetch("/api/run_batch", { method: "POST" });
		const payload = await res.json().catch(() => ({}));
		if (!res.ok || !payload.ok) {
			throw new Error(payload.error || `HTTP ${res.status}`);
		}

		setStatus(
			[
				"DONE",
				`run_id: ${payload.run_id}`,
				`export_root: ${payload.export_root}`,
				`users: ${payload.users_total} | models: ${payload.models_total} | chat_modes: ${payload.chat_modes_total}`,
				`jobs: ${payload.jobs_total} (ok: ${payload.jobs_ok}, error: ${payload.jobs_error})`,
			].join("\n"),
		);
	} catch (e) {
		setStatus(`ERROR: ${e?.message || e}`);
	} finally {
		runBtn.disabled = false;
	}
}

document.getElementById("runBtn").addEventListener("click", runExport);
setStatus("Ready.");
