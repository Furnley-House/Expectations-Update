import os
import uuid
import time
import traceback
from threading import Thread, Lock
from datetime import datetime
from flask import Flask, jsonify

from ExpectationsUpdate.PrepareBasicExpectationsFields import prepare_basic_expectations_fields
from ExpectationsUpdate.fetchValidFees import fetch_all_fees
from ExpectationsUpdate.calculateExpectations import update_expectations_file
from ExpectationsUpdate.fillAllFirstAndLastValuations import fill_all_first_and_last_valuations
from ExpectationsUpdate.ImportExpectationsToZoho import upload_expectations

app = Flask(__name__)

_state_lock = Lock()
_current_job_id = None          # currently running *batch* job id
_current_group_id = None        # currently running group id
_jobs = {}


def _utc_ts():
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


def _append_log(job_id, msg):
    with _state_lock:
        if job_id in _jobs:
            _jobs[job_id]["logs"].append(f"{_utc_ts()} {msg}")


def _set_job(job_id, **updates):
    with _state_lock:
        _jobs[job_id].update(updates)


def _run_pipeline(job_id: str, batch: int):
    def step(name, fn):
        _append_log(job_id, f"[STEP] {name} started")
        try:
            fn()
            _append_log(job_id, f"[STEP] {name} finished")
            return True
        except Exception as e:
            _append_log(job_id, f"[STEP] {name} failed: {e}")
            _append_log(job_id, traceback.format_exc())
            return False

    _set_job(job_id, status="running", started_at=time.time())

    ok = step("fetch_all_fees", lambda: fetch_all_fees(batch, 100))
    if ok:
        ok = step("prepare_basic_expectations_fields", prepare_basic_expectations_fields)
    if ok:
        ok = step("fill_all_first_and_last_valuations", fill_all_first_and_last_valuations)
    if ok:
        ok = step("update_expectations_file", update_expectations_file)
    if ok:
        ok = step("upload_expectations", upload_expectations)

    _set_job(job_id, status="completed" if ok else "failed", finished_at=time.time())
    _append_log(job_id, "[DONE] Pipeline finished" if ok else "[DONE] Pipeline failed")
    return ok


def _run_group(group_id: str, batches: int = 10, break_seconds: int = 60):
    global _current_job_id, _current_group_id

    _set_job(group_id, status="running", started_at=time.time())
    _append_log(group_id, f"[INFO] Group started: {batches} batches, {break_seconds}s break")

    ok = True
    for batch in range(1, batches + 1):
        job_id = str(uuid.uuid4())
        with _state_lock:
            _current_job_id = job_id
            _jobs[job_id] = {
                "job_id": job_id,
                "group_id": group_id,
                "batch": batch,
                "status": "queued",
                "created_at": time.time(),
                "started_at": None,
                "finished_at": None,
                "logs": [f"{_utc_ts()} [INFO] Job queued (batch {batch})"],
            }
            _jobs[group_id]["batch_job_ids"].append(job_id)

        _append_log(group_id, f"[INFO] Batch {batch} started as job {job_id}")
        ok = _run_pipeline(job_id, batch=batch)
        _append_log(group_id, f"[INFO] Batch {batch} {'completed' if ok else 'failed'}")

        if not ok:
            break

        if batch != batches:
            _append_log(group_id, f"[INFO] Sleeping {break_seconds}s before next batch")
            time.sleep(break_seconds)

    _set_job(group_id, status="completed" if ok else "failed", finished_at=time.time())
    _append_log(group_id, "[DONE] Group finished" if ok else "[DONE] Group failed")

    with _state_lock:
        _current_job_id = None
        _current_group_id = None


@app.route("/run", methods=["POST"])
def run_once():
    global _current_group_id

    with _state_lock:
        if _current_group_id is not None or _current_job_id is not None:
            gid = _current_group_id
            jid = _current_job_id
            status = _jobs[gid]["status"] if gid and gid in _jobs else (_jobs[jid]["status"] if jid in _jobs else "running")
            return jsonify(
                {
                    "message": "A run is already in progress",
                    "group_id": gid,
                    "current_job_id": jid,
                    "status": status,
                    "group_status_url": f"/status/{gid}" if gid else None,
                    "group_logs_url": f"/logs/{gid}" if gid else None,
                    "status_url": f"/status/{jid}" if jid else None,
                    "logs_url": f"/logs/{jid}" if jid else None,
                }
            ), 409

        group_id = str(uuid.uuid4())
        _current_group_id = group_id
        _jobs[group_id] = {
            "job_id": group_id,
            "type": "group",
            "status": "queued",
            "created_at": time.time(),
            "started_at": None,
            "finished_at": None,
            "batch_job_ids": [],
            "logs": [f"{_utc_ts()} [INFO] Group queued"],
        }

    t = Thread(target=_run_group, args=(group_id, 10, 500), daemon=True)
    t.start()

    return jsonify(
        {
            "message": "Group started (10 batches)",
            "group_id": group_id,
            "group_status_url": f"/status/{group_id}",
            "group_logs_url": f"/logs/{group_id}",
        }
    ), 202


@app.route("/status/<job_id>", methods=["GET"])
def status(job_id):
    with _state_lock:
        job = _jobs.get(job_id)
        current_batch = _current_job_id
        current_group = _current_group_id
    if not job:
        return jsonify({"error": "job_id not found"}), 404
    resp = {k: v for k, v in job.items() if k != "logs"}
    resp["is_current_batch"] = (job_id == current_batch)
    resp["is_current_group"] = (job_id == current_group)
    return jsonify(resp)


@app.route("/logs/<job_id>", methods=["GET"])
def logs(job_id):
    with _state_lock:
        job = _jobs.get(job_id)
    if not job:
        return jsonify({"error": "job_id not found"}), 404
    return jsonify({"job_id": job_id, "status": job["status"], "logs": job["logs"]})


@app.route("/current", methods=["GET"])
def current():
    with _state_lock:
        job_id = _current_job_id
        group_id = _current_group_id
    return jsonify({"current_group_id": group_id, "current_job_id": job_id})


listen_port = int(os.getenv("X_ZOHO_CATALYST_LISTEN_PORT", 9000))
app.run(host="0.0.0.0", port=listen_port)