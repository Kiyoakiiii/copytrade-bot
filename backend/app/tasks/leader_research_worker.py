from __future__ import annotations

import asyncio
import json
import os
import shutil
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import structlog
from sqlalchemy import select

from app.core.logging import redact_text
from app.db.session import SessionLocal
from app.models import AppSetting


log = structlog.get_logger(__name__)
JOB_PREFIX = "leader_research_job:"
SCRIPTS_DIR = Path("/app/scripts")
CACHE_DIR = Path("/research-cache")
JOB_TIMEOUT_SECONDS = 15 * 60
HISTORY_BUCKET_SECONDS = max(
    60 * 60,
    int(os.getenv("RESEARCH_HISTORY_BUCKET_SECONDS", str(6 * 60 * 60))),
)
CACHE_RETENTION_SECONDS = max(
    HISTORY_BUCKET_SECONDS * 2,
    int(os.getenv("RESEARCH_CACHE_RETENTION_SECONDS", str(2 * 24 * 60 * 60))),
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _prune_public_history_cache() -> None:
    history_root = CACHE_DIR / "public-history"
    if not history_root.exists():
        return
    oldest_ms = int((time.time() - CACHE_RETENTION_SECONDS) * 1000)
    for bucket in history_root.iterdir():
        if not bucket.is_dir():
            continue
        try:
            bucket_ms = int(bucket.name)
        except ValueError:
            continue
        if bucket_ms < oldest_ms:
            shutil.rmtree(bucket, ignore_errors=True)


def _without_addresses(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            str(key): _without_addresses(item)
            for key, item in value.items()
            if str(key).lower() not in {"address", "leader_address"}
        }
    if isinstance(value, list):
        return [_without_addresses(item) for item in value]
    return value


async def _reset_interrupted_jobs() -> None:
    async with SessionLocal() as db:
        rows = (
            await db.execute(
                select(AppSetting).where(AppSetting.key.like(f"{JOB_PREFIX}%"))
            )
        ).scalars().all()
        changed = False
        for row in rows:
            value = dict(row.value or {})
            if value.get("status") != "RUNNING":
                continue
            value.update(
                {
                    "status": "QUEUED",
                    "started_at": None,
                    "progress": "Research worker restarted; job safely requeued",
                }
            )
            row.value = value
            changed = True
        if changed:
            await db.commit()


async def _claim_job() -> tuple[str, dict[str, Any]] | None:
    async with SessionLocal() as db:
        row = (
            await db.execute(
                select(AppSetting)
                .where(AppSetting.key.like(f"{JOB_PREFIX}%"))
                .where(AppSetting.value["status"].as_string() == "QUEUED")
                .order_by(AppSetting.created_at)
                .with_for_update(skip_locked=True)
                .limit(1)
            )
        ).scalar_one_or_none()
        if row is None:
            return None
        value = dict(row.value or {})
        value.update(
            {
                "status": "RUNNING",
                "started_at": _now(),
                "completed_at": None,
                "progress": "Loading cached public history under the protected API budget",
                "error": None,
            }
        )
        row.value = value
        await db.commit()
        return row.key, value


def _command(request: dict[str, str], json_output: Path) -> list[str]:
    address = str(request["address"])
    bucket_ms = HISTORY_BUCKET_SECONDS * 1000
    cutoff_ms = int(time.time() * 1000) // bucket_ms * bucket_ms
    common = [
        "candidate=" + address,
        "--target-tail-pct",
        str(request.get("target_tail_pct") or "7.5"),
        "--round-to",
        str(request.get("round_to") or "10000"),
        "--cache-dir",
        str(CACHE_DIR / "public-history"),
        "--end-ms",
        str(cutoff_ms),
        "--json-output",
        str(json_output),
    ]
    if request["tool"] == "suitability":
        return [
            sys.executable,
            str(SCRIPTS_DIR / "leader_suitability_evaluator.py"),
            *common,
            "--friction-bps",
            str(request.get("friction_bps") or "5"),
        ]
    if request["tool"] == "balance":
        return [
            sys.executable,
            str(SCRIPTS_DIR / "leader_balance_evaluator.py"),
            *common,
            "--follower-balance",
            str(request.get("follower_balance") or "20000"),
        ]
    raise ValueError("unsupported research tool")


async def _run_job(value: dict[str, Any]) -> dict[str, Any]:
    request = dict(value.get("request") or {})
    address = str(request.get("address") or "")
    CACHE_DIR.mkdir(parents=True, exist_ok=True, mode=0o700)
    with tempfile.TemporaryDirectory(prefix="leader-research-") as directory:
        output = Path(directory) / "result.json"
        env = dict(os.environ)
        env["PYTHONPATH"] = f"{SCRIPTS_DIR}:/app"
        process = await asyncio.create_subprocess_exec(
            *_command(request, output),
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
            env=env,
            start_new_session=True,
        )
        try:
            _, stderr = await asyncio.wait_for(
                process.communicate(),
                timeout=JOB_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            process.kill()
            await process.communicate()
            raise RuntimeError("research job exceeded the 15-minute safety timeout")
        except asyncio.CancelledError:
            process.kill()
            await process.communicate()
            raise
        if process.returncode != 0:
            detail = stderr.decode("utf-8", errors="replace")[-2000:]
            if address:
                detail = detail.replace(address, "[public-address-redacted]")
            raise RuntimeError(detail.strip() or "research evaluator failed")
        payload = json.loads(output.read_text(encoding="utf-8"))
        return _without_addresses(payload)


async def _finish_job(key: str, *, result: dict[str, Any] | None, error: str | None) -> None:
    async with SessionLocal() as db:
        row = await db.get(AppSetting, key, with_for_update=True)
        if row is None:
            return
        value = dict(row.value or {})
        request = dict(value.get("request") or {})
        address = str(request.get("address") or "")
        safe_error = redact_text(error or "")[:2000] if error else None
        if address and safe_error:
            safe_error = safe_error.replace(address, "[public-address-redacted]")
        value.update(
            {
                "status": "FAILED" if safe_error else "COMPLETED",
                "completed_at": _now(),
                "progress": "Analysis failed" if safe_error else "Analysis complete",
                "result": result if not safe_error else None,
                "error": safe_error,
            }
        )
        row.value = value
        await db.commit()


async def run() -> None:
    _prune_public_history_cache()
    await _reset_interrupted_jobs()
    log.info("leader_research_worker_started", cpu_policy="isolated_low_priority")
    while True:
        claimed = await _claim_job()
        if claimed is None:
            await asyncio.sleep(2)
            continue
        key, value = claimed
        try:
            result = await _run_job(value)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await _finish_job(key, result=None, error=str(exc))
            log.warning("leader_research_job_failed", job_id=key.removeprefix(JOB_PREFIX))
        else:
            await _finish_job(key, result=result, error=None)
            log.info("leader_research_job_completed", job_id=key.removeprefix(JOB_PREFIX))
        finally:
            _prune_public_history_cache()


if __name__ == "__main__":
    asyncio.run(run())
