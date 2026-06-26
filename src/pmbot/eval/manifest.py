"""Immutable evaluation run manifests."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from pmbot.eval.models import EvaluationReport


def manifest_payload(
    *,
    report: EvaluationReport,
    config: dict[str, object],
    data_start_ns: int,
    data_end_ns: int,
) -> dict[str, object]:
    """Build a JSON-serializable immutable run manifest payload."""
    config_json = json.dumps(config, default=str, sort_keys=True, separators=(",", ":"))
    return {
        "created_at": datetime.now(UTC).isoformat(),
        "config": config,
        "config_hash": hashlib.sha256(config_json.encode("utf-8")).hexdigest(),
        "data_time_range": {"start_ns": data_start_ns, "end_ns": data_end_ns},
        "n_observations": report.scoring.n,
        "distinct_event_count": report.scoring.distinct_event_count,
        "headline": report.headline,
        "metrics": _jsonable(asdict(report)),
    }


def write_run_manifest(
    path: Path,
    *,
    report: EvaluationReport,
    config: dict[str, object],
    data_start_ns: int,
    data_end_ns: int,
) -> Path:
    """Write one immutable manifest; refuse to overwrite existing output."""
    if path.exists():
        raise FileExistsError(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = manifest_payload(
        report=report,
        config=config,
        data_start_ns=data_start_ns,
        data_end_ns=data_end_ns,
    )
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


def _jsonable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_jsonable(item) for item in value]
    return value
