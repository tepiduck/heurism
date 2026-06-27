"""Persistent collector status state and rendering."""

from __future__ import annotations

import json
import time
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pmbot.runner.models import CollectorStatus, VenueStatus

STATUS_RELATIVE_PATH = Path("collector") / "status.json"
ONE_HOUR_NS = 3_600_000_000_000
ONE_DAY_NS = 86_400_000_000_000


class CollectorStateStore:
    """Small JSON state file for operator status."""

    def __init__(self, *, data_dir: Path, pairs_tracked: int) -> None:
        self._path = data_dir / STATUS_RELATIVE_PATH
        self._status = self._load() or CollectorStatus(
            started_at_ns=time.time_ns(),
            updated_at_ns=time.time_ns(),
            pairs_tracked=pairs_tracked,
            resolutions_collected=0,
            venues=(
                VenueStatus("polymarket", None, 0, 0, ()),
                VenueStatus("kalshi", None, 0, 0, ()),
            ),
        )
        self._status = CollectorStatus(
            started_at_ns=self._status.started_at_ns,
            updated_at_ns=time.time_ns(),
            pairs_tracked=pairs_tracked,
            resolutions_collected=self._status.resolutions_collected,
            venues=self._status.venues,
        )
        self.save()

    @property
    def status(self) -> CollectorStatus:
        """Return current in-memory status."""
        return self._status

    def note_frame(self, venue: str, *, ts_local_ns: int | None = None) -> None:
        """Record a frame/update for one venue."""
        now = time.time_ns() if ts_local_ns is None else ts_local_ns
        self._update_venue(
            venue,
            lambda item: VenueStatus(
                venue=item.venue,
                last_frame_ns=now,
                frames_total=item.frames_total + 1,
                reconnects=item.reconnects,
                gap_timestamps_ns=item.gap_timestamps_ns,
            ),
        )

    def note_reconnect(self, venue: str) -> None:
        """Record a supervised task restart."""
        self._update_venue(
            venue,
            lambda item: VenueStatus(
                venue=item.venue,
                last_frame_ns=item.last_frame_ns,
                frames_total=item.frames_total,
                reconnects=item.reconnects + 1,
                gap_timestamps_ns=item.gap_timestamps_ns,
            ),
        )

    def note_gap(self, venue: str, *, ts_local_ns: int | None = None) -> None:
        """Record a gap marker timestamp."""
        now = time.time_ns() if ts_local_ns is None else ts_local_ns
        cutoff = now - ONE_DAY_NS
        self._update_venue(
            venue,
            lambda item: VenueStatus(
                venue=item.venue,
                last_frame_ns=item.last_frame_ns,
                frames_total=item.frames_total,
                reconnects=item.reconnects,
                gap_timestamps_ns=tuple(
                    timestamp
                    for timestamp in (*item.gap_timestamps_ns, now)
                    if timestamp >= cutoff
                ),
            ),
        )

    def note_resolutions(self, count: int) -> None:
        """Increment collected resolution count."""
        if count <= 0:
            return
        self._status = CollectorStatus(
            started_at_ns=self._status.started_at_ns,
            updated_at_ns=time.time_ns(),
            pairs_tracked=self._status.pairs_tracked,
            resolutions_collected=self._status.resolutions_collected + count,
            venues=self._status.venues,
        )
        self.save()

    def set_pairs_tracked(self, count: int) -> None:
        """Update the number of currently tracked pairs."""
        if count == self._status.pairs_tracked:
            return
        self._status = CollectorStatus(
            started_at_ns=self._status.started_at_ns,
            updated_at_ns=time.time_ns(),
            pairs_tracked=count,
            resolutions_collected=self._status.resolutions_collected,
            venues=self._status.venues,
        )
        self.save()

    def save(self) -> None:
        """Persist status atomically."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self._path.with_name(f"{self._path.name}.{time.time_ns()}.tmp")
        tmp_path.write_text(json.dumps(asdict(self._status), indent=2), encoding="utf-8")
        tmp_path.replace(self._path)

    def _update_venue(self, venue: str, update: Any) -> None:
        venues = list(self._status.venues)
        for index, item in enumerate(venues):
            if item.venue == venue:
                venues[index] = update(item)
                break
        else:
            venues.append(update(VenueStatus(venue, None, 0, 0, ())))
        self._status = CollectorStatus(
            started_at_ns=self._status.started_at_ns,
            updated_at_ns=time.time_ns(),
            pairs_tracked=self._status.pairs_tracked,
            resolutions_collected=self._status.resolutions_collected,
            venues=tuple(venues),
        )
        self.save()

    def _load(self) -> CollectorStatus | None:
        return _load_status_path(self._path)


def load_status(data_dir: Path) -> CollectorStatus | None:
    """Load status without mutating it."""
    return _load_status_path(data_dir / STATUS_RELATIVE_PATH)


def _load_status_path(path: Path) -> CollectorStatus | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            return None
        venues = tuple(
            VenueStatus(
                venue=str(item["venue"]),
                last_frame_ns=item["last_frame_ns"],
                frames_total=int(item["frames_total"]),
                reconnects=int(item["reconnects"]),
                gap_timestamps_ns=tuple(int(value) for value in item["gap_timestamps_ns"]),
            )
            for item in payload.get("venues", [])
            if isinstance(item, dict)
        )
        return CollectorStatus(
            started_at_ns=int(payload["started_at_ns"]),
            updated_at_ns=int(payload["updated_at_ns"]),
            pairs_tracked=int(payload["pairs_tracked"]),
            resolutions_collected=int(payload["resolutions_collected"]),
            venues=venues,
        )
    except (OSError, json.JSONDecodeError, KeyError, TypeError, ValueError):
        return None


def render_status(status: CollectorStatus | None, *, now_ns: int | None = None) -> str:
    """Render status text for CLI and tests."""
    if status is None:
        return "Collector status unavailable: no status file found."
    now = time.time_ns() if now_ns is None else now_ns
    lines = [
        "pmbot collector status",
        f"uptime: {_format_duration_ns(max(0, now - status.started_at_ns))}",
        f"pairs tracked: {status.pairs_tracked}",
        f"resolutions collected: {status.resolutions_collected}",
    ]
    for venue in status.venues:
        freshness = (
            "never"
            if venue.last_frame_ns is None
            else _format_duration_ns(max(0, now - venue.last_frame_ns)) + " ago"
        )
        frames_per_minute = _frames_per_minute(venue.frames_total, status.started_at_ns, now)
        gaps_24h = sum(1 for timestamp in venue.gap_timestamps_ns if timestamp >= now - ONE_DAY_NS)
        gaps_hour = sum(
            1 for timestamp in venue.gap_timestamps_ns if timestamp >= now - ONE_HOUR_NS
        )
        lines.append(
            f"{venue.venue}: freshness={freshness}, frames/min={frames_per_minute:.2f}, "
            f"reconnects={venue.reconnects}, gaps_1h={gaps_hour}, gaps_24h={gaps_24h}"
        )
    return "\n".join(lines)


def _format_duration_ns(duration_ns: int) -> str:
    seconds = duration_ns // 1_000_000_000
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    if hours:
        return f"{hours}h{minutes:02d}m{seconds:02d}s"
    if minutes:
        return f"{minutes}m{seconds:02d}s"
    return f"{seconds}s"


def _frames_per_minute(frames: int, started_at_ns: int, now_ns: int) -> float:
    elapsed_minutes = max(1e-9, (now_ns - started_at_ns) / 60_000_000_000)
    return frames / elapsed_minutes


def utc_iso_now() -> str:
    """Return current UTC timestamp string."""
    return datetime.now(UTC).isoformat()
