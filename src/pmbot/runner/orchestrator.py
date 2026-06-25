"""Supervised async orchestration for read-only data collection."""

from __future__ import annotations

import asyncio
import contextlib
import time
from dataclasses import dataclass
from typing import Protocol

from pmbot.ingestion.clob import BackoffPolicy
from pmbot.runner.resolutions import ResolutionPoller
from pmbot.runner.state import CollectorStateStore


class VenueTask(Protocol):
    """A long-running read-only venue ingestion task."""

    name: str

    async def run(self) -> None:
        """Run until cancelled, disconnected, or failed."""


@dataclass(frozen=True, slots=True)
class CollectorOrchestrator:
    """Run venue ingestion and resolution polling under independent supervisors."""

    venue_tasks: tuple[VenueTask, ...]
    state: CollectorStateStore
    resolution_poller: ResolutionPoller | None = None
    backoff: BackoffPolicy = BackoffPolicy(initial_seconds=1.0, max_seconds=30.0)
    resolution_interval_seconds: float = 900.0

    async def run(self, *, duration_seconds: float | None = None) -> None:
        """Run supervised tasks until cancelled or optional duration expires."""
        deadline = None if duration_seconds is None else time.monotonic() + duration_seconds
        tasks = [
            asyncio.create_task(self._supervise_venue(task, deadline=deadline))
            for task in self.venue_tasks
        ]
        if self.resolution_poller is not None:
            tasks.append(asyncio.create_task(self._run_resolution_poller(deadline=deadline)))
        try:
            if deadline is None:
                await asyncio.gather(*tasks)
            else:
                await asyncio.sleep(max(0.0, deadline - time.monotonic()))
        finally:
            for task in tasks:
                task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.gather(*tasks)

    async def _supervise_venue(self, venue_task: VenueTask, *, deadline: float | None) -> None:
        attempt = 0
        while deadline is None or time.monotonic() < deadline:
            try:
                await venue_task.run()
                attempt = 0
            except asyncio.CancelledError:
                raise
            except Exception:
                attempt += 1
                self.state.note_reconnect(venue_task.name)
            delay_attempt = min(max(1, attempt), self.backoff.max_retries)
            delay = self.backoff.delay(delay_attempt)
            if deadline is not None:
                delay = min(delay, max(0.0, deadline - time.monotonic()))
            if delay > 0:
                await asyncio.sleep(delay)

    async def _run_resolution_poller(self, *, deadline: float | None) -> None:
        if self.resolution_poller is None:
            return
        while deadline is None or time.monotonic() < deadline:
            await self.resolution_poller.poll_once()
            delay = self.resolution_interval_seconds
            if deadline is not None:
                delay = min(delay, max(0.0, deadline - time.monotonic()))
            if delay > 0:
                await asyncio.sleep(delay)
