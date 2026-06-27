"""Background raw-data compaction task for long-running collectors."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from pmbot.ingestion.compaction import CompactionSummary, compact_raw_data

LOGGER = logging.getLogger(__name__)


class RawDataCompactionTask:
    """Periodically compact closed raw-data date partitions."""

    name = "compaction"

    def __init__(
        self,
        *,
        data_dir: Path,
        interval_seconds: float,
    ) -> None:
        self._data_dir = data_dir
        self._interval_seconds = interval_seconds

    async def run(self) -> None:
        """Run compaction forever until cancelled."""
        while True:
            summary = await asyncio.to_thread(compact_raw_data, self._data_dir)
            _log_summary(summary)
            await asyncio.sleep(self._interval_seconds)


def _log_summary(summary: CompactionSummary) -> None:
    if summary.partitions_seen == 0:
        return
    LOGGER.info(
        "raw compaction: seen=%s compacted=%s active_skipped=%s files_removed=%s rows=%s",
        summary.partitions_seen,
        summary.partitions_compacted,
        summary.partitions_skipped_active,
        summary.files_removed,
        summary.rows_written,
    )
