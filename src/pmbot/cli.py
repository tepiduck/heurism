"""Command-line interface for pmbot."""

import asyncio
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from pmbot.config import Settings
from pmbot.ingestion.runner import run_ingestion
from pmbot.runner import load_status, render_status, run_collector

app = typer.Typer(
    help="Polymarket microstructure research and execution scaffold.",
    no_args_is_help=True,
)
console = Console()


@app.callback()
def main() -> None:
    """Run pmbot commands."""


@app.command()
def ingest(
    markets: Annotated[
        str,
        typer.Option(
            "--markets",
            help="Comma-separated CLOB token IDs, or all-active.",
        ),
    ],
    duration: Annotated[
        int,
        typer.Option(
            "--duration",
            min=1,
            help="Number of seconds to record market data.",
        ),
    ] = 30,
) -> None:
    """Record read-only public Polymarket market data."""
    asyncio.run(run_ingestion(markets=markets, duration_seconds=duration, settings=Settings()))


@app.command("run-collector")
def run_collector_command(
    allowlist: Annotated[
        Path,
        typer.Option(
            "--allowlist",
            exists=True,
            dir_okay=False,
            readable=True,
            help="Human-curated verified Polymarket/Kalshi pair allowlist.",
        ),
    ],
    duration: Annotated[
        float | None,
        typer.Option(
            "--duration",
            min=1,
            help="Optional bounded run duration in seconds; omit for 24/7 operation.",
        ),
    ] = None,
    resolution_interval: Annotated[
        float,
        typer.Option(
            "--resolution-interval",
            min=1,
            help="Seconds between resolution polls.",
        ),
    ] = 900.0,
    discovery_interval: Annotated[
        float | None,
        typer.Option(
            "--discovery-interval",
            min=1,
            help="Seconds between World Cup match-winner discovery refreshes.",
        ),
    ] = None,
) -> None:
    """Run the read-only Polymarket/Kalshi data collector."""
    settings = Settings()
    asyncio.run(
        run_collector(
            allowlist_path=allowlist,
            settings=settings,
            duration_seconds=duration,
            resolution_interval_seconds=resolution_interval,
            discovery_interval_seconds=(
                settings.DISCOVERY_INTERVAL_SECONDS
                if discovery_interval is None
                else discovery_interval
            ),
        )
    )


@app.command("status")
def status_command() -> None:
    """Show collector freshness and heartbeat status."""
    settings = Settings()
    console.print(render_status(load_status(Path(settings.DATA_DIR))))
