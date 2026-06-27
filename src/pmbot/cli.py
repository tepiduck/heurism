"""Command-line interface for pmbot."""

import asyncio
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from pmbot.config import Settings
from pmbot.ingestion.runner import run_ingestion
from pmbot.runner import (
    HARVEST_STATUS_RELATIVE_PATH,
    HarvestRequest,
    load_status,
    render_status,
    run_collector,
    run_harvest,
)

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


@app.command("harvest")
def harvest_command(
    polymarket_markets: Annotated[
        Path | None,
        typer.Option(
            "--polymarket-markets",
            exists=True,
            dir_okay=False,
            readable=True,
            help="File of Polymarket condition IDs or CLOB token/asset IDs.",
        ),
    ] = None,
    kalshi_tickers: Annotated[
        Path | None,
        typer.Option(
            "--kalshi-tickers",
            exists=True,
            dir_okay=False,
            readable=True,
            help="File of Kalshi market tickers.",
        ),
    ] = None,
    kalshi_series: Annotated[
        list[str] | None,
        typer.Option(
            "--kalshi-series",
            help="Harvest all open markets from one Kalshi series ticker.",
        ),
    ] = None,
    polymarket_series: Annotated[
        list[str] | None,
        typer.Option(
            "--polymarket-series",
            help="Harvest all active open markets from one Polymarket event series slug.",
        ),
    ] = None,
    polymarket_tags: Annotated[
        list[str] | None,
        typer.Option(
            "--polymarket-tag",
            help="Harvest all active open markets from one Polymarket tag slug.",
        ),
    ] = None,
    duration: Annotated[
        float | None,
        typer.Option(
            "--duration",
            min=1,
            help="Optional bounded run duration in seconds; omit for 24/7 operation.",
        ),
    ] = None,
    poll_resolutions: Annotated[
        bool,
        typer.Option(
            "--poll-resolutions",
            help="Poll resolved outcomes for harvested markets when available.",
        ),
    ] = False,
    resolution_interval: Annotated[
        float,
        typer.Option(
            "--resolution-interval",
            min=1,
            help="Seconds between optional resolution polls.",
        ),
    ] = 900.0,
    data_dir: Annotated[
        Path | None,
        typer.Option(
            "--data-dir",
            file_okay=False,
            help="Data directory for this harvest run; defaults to Settings.DATA_DIR.",
        ),
    ] = None,
    status_file: Annotated[
        Path | None,
        typer.Option(
            "--status-file",
            dir_okay=False,
            help="Harvest status JSON path; defaults to DATA_DIR/harvest/status.json.",
        ),
    ] = None,
) -> None:
    """Run generic read-only single-venue raw market-data harvest."""
    asyncio.run(
        run_harvest(
            request=HarvestRequest(
                polymarket_markets_file=polymarket_markets,
                kalshi_tickers_file=kalshi_tickers,
                kalshi_series=tuple(kalshi_series or ()),
                polymarket_series=tuple(polymarket_series or ()),
                polymarket_tags=tuple(polymarket_tags or ()),
                duration_seconds=duration,
                poll_resolutions=poll_resolutions,
                resolution_interval_seconds=resolution_interval,
                data_dir=data_dir,
                status_file=status_file,
            ),
            settings=Settings(),
        )
    )


@app.command("status")
def status_command() -> None:
    """Show collector freshness and heartbeat status."""
    settings = Settings()
    console.print(render_status(load_status(Path(settings.DATA_DIR))))


@app.command("harvest-status")
def harvest_status_command(
    data_dir: Annotated[
        Path | None,
        typer.Option(
            "--data-dir",
            file_okay=False,
            help="Harvest data directory; defaults to Settings.DATA_DIR.",
        ),
    ] = None,
    status_file: Annotated[
        Path | None,
        typer.Option(
            "--status-file",
            dir_okay=False,
            help="Harvest status JSON path; defaults to DATA_DIR/harvest/status.json.",
        ),
    ] = None,
) -> None:
    """Show harvest freshness and heartbeat status."""
    settings = Settings()
    console.print(
        render_status(
            load_status(
                Path(data_dir or settings.DATA_DIR),
                status_path=status_file,
                relative_path=HARVEST_STATUS_RELATIVE_PATH,
            ),
            tracked_label="markets tracked",
        )
    )
