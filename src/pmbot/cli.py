"""Command-line interface for pmbot."""

import asyncio
from typing import Annotated

import typer

from pmbot.config import Settings
from pmbot.ingestion.runner import run_ingestion

app = typer.Typer(
    help="Polymarket microstructure research and execution scaffold.",
    no_args_is_help=True,
)


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
