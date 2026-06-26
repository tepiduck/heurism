"""Application settings for pmbot.

Secrets are loaded from environment variables or a local .env file only.
"""

import os
from enum import StrEnum
from pathlib import Path
from typing import Self

from pydantic import AnyHttpUrl, AnyUrl, Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Mode(StrEnum):
    """Supported runtime modes."""

    PAPER = "paper"
    LIVE = "live"


class Settings(BaseSettings):
    """Typed settings loaded from environment variables and .env."""

    model_config = SettingsConfigDict(
        case_sensitive=True,
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    CLOB_HTTP_URL: AnyHttpUrl | None = None
    CLOB_WS_URL: AnyUrl | None = None
    GAMMA_URL: AnyHttpUrl | None = None
    WALLET_PRIVATE_KEY: SecretStr | None = Field(default=None, repr=False)
    KALSHI_API_KEY_ID: SecretStr | None = Field(default=None, repr=False)
    KALSHI_PRIVATE_KEY_PATH: Path | None = Field(default=None, repr=False)
    MODE: Mode = Mode.PAPER
    DATA_DIR: Path = Path("data")
    LIVE: str | None = Field(default=None, exclude=True, repr=False)

    @model_validator(mode="after")
    def require_live_flag_for_live_mode(self) -> Self:
        """Require an explicit environment gate before live mode can load."""
        if self.MODE == Mode.LIVE and os.getenv("LIVE", self.LIVE) != "1":
            raise ValueError("MODE=live requires LIVE=1")
        return self
