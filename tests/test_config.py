import pytest
from pydantic import SecretStr

from pmbot.config import Mode, Settings


def test_settings_loads_with_paper_mode_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MODE", raising=False)
    monkeypatch.delenv("LIVE", raising=False)

    settings = Settings(_env_file=None)

    assert settings.MODE == Mode.PAPER


def test_live_mode_with_live_unset_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MODE", "live")
    monkeypatch.delenv("LIVE", raising=False)

    with pytest.raises(ValueError, match="MODE=live requires LIVE=1"):
        Settings(_env_file=None)


def test_live_mode_with_live_zero_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MODE", "live")
    monkeypatch.setenv("LIVE", "0")

    with pytest.raises(ValueError, match="MODE=live requires LIVE=1"):
        Settings(_env_file=None)


def test_live_mode_with_live_one_loads(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MODE", "live")
    monkeypatch.setenv("LIVE", "1")

    settings = Settings(_env_file=None)

    assert settings.MODE == Mode.LIVE


def test_kalshi_credentials_load_without_plain_repr(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KALSHI_API_KEY_ID", "test-key-id")
    monkeypatch.setenv("KALSHI_PRIVATE_KEY_PATH", "secrets/kalshi-test.pem")

    settings = Settings(_env_file=None)

    assert isinstance(settings.KALSHI_API_KEY_ID, SecretStr)
    assert settings.KALSHI_API_KEY_ID.get_secret_value() == "test-key-id"
    assert str(settings.KALSHI_PRIVATE_KEY_PATH) == "secrets/kalshi-test.pem"
    assert "test-key-id" not in repr(settings)
