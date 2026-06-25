import pytest


@pytest.mark.xfail(
    strict=True,
    reason=(
        "TODO: capture ~30s of real Polymarket CLOB market-channel frames "
        "from a non-SWG network; this workspace fails CLOB websocket TLS "
        "behind the corporate SWG before any frame is received."
    ),
)
def test_real_clob_market_channel_frame_fixture_validation_pending() -> None:
    pytest.fail("real CLOB market-channel frame fixture has not been captured yet")
