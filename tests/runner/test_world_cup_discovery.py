import asyncio

from pmbot.runner.models import ResolutionRecord
from pmbot.runner.pairs import PairRegistry
from pmbot.runner.resolutions import StaticResolutionSource
from pmbot.runner.state import CollectorStateStore
from pmbot.runner.streams import ResolutionParquetStore
from pmbot.runner.world_cup_discovery import (
    WORLD_CUP_SERIES_TICKER,
    WorldCupAllowlistRefresher,
    WorldCupMatchWinnerDiscovery,
    match_world_cup_match_winner_markets,
)
from pmbot.signals.contracts import VerifiedContractPair


def test_world_cup_discovery_matches_fixture_markets_with_no_token() -> None:
    pairs = match_world_cup_match_winner_markets(
        [_polymarket_fixture()],
        [_kalshi_fixture()],
    )

    assert pairs == (
        VerifiedContractPair(
            polymarket_condition_id="0xwc-colombia-uzbekistan",
            polymarket_asset_id="1111111111111111111111111111111111111111",
            polymarket_no_asset_id="2222222222222222222222222222222222222222",
            kalshi_ticker="KXWCGAME-26JUN27COLUZB-COL",
            rationale=(
                "world cup match-winner auto-discovery: Colombia vs Uzbekistan on 2026-06-27; "
                "exact title, rules, close time, outcome, and yes payout condition match"
            ),
            invert_kalshi=False,
        ),
    )


def test_world_cup_discoverer_queries_gamma_and_kxwcgame_series() -> None:
    async def run() -> None:
        discovery = WorldCupMatchWinnerDiscovery(
            gamma=FakeGammaClient([_polymarket_fixture()]),
            kalshi=FakeKalshiClient([_kalshi_fixture()]),
        )

        pairs = await discovery.discover()

        assert len(pairs) == 1

    asyncio.run(run())


def test_world_cup_discovery_excludes_resolved_markets() -> None:
    closed_polymarket = {**_polymarket_fixture(), "closed": True}
    settled_kalshi = {**_kalshi_fixture(), "status": "settled"}

    assert match_world_cup_match_winner_markets(
        [closed_polymarket],
        [_kalshi_fixture()],
    ) == ()
    assert match_world_cup_match_winner_markets(
        [_polymarket_fixture()],
        [settled_kalshi],
    ) == ()


def test_world_cup_refresh_adds_new_pairs_and_drops_resolved(tmp_path) -> None:
    async def run() -> None:
        old_pair = _pair("0xold", "old-yes", "old-no", "KXWCGAME-26JUN26OLD-OLD")
        keep_pair = _pair("0xkeep", "keep-yes", "keep-no", "KXWCGAME-26JUN26KEEP-KEEP")
        new_pair = _pair("0xnew", "new-yes", "new-no", "KXWCGAME-26JUN27NEW-NEW")
        registry = PairRegistry((old_pair, keep_pair))
        state = CollectorStateStore(data_dir=tmp_path, pairs_tracked=2)
        store = ResolutionParquetStore(data_dir=tmp_path)
        record = ResolutionRecord(
            venue="kalshi",
            market_id=old_pair.kalshi_ticker,
            resolved_outcome="yes",
            resolved_at="2026-06-26T22:00:00Z",
            fetched_at="2026-06-26T22:01:00Z",
            raw_json=None,
        )
        refresher = WorldCupAllowlistRefresher(
            registry=registry,
            discoverer=StaticDiscoverer((new_pair,)),
            resolution_sources=(StaticResolutionSource((record,)),),
            resolution_store=store,
            state=state,
        )

        refreshed = await refresher.refresh_once()

        assert refreshed == (keep_pair, new_pair)
        assert registry.snapshot() == (keep_pair, new_pair)
        assert store.count == 1
        assert state.status.resolutions_collected == 1
        assert state.status.pairs_tracked == 2

    asyncio.run(run())


def test_world_cup_discovery_never_adds_non_world_cup_market() -> None:
    non_wc_polymarket = {
        **_polymarket_fixture(),
        "eventTitle": "International friendly: Colombia vs Uzbekistan",
        "slug": "colombia-uzbekistan-friendly",
    }
    non_wc_kalshi = {
        **_kalshi_fixture(),
        "ticker": "KXFRIENDLY-26JUN27COLUZB-COL",
    }

    assert match_world_cup_match_winner_markets(
        [non_wc_polymarket],
        [_kalshi_fixture()],
    ) == ()
    assert match_world_cup_match_winner_markets(
        [_polymarket_fixture()],
        [non_wc_kalshi],
    ) == ()


class StaticDiscoverer:
    def __init__(self, pairs: tuple[VerifiedContractPair, ...]) -> None:
        self._pairs = pairs

    async def discover(self) -> tuple[VerifiedContractPair, ...]:
        return self._pairs


class FakeGammaClient:
    def __init__(self, markets: list[dict[str, object]]) -> None:
        self._markets = markets

    async def fetch_markets(
        self,
        *,
        active: bool | None = None,
        closed: bool | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, object]]:
        assert active is True
        assert closed is False
        assert limit == 200
        return self._markets[offset : offset + limit]


class FakeKalshiClient:
    def __init__(self, markets: list[dict[str, object]]) -> None:
        self._markets = markets

    async def fetch_markets(
        self,
        *,
        status: str | None = None,
        series_ticker: str | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> tuple[list[dict[str, object]], str | None]:
        del cursor
        assert status == "open"
        assert series_ticker == WORLD_CUP_SERIES_TICKER
        assert limit == 200
        return self._markets, None


def _pair(
    condition_id: str,
    yes_asset_id: str,
    no_asset_id: str,
    kalshi_ticker: str,
) -> VerifiedContractPair:
    return VerifiedContractPair(
        polymarket_condition_id=condition_id,
        polymarket_asset_id=yes_asset_id,
        polymarket_no_asset_id=no_asset_id,
        kalshi_ticker=kalshi_ticker,
        rationale="fixture",
    )


def _polymarket_fixture() -> dict[str, object]:
    return {
        "conditionId": "0xwc-colombia-uzbekistan",
        "question": "Will Colombia win on June 27, 2026?",
        "eventTitle": "2026 FIFA World Cup: Colombia vs Uzbekistan",
        "slug": "2026-fifa-world-cup-colombia-vs-uzbekistan",
        "outcomes": '["Yes","No"]',
        "clobTokenIds": (
            '["1111111111111111111111111111111111111111",'
            '"2222222222222222222222222222222222222222"]'
        ),
        "active": True,
        "closed": False,
        "endDateIso": "2026-06-27T22:00:00Z",
    }


def _kalshi_fixture() -> dict[str, object]:
    return {
        "ticker": "KXWCGAME-26JUN27COLUZB-COL",
        "title": "Will Colombia win against Uzbekistan on June 27, 2026?",
        "event_title": "2026 FIFA World Cup: Colombia vs Uzbekistan",
        "status": "open",
        "close_time": "2026-06-27T22:00:00Z",
    }
