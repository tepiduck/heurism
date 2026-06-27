"""World Cup match-winner auto-discovery only.

Auto-discovery is restricted to the vetted FIFA World Cup single-team
match-winner family. Adding any other market family requires prior human
verification that the Polymarket and Kalshi resolution criteria are equivalent.
"""

from __future__ import annotations

import asyncio
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import date
from typing import Protocol, cast

import httpx

from pmbot.runner.models import ResolutionRecord
from pmbot.runner.pairs import PairRegistry, merge_tracked_pairs
from pmbot.runner.resolutions import ResolutionSource
from pmbot.runner.signal_logging import SignalEventObserver
from pmbot.runner.state import CollectorStateStore
from pmbot.runner.streams import ResolutionParquetStore
from pmbot.signals.contracts import ContractMapper, ContractMetadata, VerifiedContractPair

WORLD_CUP_SERIES_TICKER = "KXWCGAME"
WORLD_CUP_DISCOVERY_RATIONALE = "world cup match-winner auto-discovery"


class GammaMarketClient(Protocol):
    """Gamma subset needed by World Cup discovery."""

    async def fetch_markets(
        self,
        *,
        active: bool | None = None,
        closed: bool | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Mapping[str, object]]:
        """Fetch one Gamma market page."""


class KalshiSeriesClient(Protocol):
    """Kalshi subset needed by World Cup discovery."""

    async def fetch_markets(
        self,
        *,
        status: str | None = None,
        series_ticker: str | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> tuple[list[Mapping[str, object]], str | None]:
        """Fetch one Kalshi market page."""


class WorldCupPairDiscoverer(Protocol):
    """Async source of verified World Cup match-winner pairs."""

    async def discover(self) -> tuple[VerifiedContractPair, ...]:
        """Discover currently unresolved World Cup match-winner pairs."""


@dataclass(frozen=True, slots=True)
class WorldCupMatchWinnerDiscovery:
    """Discover vetted World Cup match-winner Polymarket/Kalshi pairs."""

    gamma: GammaMarketClient
    kalshi: KalshiSeriesClient
    page_size: int = 200

    async def discover(self) -> tuple[VerifiedContractPair, ...]:
        """Fetch venue metadata and return verified unresolved pair matches."""
        polymarket, kalshi = await asyncio.gather(
            self._fetch_polymarket_markets(),
            self._fetch_kalshi_markets(),
        )
        return match_world_cup_match_winner_markets(polymarket, kalshi)

    async def _fetch_polymarket_markets(self) -> tuple[Mapping[str, object], ...]:
        markets: list[Mapping[str, object]] = []
        offset = 0
        while True:
            try:
                page = await self.gamma.fetch_markets(
                    active=True,
                    closed=False,
                    limit=self.page_size,
                    offset=offset,
                )
            except httpx.HTTPStatusError as exc:
                if offset > 0 and exc.response.status_code == 422:
                    break
                raise
            markets.extend(page)
            if len(page) < self.page_size:
                break
            offset += self.page_size
        return tuple(markets)

    async def _fetch_kalshi_markets(self) -> tuple[Mapping[str, object], ...]:
        markets: list[Mapping[str, object]] = []
        cursor: str | None = None
        while True:
            page, cursor = await self.kalshi.fetch_markets(
                status="open",
                series_ticker=WORLD_CUP_SERIES_TICKER,
                limit=self.page_size,
                cursor=cursor,
            )
            markets.extend(page)
            if not page or cursor is None:
                break
        return tuple(markets)


@dataclass(frozen=True, slots=True)
class WorldCupAllowlistRefresher:
    """Periodically refresh the collector's tracked World Cup pair set."""

    registry: PairRegistry
    discoverer: WorldCupPairDiscoverer
    resolution_sources: tuple[ResolutionSource, ...]
    resolution_store: ResolutionParquetStore
    state: CollectorStateStore | None = None
    observer: SignalEventObserver | None = None

    async def refresh_once(self) -> tuple[VerifiedContractPair, ...]:
        """Add newly discovered pairs and drop resolved markets."""
        current_pairs = self.registry.snapshot()
        resolution_records = await _fetch_resolution_records(
            pairs=current_pairs,
            sources=self.resolution_sources,
        )
        written = self.resolution_store.record_many(resolution_records)
        if self.state is not None:
            self.state.note_resolutions(written)

        discovered_pairs = await self.discoverer.discover()
        resolved_market_ids = {record.market_id for record in resolution_records}
        next_pairs = merge_tracked_pairs(
            current_pairs,
            discovered_pairs,
            resolved_market_ids=resolved_market_ids,
        )
        self.registry.replace(next_pairs)
        if self.observer is not None:
            self.observer.set_pairs(next_pairs)
        if self.state is not None:
            self.state.set_pairs_tracked(len(next_pairs))
        return next_pairs

    async def run(self, *, interval_seconds: float = 1800.0) -> None:
        """Refresh forever at a fixed interval."""
        while True:
            await self.refresh_once()
            await asyncio.sleep(interval_seconds)


def match_world_cup_match_winner_markets(
    polymarket_markets: Sequence[Mapping[str, object]],
    kalshi_markets: Sequence[Mapping[str, object]],
) -> tuple[VerifiedContractPair, ...]:
    """Match unresolved World Cup match-winner markets into verified pairs."""
    polymarket_legs = tuple(
        leg for raw in polymarket_markets if (leg := _polymarket_leg(raw)) is not None
    )
    kalshi_legs = tuple(leg for raw in kalshi_markets if (leg := _kalshi_leg(raw)) is not None)
    kalshi_by_key = {leg.match_key: leg for leg in kalshi_legs}

    pairs: list[VerifiedContractPair] = []
    mapper = ContractMapper()
    for polymarket in polymarket_legs:
        kalshi = kalshi_by_key.get(polymarket.match_key)
        if kalshi is None:
            continue
        yes_payout = _canonical_yes_payout(polymarket.team, polymarket.opponent, polymarket.day)
        metadata = _canonical_metadata(polymarket.team, polymarket.opponent, polymarket.day)
        result = mapper.verify_pair(
            polymarket=ContractMetadata(
                venue="polymarket",
                identifier=polymarket.identifier,
                title=metadata.title,
                rules=metadata.rules,
                close_time=metadata.close_time,
                yes_payout_condition=yes_payout,
            ),
            kalshi=ContractMetadata(
                venue="kalshi",
                identifier=kalshi.identifier,
                title=metadata.title,
                rules=metadata.rules,
                close_time=metadata.close_time,
                yes_payout_condition=yes_payout,
            ),
            polymarket_asset_id=polymarket.yes_token_id,
            polymarket_no_asset_id=polymarket.no_token_id,
        )
        if isinstance(result, VerifiedContractPair):
            pairs.append(
                replace(
                    result,
                    rationale=(
                        f"{WORLD_CUP_DISCOVERY_RATIONALE}: {polymarket.team} vs "
                        f"{polymarket.opponent} on {polymarket.day.isoformat()}; "
                        f"{result.rationale}"
                    ),
                )
            )
    return tuple(sorted(pairs, key=lambda pair: (pair.polymarket_condition_id, pair.kalshi_ticker)))


async def _fetch_resolution_records(
    *,
    pairs: tuple[VerifiedContractPair, ...],
    sources: tuple[ResolutionSource, ...],
) -> tuple[ResolutionRecord, ...]:
    records: list[ResolutionRecord] = []
    for source in sources:
        records.extend(await source.fetch(pairs))
    return tuple(records)


@dataclass(frozen=True, slots=True)
class _CanonicalMetadata:
    title: str
    rules: str
    close_time: str


@dataclass(frozen=True, slots=True)
class _MarketLeg:
    identifier: str
    team: str
    opponent: str
    day: date
    match_key: tuple[str, str, tuple[str, str]]
    yes_token_id: str
    no_token_id: str


def _polymarket_leg(raw: Mapping[str, object]) -> _MarketLeg | None:
    if not _is_open_polymarket(raw) or not _has_world_cup_marker(raw):
        return None
    question = _first_optional_str(raw, "question", "title")
    if question is None:
        return None
    parsed = _parse_win_question(question)
    if parsed is None:
        return None
    team, opponent, date_text = parsed
    day = _parse_date(date_text, default_year=_year_from_market(raw))
    if day is None:
        return None
    opponent = opponent or _opponent_from_matchup_texts(raw, team)
    if opponent is None:
        return None
    yes_token_id, no_token_id = _polymarket_yes_no_token_ids(raw)
    if yes_token_id is None or no_token_id is None:
        return None
    return _MarketLeg(
        identifier=_first_present_str(raw, "conditionId", "condition_id", "market"),
        team=_clean_team(team),
        opponent=_clean_team(opponent),
        day=day,
        match_key=_match_key(team, opponent, day),
        yes_token_id=yes_token_id,
        no_token_id=no_token_id,
    )


def _kalshi_leg(raw: Mapping[str, object]) -> _MarketLeg | None:
    ticker = _optional_str(raw.get("ticker"))
    if ticker is None or not ticker.startswith(f"{WORLD_CUP_SERIES_TICKER}-"):
        return None
    if not _is_open_kalshi(raw):
        return None
    question = _first_optional_str(raw, "title", "subtitle", "yes_sub_title")
    if question is None:
        return None
    parsed = _parse_win_question(question)
    if parsed is None:
        return None
    team, opponent, date_text = parsed
    day = (
        _parse_date(date_text, default_year=_year_from_market(raw))
        or _date_from_kalshi_ticker(ticker)
        or _date_from_market(raw)
    )
    if day is None:
        return None
    opponent = opponent or _opponent_from_matchup_texts(raw, team)
    if opponent is None:
        return None
    return _MarketLeg(
        identifier=ticker,
        team=_clean_team(team),
        opponent=_clean_team(opponent),
        day=day,
        match_key=_match_key(team, opponent, day),
        yes_token_id=ticker,
        no_token_id=ticker,
    )


def _parse_win_question(question: str) -> tuple[str, str | None, str | None] | None:
    normalized = " ".join(question.strip().rstrip("?").split())
    with_opponent = re.fullmatch(
        r"Will (?P<team>.+?) win (?:against|versus|vs\.?) (?P<opponent>.+?)"
        r"(?: on (?P<date>.+))?",
        normalized,
        flags=re.IGNORECASE,
    )
    if with_opponent is not None:
        return (
            with_opponent.group("team"),
            with_opponent.group("opponent"),
            with_opponent.group("date"),
        )
    no_opponent = re.fullmatch(
        r"Will (?P<team>.+?) win on (?P<date>.+)",
        normalized,
        flags=re.IGNORECASE,
    )
    if no_opponent is not None:
        return no_opponent.group("team"), None, no_opponent.group("date")
    return None


def _polymarket_yes_no_token_ids(raw: Mapping[str, object]) -> tuple[str | None, str | None]:
    outcomes = _json_string_list(raw.get("outcomes"))
    token_ids = _json_string_list(raw.get("clobTokenIds"))
    if len(token_ids) < 2:
        return None, None
    yes_index = _index_of(outcomes, "yes") if outcomes else 0
    no_index = _index_of(outcomes, "no") if outcomes else 1
    if yes_index is None or no_index is None:
        return None, None
    if yes_index >= len(token_ids) or no_index >= len(token_ids):
        return None, None
    return token_ids[yes_index], token_ids[no_index]


def _index_of(values: Sequence[str], expected: str) -> int | None:
    for index, value in enumerate(values):
        if value.strip().casefold() == expected:
            return index
    return None


def _is_open_polymarket(raw: Mapping[str, object]) -> bool:
    if _optional_bool(raw.get("closed")) is True:
        return False
    if _optional_bool(raw.get("active")) is False:
        return False
    return not _has_resolution_value(raw)


def _is_open_kalshi(raw: Mapping[str, object]) -> bool:
    status = (_optional_str(raw.get("status")) or "").casefold()
    if status in {"closed", "settled", "resolved", "finalized"}:
        return False
    if status and status not in {"open", "active"}:
        return False
    return not _has_resolution_value(raw)


def _has_resolution_value(raw: Mapping[str, object]) -> bool:
    for key in (
        "resolved_outcome",
        "resolvedOutcome",
        "winningOutcome",
        "winner",
        "result",
        "resolution",
        "outcome",
    ):
        value = raw.get(key)
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return True
    return False


def _has_world_cup_marker(raw: Mapping[str, object]) -> bool:
    return any(
        marker in text.casefold()
        for text in _all_text_values(raw)
        for marker in ("world cup", "fifa", "kxwcgame")
    )


def _opponent_from_matchup_texts(raw: Mapping[str, object], team: str) -> str | None:
    team_key = _team_key(team)
    for text in _all_text_values(raw):
        for segment in re.split(r"[:|]", text):
            matchup = re.search(
                r"(?P<left>[A-Za-z][A-Za-z .'-]+?)\s+"
                r"(?:vs\.?|v\.?|versus|against)\s+"
                r"(?P<right>[A-Za-z][A-Za-z .'-]+)",
                segment,
                flags=re.IGNORECASE,
            )
            if matchup is None:
                continue
            left = _clean_team(matchup.group("left"))
            right = _clean_team(matchup.group("right"))
            if _team_key(left) == team_key:
                return right
            if _team_key(right) == team_key:
                return left
    return None


def _match_key(team: str, opponent: str, day: date) -> tuple[str, str, tuple[str, str]]:
    team_key = _team_key(team)
    opponent_key = _team_key(opponent)
    matchup = sorted((team_key, opponent_key))
    return day.isoformat(), team_key, (matchup[0], matchup[1])


def _canonical_metadata(team: str, opponent: str, day: date) -> _CanonicalMetadata:
    day_text = day.isoformat()
    title = f"Will {team} win its FIFA World Cup match against {opponent} on {day_text}?"
    yes_payout = _canonical_yes_payout(team, opponent, day)
    return _CanonicalMetadata(
        title=title,
        rules=f"Resolves yes if {yes_payout}",
        close_time=day_text,
    )


def _canonical_yes_payout(team: str, opponent: str, day: date) -> str:
    return f"{team} wins its FIFA World Cup match against {opponent} on {day.isoformat()}."


def _date_from_kalshi_ticker(ticker: str) -> date | None:
    match = re.search(
        rf"{WORLD_CUP_SERIES_TICKER}-(?P<year>\d{{2}})(?P<month>[A-Z]{{3}})(?P<day>\d{{1,2}})",
        ticker,
    )
    if match is None:
        return None
    month = _MONTHS.get(match.group("month").casefold())
    if month is None:
        return None
    return date(2000 + int(match.group("year")), month, int(match.group("day")))


def _date_from_market(raw: Mapping[str, object]) -> date | None:
    for key in (
        "expiration_time",
        "close_time",
        "expected_expiration_time",
        "endDateIso",
        "endDate",
        "resolutionTime",
    ):
        value = _optional_str(raw.get(key))
        parsed = _parse_date(value, default_year=None) if value is not None else None
        if parsed is not None:
            return parsed
    return None


def _year_from_market(raw: Mapping[str, object]) -> int | None:
    parsed = _date_from_market(raw)
    return None if parsed is None else parsed.year


def _parse_date(value: str | None, *, default_year: int | None) -> date | None:
    if value is None:
        return None
    text = value.strip()
    iso = re.search(r"(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})", text)
    if iso is not None:
        return date(int(iso.group("year")), int(iso.group("month")), int(iso.group("day")))
    month_match = re.search(
        r"(?P<month>Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|"
        r"Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)"
        r"\.?\s+(?P<day>\d{1,2})(?:,\s*(?P<year>\d{4}))?",
        text,
        flags=re.IGNORECASE,
    )
    if month_match is None:
        return None
    year = int(month_match.group("year") or default_year or 0)
    if year == 0:
        return None
    month = _MONTHS[month_match.group("month")[:3].casefold()]
    return date(year, month, int(month_match.group("day")))


def _all_text_values(raw: Mapping[str, object]) -> tuple[str, ...]:
    values: list[str] = []
    for key in (
        "question",
        "title",
        "subtitle",
        "eventTitle",
        "event_title",
        "groupItemTitle",
        "description",
        "rules",
        "slug",
        "eventSlug",
        "series_ticker",
        "ticker",
    ):
        value = raw.get(key)
        if isinstance(value, str):
            values.append(value)
    return tuple(values)


def _json_string_list(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        decoded = json.loads(value)
        if not isinstance(decoded, list):
            return []
        return [str(item) for item in decoded]
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        return [str(item) for item in value]
    return []


def _first_present_str(raw: Mapping[str, object], *keys: str) -> str:
    value = _first_optional_str(raw, *keys)
    if value is None:
        raise KeyError(keys[0])
    return value


def _first_optional_str(raw: Mapping[str, object], *keys: str) -> str | None:
    for key in keys:
        value = _optional_str(raw.get(key))
        if value is not None:
            return value
    return None


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, int):
        return str(value)
    return None


def _optional_bool(value: object) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.casefold() == "true"
    return None


def _clean_team(value: str) -> str:
    cleaned = re.sub(r"\s+", " ", value.strip().strip(" .,-")).strip()
    return cleaned


def _team_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", _clean_team(value).casefold())


_MONTHS = cast(
    dict[str, int],
    {
        "jan": 1,
        "january": 1,
        "feb": 2,
        "february": 2,
        "mar": 3,
        "march": 3,
        "apr": 4,
        "april": 4,
        "may": 5,
        "jun": 6,
        "june": 6,
        "jul": 7,
        "july": 7,
        "aug": 8,
        "august": 8,
        "sep": 9,
        "september": 9,
        "oct": 10,
        "october": 10,
        "nov": 11,
        "november": 11,
        "dec": 12,
        "december": 12,
    },
)
