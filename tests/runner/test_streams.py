from decimal import ROUND_HALF_UP, Decimal, localcontext

import polars as pl

from pmbot.runner.models import ResolutionRecord, SignalRecord
from pmbot.runner.streams import ResolutionParquetStore, SignalParquetWriter
from pmbot.signals.cross_venue import guarded_log_odds_pool


def test_signal_writer_quantizes_extreme_precision_logit_values(tmp_path) -> None:
    pm_probability = Decimal("0.1234567890123456789012345678")
    kalshi_probability = Decimal("0.8123456789012345678901234567")
    p_hat = guarded_log_odds_pool(
        polymarket_probability=pm_probability,
        kalshi_probability=kalshi_probability,
        kalshi_weight=Decimal("1"),
        double_counting_overlap=Decimal("0.50"),
    )
    signal_value = p_hat - pm_probability
    confidence = abs(signal_value) / Decimal("3")
    assert p_hat.as_tuple().exponent < -12
    assert signal_value.as_tuple().exponent < -12
    assert confidence.as_tuple().exponent < -12

    writer = SignalParquetWriter(
        data_dir=tmp_path,
        buffer_size=100,
        flush_interval_seconds=999,
    )
    writer.record(
        SignalRecord(
            ts_local_ns=1,
            polymarket_condition_id="pm-condition",
            polymarket_asset_id="pm-yes",
            kalshi_ticker="KXTEST",
            p_hat=p_hat,
            polymarket_probability=pm_probability,
            kalshi_probability=kalshi_probability,
            signal_value=signal_value,
            direction="buy",
            confidence=confidence,
            rationale="fixture",
        )
    )
    writer.flush_all()

    frame = pl.read_parquet(list(tmp_path.glob("signals/date=*/market=pm-condition/*.parquet")))

    assert frame.schema["p_hat"] == pl.Decimal(38, 12)
    assert frame.schema["signal_value"] == pl.Decimal(38, 12)
    assert frame.schema["confidence"] == pl.Decimal(38, 12)
    assert frame["p_hat"].to_list() == [_quantize_12(p_hat)]
    assert frame["signal_value"].to_list() == [_quantize_12(signal_value)]
    assert frame["confidence"].to_list() == [_quantize_12(confidence)]


def test_resolution_writer_uses_shared_schema_normalization_path(tmp_path) -> None:
    store = ResolutionParquetStore(data_dir=tmp_path)

    assert (
        store.record_many(
            (
                ResolutionRecord(
                    venue="polymarket",
                    market_id="pm-condition",
                    resolved_outcome="yes",
                    resolved_at="2026-06-28T00:00:00Z",
                    fetched_at="2026-06-28T00:01:00Z",
                    raw_json='{"fixture":true}',
                ),
            )
        )
        == 1
    )

    frame = pl.read_parquet(
        list(tmp_path.glob("resolutions/date=*/market=pm-condition/*.parquet"))
    )

    assert frame.schema == {
        "venue": pl.Utf8,
        "market_id": pl.Utf8,
        "resolved_outcome": pl.Utf8,
        "resolved_at": pl.Utf8,
        "fetched_at": pl.Utf8,
        "raw_json": pl.Utf8,
    }


def _quantize_12(value: Decimal) -> Decimal:
    with localcontext() as context:
        context.prec = 50
        return value.quantize(Decimal("0.000000000001"), rounding=ROUND_HALF_UP)
