from pmbot.ingestion.processor import MarketDataProcessor
from pmbot.ingestion.schemas import GapEvent, GapReason


def test_sequence_jump_emits_gap_event() -> None:
    processor = MarketDataProcessor(verify_hashes=False)
    processor.process_payload(
        {
            "event_type": "book",
            "asset_id": "asset-yes",
            "market": "condition-1",
            "bids": [{"price": "0.48", "size": "30"}],
            "asks": [{"price": "0.52", "size": "25"}],
            "timestamp": "1",
            "sequence": "1",
        },
        ts_local_ns=100,
        ts_monotonic_ns=10,
    )

    events = processor.process_payload(
        {
            "event_type": "book",
            "asset_id": "asset-yes",
            "market": "condition-1",
            "bids": [{"price": "0.49", "size": "20"}],
            "asks": [{"price": "0.53", "size": "10"}],
            "timestamp": "3",
            "sequence": "3",
        },
        ts_local_ns=200,
        ts_monotonic_ns=20,
    )

    gaps = [event for event in events if isinstance(event, GapEvent)]

    assert len(gaps) == 1
    assert gaps[0].reason == GapReason.SEQUENCE_GAP
    assert gaps[0].ts_monotonic_ns == 20
    assert gaps[0].details == "sequence jumped from 1 to 3"


def test_dropped_connection_can_be_marked_as_gap_event() -> None:
    processor = MarketDataProcessor()

    gap = processor.dropped_connection_gap(
        condition_id="condition-1",
        asset_id="asset-yes",
        details="simulated disconnect",
        ts_local_ns=300,
        ts_monotonic_ns=30,
    )

    assert gap.reason == GapReason.CONNECTION_LOST
    assert gap.ts_monotonic_ns == 30
    assert gap.details == "simulated disconnect"


def test_checksum_mismatch_emits_gap_event() -> None:
    processor = MarketDataProcessor(verify_hashes=True)

    events = processor.process_payload(
        {
            "event_type": "book",
            "asset_id": "asset-yes",
            "market": "condition-1",
            "bids": [{"price": "0.48", "size": "30"}],
            "asks": [{"price": "0.52", "size": "25"}],
            "timestamp": "1",
            "hash": "definitely-wrong",
        },
        ts_local_ns=400,
        ts_monotonic_ns=40,
    )

    gaps = [event for event in events if isinstance(event, GapEvent)]

    assert len(gaps) == 1
    assert gaps[0].reason == GapReason.CHECKSUM_MISMATCH
    assert gaps[0].ts_monotonic_ns == 40


def test_delta_without_snapshot_does_not_emit_checksum_mismatch() -> None:
    processor = MarketDataProcessor(verify_hashes=True)

    events = processor.process_payload(
        {
            "event_type": "price_change",
            "market": "condition-1",
            "timestamp": "1",
            "price_changes": [
                {
                    "asset_id": "asset-yes",
                    "price": "0.49",
                    "size": "20",
                    "side": "BUY",
                    "hash": "not-verifiable-yet",
                }
            ],
        },
        ts_local_ns=500,
        ts_monotonic_ns=50,
    )

    gaps = [event for event in events if isinstance(event, GapEvent)]

    assert [gap.reason for gap in gaps] == [GapReason.MISSING_SNAPSHOT]
