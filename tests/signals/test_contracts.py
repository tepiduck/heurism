import json

from pmbot.signals.contracts import (
    ContractMapper,
    ContractMetadata,
    ManualAllowlistEntry,
    RejectedContractPair,
    VerifiedContractPair,
    save_verified_pairs,
)

YES_PAYOUT = "Example wins the 2028 US presidential election"
NO_PAYOUT = "Example does not win the 2028 US presidential election"


def test_contract_mapper_verifies_known_identical_pair_and_persists_rationale(tmp_path) -> None:
    mapper = ContractMapper()

    pair = mapper.verify_pair(
        polymarket=ContractMetadata(
            venue="polymarket",
            identifier="pm-condition-1",
            title="Will Example win the 2028 presidential election?",
            rules="Resolves yes if Example wins the 2028 US presidential election.",
            close_time="2028-11-08T00:00:00Z",
            yes_payout_condition=YES_PAYOUT,
        ),
        kalshi=ContractMetadata(
            venue="kalshi",
            identifier="KXEXAMPLE-28-PRES",
            title="Will Example win the 2028 presidential election?",
            rules="Resolves yes if Example wins the 2028 US presidential election.",
            close_time="2028-11-08T00:00:00Z",
            yes_payout_condition=YES_PAYOUT,
        ),
        polymarket_asset_id="pm-yes-token",
    )

    assert isinstance(pair, VerifiedContractPair)
    assert pair.polymarket_asset_id == "pm-yes-token"
    assert pair.invert_kalshi is False
    assert "exact title" in pair.rationale

    output_path = tmp_path / "pairs.jsonl"
    save_verified_pairs(output_path, [pair])
    persisted = json.loads(output_path.read_text(encoding="utf-8"))
    assert persisted["kalshi_ticker"] == "KXEXAMPLE-28-PRES"
    assert persisted["invert_kalshi"] is False
    assert persisted["rationale"] == pair.rationale


def test_contract_mapper_excludes_known_mismatch() -> None:
    mapper = ContractMapper()

    result = mapper.verify_pair(
        polymarket=ContractMetadata(
            venue="polymarket",
            identifier="pm-condition-1",
            title="Will Example win the popular vote?",
            rules="Resolves yes if Example wins the national popular vote.",
            close_time="2028-11-08T00:00:00Z",
            yes_payout_condition="Example wins the national popular vote",
        ),
        kalshi=ContractMetadata(
            venue="kalshi",
            identifier="KXEXAMPLE-28-EC",
            title="Will Example win the popular vote?",
            rules="Resolves yes if Example wins the Electoral College.",
            close_time="2028-11-08T00:00:00Z",
            yes_payout_condition="Example wins the Electoral College",
        ),
        polymarket_asset_id="pm-yes-token",
    )

    assert isinstance(result, RejectedContractPair)
    assert result.reason == "resolution rules mismatch"


def test_contract_mapper_rejects_opposite_yes_direction_without_invert() -> None:
    mapper = ContractMapper()

    result = mapper.verify_pair(
        polymarket=ContractMetadata(
            venue="polymarket",
            identifier="pm-condition-1",
            title="Will Example win the 2028 presidential election?",
            rules="Resolves yes if Example wins the 2028 US presidential election.",
            close_time="2028-11-08T00:00:00Z",
            yes_payout_condition=YES_PAYOUT,
        ),
        kalshi=ContractMetadata(
            venue="kalshi",
            identifier="KXEXAMPLE-28-PRES",
            title="Will Example win the 2028 presidential election?",
            rules="Resolves yes if Example wins the 2028 US presidential election.",
            close_time="2028-11-08T00:00:00Z",
            yes_payout_condition=NO_PAYOUT,
        ),
        polymarket_asset_id="pm-yes-token",
    )

    assert isinstance(result, RejectedContractPair)
    assert result.reason == "yes payout condition mismatch"


def test_contract_mapper_manual_allowlist_can_verify_known_pair() -> None:
    mapper = ContractMapper(
        [
            ManualAllowlistEntry(
                polymarket_condition_id="pm-condition-2",
                polymarket_asset_id="pm-yes-token-2",
                kalshi_ticker="KXMANUAL",
                polymarket_yes_payout_condition=YES_PAYOUT,
                kalshi_yes_payout_condition=YES_PAYOUT,
                invert=False,
                rationale="Human reviewed identical exchange rulebooks.",
            )
        ]
    )

    result = mapper.verify_pair(
        polymarket=ContractMetadata(
            venue="polymarket",
            identifier="pm-condition-2",
            title="Different display text",
            rules="Different rule text",
            close_time="2028-11-08T00:00:00Z",
            yes_payout_condition=YES_PAYOUT,
        ),
        kalshi=ContractMetadata(
            venue="kalshi",
            identifier="KXMANUAL",
            title="Manual text",
            rules="Manual rule text",
            close_time="2028-11-09T00:00:00Z",
            yes_payout_condition=YES_PAYOUT,
        ),
        polymarket_asset_id="ignored-by-manual-entry",
    )

    assert isinstance(result, VerifiedContractPair)
    assert result.polymarket_asset_id == "pm-yes-token-2"
    assert result.rationale.startswith("manual allowlist")


def test_contract_mapper_manual_allowlist_invert_verifies_opposite_direction() -> None:
    mapper = ContractMapper(
        [
            ManualAllowlistEntry(
                polymarket_condition_id="pm-condition-3",
                polymarket_asset_id="pm-yes-token-3",
                kalshi_ticker="KXINVERTED",
                polymarket_yes_payout_condition=YES_PAYOUT,
                kalshi_yes_payout_condition=NO_PAYOUT,
                invert=True,
                rationale="Human reviewed that Kalshi YES is the opposite side.",
            )
        ]
    )

    result = mapper.verify_pair(
        polymarket=ContractMetadata(
            venue="polymarket",
            identifier="pm-condition-3",
            title="Different display text",
            rules="Different rule text",
            close_time="2028-11-08T00:00:00Z",
            yes_payout_condition=YES_PAYOUT,
        ),
        kalshi=ContractMetadata(
            venue="kalshi",
            identifier="KXINVERTED",
            title="Manual text",
            rules="Manual rule text",
            close_time="2028-11-09T00:00:00Z",
            yes_payout_condition=NO_PAYOUT,
        ),
        polymarket_asset_id="ignored-by-manual-entry",
    )

    assert isinstance(result, VerifiedContractPair)
    assert result.invert_kalshi is True
    assert "kalshi_inverted=True" in result.rationale
