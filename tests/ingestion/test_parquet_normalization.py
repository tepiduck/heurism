from decimal import Decimal

import polars as pl

from pmbot.ingestion.parquet_normalization import normalize_rows_for_schema


def test_large_negative_decimal_clamps_to_exact_decimal128_minimum() -> None:
    schema = pl.Schema({"value": pl.Decimal(38, 12)})

    rows = normalize_rows_for_schema([{"value": Decimal("-1e40")}], schema)
    frame = pl.DataFrame(rows).cast(schema)

    assert frame["value"].to_list() == [
        Decimal("-99999999999999999999999999.999999999999")
    ]
