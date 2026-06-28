"""Helpers for normalizing Python rows before Parquet schema casts."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from decimal import ROUND_HALF_UP, Decimal, localcontext

import polars as pl


def normalize_rows_for_schema(
    rows: Iterable[Mapping[str, object]],
    schema: pl.Schema,
) -> list[dict[str, object]]:
    """Return rows with Decimal values clamped/quantized to schema decimal columns."""
    return [normalize_row_for_schema(row, schema) for row in rows]


def normalize_row_for_schema(
    row: Mapping[str, object],
    schema: pl.Schema,
) -> dict[str, object]:
    """Normalize Decimal values to their target Parquet schema precision and scale."""
    normalized = dict(row)
    for column, dtype in schema.items():
        if not isinstance(dtype, pl.Decimal):
            continue
        value = normalized.get(column)
        if value is None:
            continue
        if isinstance(value, Decimal):
            normalized[column] = normalize_decimal_for_dtype(value, dtype)
    return normalized


def normalize_decimal_for_dtype(value: Decimal, dtype: pl.Decimal) -> Decimal:
    """Clamp and quantize one Decimal for a Polars Decimal dtype."""
    precision = dtype.precision
    scale = dtype.scale
    if precision is None or scale is None:
        return value
    quantum = Decimal(1).scaleb(-scale)
    minimum, maximum = _decimal_bounds(precision, scale)
    if value.is_nan():
        clamped = Decimal("0")
    elif value.is_infinite():
        clamped = maximum if value > 0 else minimum
    else:
        clamped = min(max(value, minimum), maximum)
    with localcontext() as context:
        context.prec = max(precision + 8, len(clamped.as_tuple().digits) + 8)
        return clamped.quantize(quantum, rounding=ROUND_HALF_UP)


def _decimal_bounds(precision: int, scale: int) -> tuple[Decimal, Decimal]:
    integer_digits = precision - scale
    integral = "9" * integer_digits if integer_digits > 0 else "0"
    fractional = "9" * scale
    maximum = Decimal(f"{integral}.{fractional}" if scale else integral)
    return -maximum, maximum
