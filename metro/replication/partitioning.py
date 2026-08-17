"""Helpers de partição Hive por coluna de data."""

from __future__ import annotations

from datetime import date, timedelta

import polars as pl

_PART_YEAR = "_metro_part_year"
_PART_MONTH = "_metro_part_month"
_PART_DAY = "_metro_part_day"
_HELPER_COLUMNS = (_PART_YEAR, _PART_MONTH, _PART_DAY)


def truncate(value: date, granularity: str) -> date:
    """Trunca a data para o início do período (`year`/`month`/`day`)."""
    if granularity == "year":
        return date(value.year, 1, 1)
    if granularity == "month":
        return date(value.year, value.month, 1)
    if granularity == "day":
        return value
    raise ValueError(f"granularity inválida: {granularity}")


def subtract(value: date, granularity: str, periods: int) -> date:
    """Recua `periods` unidades de `granularity` a partir de `value`."""
    if periods < 0:
        raise ValueError("periods deve ser >= 0")
    if granularity == "year":
        return date(value.year - periods, 1, 1)
    if granularity == "day":
        return value - timedelta(days=periods)
    if granularity == "month":
        total = value.year * 12 + (value.month - 1) - periods
        year, month_index = divmod(total, 12)
        return date(year, month_index + 1, 1)
    raise ValueError(f"granularity inválida: {granularity}")


def add(value: date, granularity: str, periods: int) -> date:
    """Avança `periods` unidades de `granularity` a partir de `value`."""
    if periods < 0:
        raise ValueError("periods deve ser >= 0")
    if granularity == "year":
        return date(value.year + periods, 1, 1)
    if granularity == "day":
        return value + timedelta(days=periods)
    if granularity == "month":
        total = value.year * 12 + (value.month - 1) + periods
        year, month_index = divmod(total, 12)
        return date(year, month_index + 1, 1)
    raise ValueError(f"granularity inválida: {granularity}")


def window_cutoff(today: date, granularity: str, lookback_periods: int) -> date:
    """Calcula o limite inferior inclusivo da janela de lookback."""
    truncated = truncate(today, granularity)
    return subtract(truncated, granularity, lookback_periods - 1)


def window_partitions(
    today: date,
    granularity: str,
    lookback_periods: int,
) -> list[str]:
    """Lista paths Hive relativos da janela de lookback (do cutoff até hoje)."""
    current = window_cutoff(today, granularity, lookback_periods)
    end = truncate(today, granularity)
    partitions: list[str] = []
    while current <= end:
        partitions.append(partition_relpath(current, granularity))
        current = add(current, granularity, 1)
    return partitions


def partition_relpath(value: date, granularity: str) -> str:
    """Monta o path relativo Hive (`year=…[/month=…][/day=…]`)."""
    if granularity == "year":
        return f"year={value.year:04d}"
    if granularity == "month":
        return f"year={value.year:04d}/month={value.month:02d}"
    if granularity == "day":
        return (
            f"year={value.year:04d}/month={value.month:02d}/day={value.day:02d}"
        )
    raise ValueError(f"granularity inválida: {granularity}")


def split_by_partition(
    dataframe: pl.DataFrame,
    reference_column: str,
    granularity: str,
) -> list[tuple[str, pl.DataFrame]]:
    """Divide o DataFrame em partições Hive pela coluna de referência temporal."""
    filtered = dataframe.filter(pl.col(reference_column).is_not_null())
    if filtered.height == 0:
        return []

    expressions = [
        pl.col(reference_column).dt.year().alias(_PART_YEAR),
    ]
    group_cols = [_PART_YEAR]
    if granularity in {"month", "day"}:
        expressions.append(pl.col(reference_column).dt.month().alias(_PART_MONTH))
        group_cols.append(_PART_MONTH)
    if granularity == "day":
        expressions.append(pl.col(reference_column).dt.day().alias(_PART_DAY))
        group_cols.append(_PART_DAY)

    enriched = filtered.with_columns(expressions)
    parts: list[tuple[str, pl.DataFrame]] = []
    for group in enriched.partition_by(group_cols, maintain_order=True):
        first = group.row(0, named=True)
        partition_date = date(
            int(first[_PART_YEAR]),
            int(first.get(_PART_MONTH, 1)),
            int(first.get(_PART_DAY, 1)),
        )
        path = partition_relpath(partition_date, granularity)
        cleaned = group.drop([col for col in _HELPER_COLUMNS if col in group.columns])
        parts.append((path, cleaned))
    return parts
