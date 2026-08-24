"""Reusable frequency classification and filtering for Kalshi series."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import numpy as np
import pandas as pd


SHORT_RECC = frozenset({"weekly", "daily", "hourly", "fifteen_min"})
LONG_RECC = frozenset({"annual", "monthly", "quarterly"})
GROUP_ORDER = ("short_recc", "long_recc", "rest")
GROUP_COLORS = {
    "short_recc": "#4C78A8",
    "long_recc": "#59A14F",
    "rest": "#F28E2B",
}


@dataclass(frozen=True)
class SeriesFilter:
    """Criteria for constructing a research series universe."""

    min_volume: float = 0.0
    exclude_groups: tuple[str, ...] = ()
    exclude_categories: tuple[str, ...] = ()
    exclude_frequencies: tuple[str, ...] = ()


def _as_values(values: str | Iterable[str] | None) -> list[str] | None:
    if values is None:
        return None
    if isinstance(values, str):
        return [values]
    return list(values)


def add_frequency_group(data: pd.DataFrame) -> pd.DataFrame:
    """Add the operational group while preserving the raw ``frequency`` field."""

    result = data.copy()
    result["frequency_group"] = np.select(
        [result["frequency"].isin(SHORT_RECC), result["frequency"].isin(LONG_RECC)],
        ["short_recc", "long_recc"],
        default="rest",
    )
    return result


def select_series(
    data: pd.DataFrame,
    min_volume: float = 0,
    exclude_groups: str | Iterable[str] | None = None,
    exclude_categories: str | Iterable[str] | None = None,
    exclude_frequencies: str | Iterable[str] | None = None,
) -> pd.DataFrame:
    """Return series passing the volume floor and optional exclusions.

    The volume boundary is inclusive: ``volume_fp >= min_volume``.
    """

    if min_volume is None or min_volume < 0:
        raise ValueError("min_volume must be non-negative")

    result = add_frequency_group(data)
    mask = result["volume_fp"] >= float(min_volume)

    values = _as_values(exclude_groups)
    if values is not None:
        mask &= ~result["frequency_group"].isin(values)

    values = _as_values(exclude_categories)
    if values is not None:
        mask &= ~result["category"].isin(values)

    values = _as_values(exclude_frequencies)
    if values is not None:
        mask &= ~result["frequency"].isin(values)

    return result.loc[mask].copy()

