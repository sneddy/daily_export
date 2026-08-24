"""Tables and plots for inspecting a Kalshi series universe."""

from __future__ import annotations

import numpy as np
import pandas as pd

from .series_selection import GROUP_COLORS, GROUP_ORDER


def selection_summary(data: pd.DataFrame) -> pd.DataFrame:
    volume = data["volume_fp"]
    return pd.DataFrame(
        {"value": [
            len(data),
            (volume == 0).sum(),
            (volume > 0).sum(),
            volume.sum(),
            volume.median(),
            volume.quantile(0.9),
            volume.max(),
        ]},
        index=[
            "series_count",
            "volume_zero",
            "volume_positive",
            "volume_total",
            "volume_median",
            "volume_p90",
            "volume_max",
        ],
    )


def threshold_table(
    data: pd.DataFrame,
    thresholds: tuple[float, ...] = (0, 100, 1_000, 10_000, 100_000, 1_000_000),
) -> pd.DataFrame:
    total_volume = data["volume_fp"].sum()
    rows = []
    for threshold in thresholds:
        selected = data[data["volume_fp"] >= threshold]
        rows.append(
            {
                "min_volume": threshold,
                "series_count": len(selected),
                "share_of_series_pct": 100 * len(selected) / len(data) if len(data) else 0,
                "share_of_volume_pct": (
                    100 * selected["volume_fp"].sum() / total_volume if total_volume else 0
                ),
            }
        )
    return pd.DataFrame(rows)


def frequency_table(data: pd.DataFrame) -> pd.DataFrame:
    return data["frequency"].value_counts().rename_axis("frequency").to_frame("series_count")


def group_table(data: pd.DataFrame) -> pd.DataFrame:
    result = (
        data["frequency_group"]
        .value_counts()
        .reindex(GROUP_ORDER, fill_value=0)
        .to_frame("series_count")
    )
    result["share_pct"] = 100 * result["series_count"] / len(data) if len(data) else 0
    return result


def category_table(data: pd.DataFrame, top_n: int = 20) -> pd.DataFrame:
    return (
        data["category"]
        .value_counts()
        .head(top_n)
        .rename_axis("category")
        .to_frame("series_count")
    )


def top_volume_table(data: pd.DataFrame, top_n: int = 25) -> pd.DataFrame:
    columns = [
        "series_ticker",
        "title",
        "category",
        "frequency",
        "frequency_group",
        "volume_fp",
    ]
    return data.sort_values("volume_fp", ascending=False)[columns].head(top_n)


def volume_bucket_table(data: pd.DataFrame) -> pd.DataFrame:
    bins = [0, 5_000, 10_000, 20_000, 30_000, 50_000, 80_000, 100_000, 200_000, np.inf]
    labels = [
        "0–<5k",
        "5k–<10k",
        "10k–<20k",
        "20k–<30k",
        "30k–<50k",
        "50k–<80k",
        "80k–<100k",
        "100k–<200k",
        "200k+",
    ]
    buckets = pd.cut(
        data["volume_fp"].clip(lower=0),
        bins=bins,
        labels=labels,
        right=False,
    )
    result = buckets.value_counts(sort=False).rename("series_count").to_frame()
    result["share_pct"] = 100 * result["series_count"] / len(data) if len(data) else 0
    return result


def plot_log_volume(data: pd.DataFrame, min_volume: float = 0, title: str = "Log volume"):
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(9, 4))
    ax.hist(np.log10(data["volume_fp"] + 1), bins=50, color="#F28E2B")
    ax.axvline(
        np.log10(min_volume + 1),
        color="#E15759",
        linestyle="--",
        label=f"min={min_volume:,.0f}",
    )
    ax.set_title(title)
    ax.set_xlabel("log10(1 + volume_fp)")
    ax.set_ylabel("Number of series")
    ax.legend()
    fig.tight_layout()
    return fig


def plot_threshold_counts(
    data: pd.DataFrame,
    thresholds: tuple[float, ...] = (1_000, 5_000, 10_000, 20_000, 100_000),
):
    import matplotlib.pyplot as plt

    table = threshold_table(data, thresholds)
    x = np.arange(len(table))
    y = table["series_count"].to_numpy()
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.plot(x, y, marker="o", markersize=8, linewidth=1.8, color="#4C78A8", zorder=3)
    for position, (threshold, count) in enumerate(zip(table["min_volume"], y)):
        ax.vlines(position, 0, count, color="#4C78A8", linestyle="--", linewidth=1, alpha=.45, zorder=1)
        if position > 0:
            ax.hlines(count, 0, position, color="#4C78A8", linestyle="--", linewidth=1, alpha=.35, zorder=1)
        ax.annotate(
            f"{count:,.0f}",
            (position, count),
            xytext=(0, 9),
            textcoords="offset points",
            ha="center",
            va="bottom",
            fontsize=9,
        )
    ax.set_xticks(x)
    ax.set_xticklabels([f"{value:,.0f}" for value in table["min_volume"]])
    ax.set_title("Series retained by volume threshold")
    ax.set_xlabel("min_volume")
    ax.set_ylabel("Number of series")
    ax.grid(axis="y", linestyle="--", alpha=.35)
    fig.tight_layout()
    return fig


def plot_frequency_counts(data: pd.DataFrame, title: str = "Series by source frequency"):
    import matplotlib.pyplot as plt

    counts = frequency_table(data).sort_values("series_count")
    fig, ax = plt.subplots(figsize=(9, 4))
    ax.barh(counts.index, counts["series_count"], color="#4C78A8")
    ax.set_title(title)
    ax.set_xlabel("Number of series")
    fig.tight_layout()
    return fig


def plot_group_counts(data: pd.DataFrame, title: str = "Series by operational group"):
    import matplotlib.pyplot as plt

    counts = group_table(data)
    fig, ax = plt.subplots(figsize=(7, 4))
    ax.bar(counts.index, counts["series_count"], color=[GROUP_COLORS[x] for x in GROUP_ORDER])
    ax.set_title(title)
    ax.set_ylabel("Number of series")
    fig.tight_layout()
    return fig


def plot_group_volume(data: pd.DataFrame, title: str = "Volume by operational group"):
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(8, 4))
    log_volume = np.log10(data["volume_fp"] + 1)
    for group in GROUP_ORDER:
        values = log_volume[data["frequency_group"] == group]
        if len(values):
            ax.hist(values, bins=35, alpha=.55, label=group, color=GROUP_COLORS[group])
    ax.set_title(title)
    ax.set_xlabel("log10(1 + volume_fp)")
    ax.set_ylabel("Number of series")
    ax.legend()
    fig.tight_layout()
    return fig


def plot_category_counts(data: pd.DataFrame, top_n: int = 20, title: str = "Top categories"):
    import matplotlib.pyplot as plt

    counts = category_table(data, top_n).sort_values("series_count")
    fig, ax = plt.subplots(figsize=(9, 6))
    ax.barh(counts.index, counts["series_count"], color="#76B7B2")
    ax.set_title(title)
    ax.set_xlabel("Number of series")
    fig.tight_layout()
    return fig

