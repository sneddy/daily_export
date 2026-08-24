"""Build immutable, reproducible selection manifests from a Kalshi series run."""

from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime
import csv
import hashlib
import io
import json
from pathlib import Path
import sqlite3
from typing import Any, Iterable

from .schema import ensure_schema
from .series_selection import LONG_RECC, SHORT_RECC


DEFAULT_MIN_VOLUME = 10_000.0
DEFAULT_EXCLUDED_FREQUENCY_GROUPS = ("short_recc",)
DEFAULT_SPORT_CATEGORY = "Sports"
DEFAULT_CRYPTO_CATEGORY = "Crypto"
MAIN_GROUP = "non_sport_crypto"
MANIFEST_GROUPS = (MAIN_GROUP, "sport", "crypto")

_MEMBER_COLUMNS = (
    "series_ticker",
    "title",
    "category",
    "category_norm",
    "frequency",
    "frequency_group",
    "volume_fp",
    "volume_known",
    "eligible",
    "selection_group",
    "exclusion_reason",
)


def _now_utc() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _text(value: Any) -> str | None:
    if value is None:
        return None
    result = str(value).strip()
    return result or None


def _normalized(value: Any) -> str:
    return (_text(value) or "").casefold()


def _float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _frequency_group(frequency: Any) -> str:
    frequency_norm = _normalized(frequency)
    if frequency_norm in SHORT_RECC:
        return "short_recc"
    if frequency_norm in LONG_RECC:
        return "long_recc"
    return "rest"


def _member_for_payload(
    payload: dict[str, Any],
    *,
    entity_key: str,
    min_volume: float,
    excluded_frequency_groups: set[str],
    sport_category_norm: str,
    crypto_category_norm: str,
) -> dict[str, Any]:
    ticker = _text(payload.get("ticker")) or entity_key
    category = _text(payload.get("category"))
    category_norm = _normalized(category)
    frequency = _text(payload.get("frequency"))
    frequency_group = _frequency_group(frequency)
    volume_fp = _float(payload.get("volume_fp"))
    volume_known = volume_fp is not None

    exclusion_reason: str | None = None
    if not volume_known:
        exclusion_reason = "volume_unknown"
    elif volume_fp < min_volume:
        exclusion_reason = "below_min_volume"
    elif frequency_group in excluded_frequency_groups:
        exclusion_reason = f"excluded_frequency_group:{frequency_group}"

    eligible = exclusion_reason is None
    if eligible and category_norm == sport_category_norm:
        selection_group = "sport"
    elif eligible and category_norm == crypto_category_norm:
        selection_group = "crypto"
    elif eligible:
        selection_group = MAIN_GROUP
    else:
        selection_group = None

    return {
        "series_ticker": ticker,
        "title": _text(payload.get("title")),
        "category": category,
        "category_norm": category_norm,
        "frequency": frequency,
        "frequency_group": frequency_group,
        "volume_fp": volume_fp,
        "volume_known": int(volume_known),
        "eligible": int(eligible),
        "selection_group": selection_group,
        "exclusion_reason": exclusion_reason,
    }


def _canonical_membership(rows: Iterable[dict[str, Any]]) -> str:
    return "".join(f"{_json_text(row)}\n" for row in rows)


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=_MEMBER_COLUMNS, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    content = buffer.getvalue()
    path.write_text(content, encoding="utf-8")
    return _sha256_text(content)


def _series_run(conn: sqlite3.Connection, series_run_id: str | None) -> sqlite3.Row:
    if series_run_id is None:
        row = conn.execute(
            """
            SELECT run_id, started_at_utc, finished_at_utc, status
            FROM metadata_runs
            WHERE source_mode = 'series' AND status = 'success'
            ORDER BY finished_at_utc DESC, started_at_utc DESC
            LIMIT 1
            """
        ).fetchone()
    else:
        row = conn.execute(
            """
            SELECT run_id, started_at_utc, finished_at_utc, status
            FROM metadata_runs
            WHERE run_id = ?
            """,
            (series_run_id,),
        ).fetchone()
    if row is None:
        requested = series_run_id or "the latest successful series run"
        raise ValueError(f"Could not find {requested}")
    if row[3] != "success":
        raise ValueError(f"Series run {row[0]} is not successful: {row[3]}")
    return row


def build_series_manifest(
    conn: sqlite3.Connection,
    *,
    series_run_id: str | None = None,
    min_volume: float = DEFAULT_MIN_VOLUME,
    excluded_frequency_groups: Iterable[str] = DEFAULT_EXCLUDED_FREQUENCY_GROUPS,
    sport_category: str = DEFAULT_SPORT_CATEGORY,
    crypto_category: str = DEFAULT_CRYPTO_CATEGORY,
    output_path: Path | None = None,
) -> dict[str, Any]:
    """Materialize a deterministic series selection and persist it in SQLite and CSV."""

    ensure_schema(conn)
    if min_volume < 0:
        raise ValueError("min_volume must be non-negative")

    excluded_groups = tuple(sorted({_normalized(value) for value in excluded_frequency_groups}))
    excluded_group_set = set(excluded_groups)
    sport_category_norm = _normalized(sport_category)
    crypto_category_norm = _normalized(crypto_category)
    if sport_category_norm == crypto_category_norm:
        raise ValueError("sport_category and crypto_category must be different")

    source_run = _series_run(conn, series_run_id)
    resolved_series_run_id = str(source_run[0])
    source_rows = conn.execute(
        """
        SELECT entity_key, payload_json
        FROM raw_payloads
        WHERE run_id = ? AND entity_type = 'series' AND source_endpoint = '/series'
        ORDER BY entity_key
        """,
        (resolved_series_run_id,),
    ).fetchall()
    if not source_rows:
        raise ValueError(f"Series run {resolved_series_run_id} contains no /series payloads")

    rule = {
        "schema_version": 1,
        "series_run_id": resolved_series_run_id,
        "min_volume": float(min_volume),
        "unknown_volume_policy": "exclude",
        "excluded_frequency_groups": list(excluded_groups),
        "frequency_group_sets": {
            "short_recc": sorted(SHORT_RECC),
            "long_recc": sorted(LONG_RECC),
            "rest": "all other frequencies",
        },
        "category_normalization": "strip.casefold",
        "category_groups": {
            "sport": [sport_category_norm],
            "crypto": [crypto_category_norm],
            MAIN_GROUP: "all other categories",
        },
    }
    rule_json = _json_text(rule)
    rule_sha256 = _sha256_text(rule_json)
    selection_id = f"{resolved_series_run_id}-{rule_sha256[:12]}"

    members: list[dict[str, Any]] = []
    for entity_key, payload_json in source_rows:
        payload = json.loads(str(payload_json))
        if not isinstance(payload, dict):
            raise ValueError(f"Invalid series payload for {entity_key}")
        members.append(
            _member_for_payload(
                payload,
                entity_key=str(entity_key),
                min_volume=min_volume,
                excluded_frequency_groups=excluded_group_set,
                sport_category_norm=sport_category_norm,
                crypto_category_norm=crypto_category_norm,
            )
        )
    members.sort(key=lambda row: row["series_ticker"])
    membership_sha256 = _sha256_text(_canonical_membership(members))

    if output_path is None:
        output_path = Path("manifests") / f"series_selection_{selection_id}.csv"
    output_path = Path(output_path)
    file_sha256 = _write_csv(output_path, members)

    eligible_members = [row for row in members if row["eligible"]]
    group_counts = Counter(row["selection_group"] for row in eligible_members)
    exclusion_counts = Counter(row["exclusion_reason"] for row in members if row["exclusion_reason"])
    stats = {
        "series_seen": len(members),
        "series_eligible": len(eligible_members),
        "group_counts": {group: int(group_counts.get(group, 0)) for group in MANIFEST_GROUPS},
        "exclusion_counts": dict(sorted(exclusion_counts.items())),
        "manifest_path": str(output_path),
        "manifest_file_sha256": file_sha256,
    }
    stats_json = _json_text(stats)

    with conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO series_selection_runs
                (selection_id, created_at_utc, series_run_id, series_started_at_utc,
                 series_finished_at_utc, rule_json, rule_sha256, membership_sha256,
                 status, stats_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'success', ?)
            ON CONFLICT(selection_id) DO UPDATE SET
                status = excluded.status,
                stats_json = excluded.stats_json
            """,
            (
                selection_id,
                _now_utc(),
                resolved_series_run_id,
                source_run[1],
                source_run[2],
                rule_json,
                rule_sha256,
                membership_sha256,
                stats_json,
            ),
        )
        existing = conn.execute(
            "SELECT membership_sha256 FROM series_selection_runs WHERE selection_id = ?",
            (selection_id,),
        ).fetchone()
        if existing is None or existing[0] != membership_sha256:
            raise ValueError(f"Selection {selection_id} already exists with different membership")
        conn.executemany(
            """
            INSERT OR IGNORE INTO series_selection_members
                (selection_id, series_ticker, title, category, category_norm, frequency,
                 frequency_group, volume_fp, volume_known, eligible, selection_group,
                 exclusion_reason)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    selection_id,
                    row["series_ticker"],
                    row["title"],
                    row["category"],
                    row["category_norm"],
                    row["frequency"],
                    row["frequency_group"],
                    row["volume_fp"],
                    row["volume_known"],
                    row["eligible"],
                    row["selection_group"],
                    row["exclusion_reason"],
                )
                for row in members
            ],
        )

    return {
        "selection_id": selection_id,
        "series_run_id": resolved_series_run_id,
        "series_started_at_utc": source_run[1],
        "series_finished_at_utc": source_run[2],
        "rule_sha256": rule_sha256,
        "membership_sha256": membership_sha256,
        "manifest_path": str(output_path),
        **stats,
    }
