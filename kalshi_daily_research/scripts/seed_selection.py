"""Seed a separate SQLite database with one frozen series selection."""

from __future__ import annotations

import argparse
from pathlib import Path
import sqlite3
from typing import Any

from kalshi_daily_research.schema import ensure_schema


_TABLES_TO_COPY = (
    "metadata_runs",
    "raw_payloads",
    "raw_series",
    "series_selection_runs",
    "series_selection_members",
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source-db-path",
        type=Path,
        required=True,
        help="Existing SQLite database containing the frozen selection.",
    )
    parser.add_argument(
        "--target-db-path",
        type=Path,
        required=True,
        help="New or previously seeded historical SQLite database.",
    )
    parser.add_argument("--selection-id", required=True, help="Frozen series selection to copy.")
    return parser


def _table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    return [str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")]


def _copy_rows(
    source: sqlite3.Connection,
    target: sqlite3.Connection,
    *,
    table: str,
    where: str,
    params: tuple[Any, ...],
) -> int:
    columns = _table_columns(source, table)
    rows = source.execute(
        f"SELECT {', '.join(columns)} FROM {table} WHERE {where}",
        params,
    ).fetchall()
    if not rows:
        return 0
    placeholders = ", ".join("?" for _ in columns)
    target.executemany(
        f"INSERT OR IGNORE INTO {table} ({', '.join(columns)}) VALUES ({placeholders})",
        rows,
    )
    return len(rows)


def seed_selection(
    *,
    source_db_path: Path,
    target_db_path: Path,
    selection_id: str,
) -> dict[str, Any]:
    source_path = source_db_path.expanduser().resolve()
    target_path = target_db_path.expanduser().resolve()
    if source_path == target_path:
        raise ValueError("source-db-path and target-db-path must be different")
    if not source_path.exists():
        raise FileNotFoundError(source_path)

    target_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(source_path) as source, sqlite3.connect(target_path) as target:
        source.row_factory = sqlite3.Row
        ensure_schema(target)
        live_run = target.execute(
            "SELECT run_id FROM metadata_runs WHERE source_mode = 'live' LIMIT 1"
        ).fetchone()
        if live_run is not None:
            raise ValueError(
                "Target database already contains live runs; use a separate empty "
                "historical database to keep the streams isolated"
            )
        selection = source.execute(
            """
            SELECT selection_id, series_run_id, membership_sha256
            FROM series_selection_runs
            WHERE selection_id = ?
            """,
            (selection_id,),
        ).fetchone()
        if selection is None:
            raise ValueError(f"Could not find selection {selection_id!r} in {source_path}")

        existing = target.execute(
            "SELECT membership_sha256 FROM series_selection_runs WHERE selection_id = ?",
            (selection_id,),
        ).fetchone()
        if existing is not None:
            if str(existing[0]) != str(selection["membership_sha256"]):
                raise ValueError(
                    f"Target already contains selection {selection_id!r} with different membership"
                )
            return {
                "selection_id": selection_id,
                "target_db_path": str(target_path),
                "already_seeded": True,
                "rows_copied": {},
            }

        series_run_id = str(selection["series_run_id"])
        series_run = source.execute(
            "SELECT run_id FROM metadata_runs WHERE run_id = ?",
            (series_run_id,),
        ).fetchone()
        if series_run is None:
            raise ValueError(f"Selection {selection_id!r} references missing series run {series_run_id!r}")

        rows_copied: dict[str, int] = {}
        with target:
            rows_copied["metadata_runs"] = _copy_rows(
                source,
                target,
                table="metadata_runs",
                where="run_id = ?",
                params=(series_run_id,),
            )
            rows_copied["raw_payloads"] = _copy_rows(
                source,
                target,
                table="raw_payloads",
                where="run_id = ? AND entity_type = 'series'",
                params=(series_run_id,),
            )
            rows_copied["raw_series"] = _copy_rows(
                source,
                target,
                table="raw_series",
                where="run_id = ?",
                params=(series_run_id,),
            )
            rows_copied["series_selection_runs"] = _copy_rows(
                source,
                target,
                table="series_selection_runs",
                where="selection_id = ?",
                params=(selection_id,),
            )
            rows_copied["series_selection_members"] = _copy_rows(
                source,
                target,
                table="series_selection_members",
                where="selection_id = ?",
                params=(selection_id,),
            )

        return {
            "selection_id": selection_id,
            "series_run_id": series_run_id,
            "target_db_path": str(target_path),
            "already_seeded": False,
            "rows_copied": rows_copied,
        }


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    print(
        seed_selection(
            source_db_path=args.source_db_path,
            target_db_path=args.target_db_path,
            selection_id=args.selection_id,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
