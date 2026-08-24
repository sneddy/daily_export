"""Small public Kalshi metadata client used by the daily research pipeline."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
import os
import random
import time
from typing import Any
from urllib.parse import quote, urljoin

import requests


_RETRY_STATUS_CODES = {408, 429, 500, 502, 503, 504}


@dataclass(frozen=True)
class MetadataHttpConfig:
    """HTTP settings for public metadata extraction."""

    timeout_seconds: float = 30.0
    max_retries: int = 6
    backoff_base_seconds: float = 1.0
    backoff_max_seconds: float = 60.0
    user_agent: str = "kalshi_daily_research/0.1"

    @classmethod
    def from_env(cls) -> "MetadataHttpConfig":
        def as_float(name: str, default: float) -> float:
            return float(os.getenv(name, default))

        def as_int(name: str, default: int) -> int:
            return int(os.getenv(name, default))

        return cls(
            timeout_seconds=as_float("KALSHI_HTTP_TIMEOUT_SECONDS", cls.timeout_seconds),
            max_retries=as_int("KALSHI_HTTP_MAX_RETRIES", cls.max_retries),
            backoff_base_seconds=as_float("KALSHI_HTTP_BACKOFF_BASE_SECONDS", cls.backoff_base_seconds),
            backoff_max_seconds=as_float("KALSHI_HTTP_BACKOFF_MAX_SECONDS", cls.backoff_max_seconds),
            user_agent=os.getenv("KALSHI_USER_AGENT", cls.user_agent),
        )


class KalshiMetadataClient:
    """Cursor-paginated client for Kalshi series, events, and market metadata."""

    def __init__(
        self,
        *,
        base_url: str | None = None,
        config: MetadataHttpConfig | None = None,
        session: requests.Session | None = None,
    ) -> None:
        self.base_url = (base_url or os.getenv("KALSHI_BASE_URL") or "https://api.elections.kalshi.com/trade-api/v2").rstrip("/") + "/"
        self.config = config or MetadataHttpConfig.from_env()
        self.session = session or requests.Session()
        self.session.headers.setdefault("Accept", "application/json")
        self.session.headers.setdefault("User-Agent", self.config.user_agent)

    def iter_series(
        self,
        *,
        limit: int = 200,
        max_pages: int | None = None,
        include_volume: bool = False,
    ) -> Iterator[dict[str, Any]]:
        yield from self._iter_cursor(
            "series",
            "series",
            limit=limit,
            max_pages=max_pages,
            include_volume=str(bool(include_volume)).lower(),
        )

    def get_series(self, series_ticker: str) -> dict[str, Any]:
        payload = self._get_json(f"series/{series_ticker}", params={"include_volume": "true"})
        return payload if isinstance(payload, dict) else {}

    def iter_events(
        self,
        *,
        limit: int = 200,
        max_pages: int | None = None,
        with_nested_markets: bool = False,
    ) -> Iterator[dict[str, Any]]:
        yield from self._iter_cursor(
            "events",
            "events",
            limit=limit,
            max_pages=max_pages,
            with_nested_markets=str(bool(with_nested_markets)).lower(),
        )

    def iter_markets(
        self,
        *,
        limit: int = 200,
        max_pages: int | None = None,
        **params: Any,
    ) -> Iterator[dict[str, Any]]:
        yield from self._iter_cursor("markets", "markets", limit=limit, max_pages=max_pages, **params)

    def iter_historical_markets(
        self,
        *,
        limit: int = 200,
        max_pages: int | None = None,
        **params: Any,
    ) -> Iterator[dict[str, Any]]:
        yield from self._iter_cursor("historical/markets", "markets", limit=limit, max_pages=max_pages, **params)

    def get_event(self, event_ticker: str) -> dict[str, Any]:
        payload = self._get_json(
            f"events/{event_ticker}",
            params={"with_nested_markets": "false"},
        )
        return payload if isinstance(payload, dict) else {}

    def get_market_candlesticks(
        self,
        *,
        series_ticker: str,
        market_ticker: str,
        start_ts: int,
        end_ts: int,
        period_interval: int = 1440,
        include_latest_before_start: bool = False,
    ) -> dict[str, Any]:
        """Fetch native candlesticks for one live market."""

        payload = self._get_json(
            f"series/{quote(series_ticker, safe='')}/markets/{quote(market_ticker, safe='')}/candlesticks",
            params={
                "start_ts": int(start_ts),
                "end_ts": int(end_ts),
                "period_interval": int(period_interval),
                "include_latest_before_start": str(bool(include_latest_before_start)).lower(),
            },
        )
        return payload if isinstance(payload, dict) else {}

    def get_batch_market_candlesticks(
        self,
        *,
        market_tickers: list[str],
        start_ts: int,
        end_ts: int,
        period_interval: int = 1440,
        include_latest_before_start: bool = False,
    ) -> dict[str, Any]:
        """Fetch native candlesticks for a batch of live markets."""

        if not market_tickers:
            return {"markets": []}
        if len(market_tickers) > 100:
            raise ValueError("The live batch candlestick endpoint accepts at most 100 market tickers")
        payload = self._get_json(
            "markets/candlesticks",
            params={
                "market_tickers": ",".join(market_tickers),
                "start_ts": int(start_ts),
                "end_ts": int(end_ts),
                "period_interval": int(period_interval),
                "include_latest_before_start": str(bool(include_latest_before_start)).lower(),
            },
        )
        return payload if isinstance(payload, dict) else {}

    def get_historical_market_candlesticks(
        self,
        *,
        market_ticker: str,
        start_ts: int,
        end_ts: int,
        period_interval: int = 1440,
    ) -> dict[str, Any]:
        """Fetch native candlesticks for one archived market."""

        payload = self._get_json(
            f"historical/markets/{quote(market_ticker, safe='')}/candlesticks",
            params={
                "start_ts": int(start_ts),
                "end_ts": int(end_ts),
                "period_interval": int(period_interval),
            },
        )
        return payload if isinstance(payload, dict) else {}

    def _iter_cursor(
        self,
        path: str,
        list_key: str,
        *,
        limit: int,
        max_pages: int | None,
        **params: Any,
    ) -> Iterator[dict[str, Any]]:
        cursor: str | None = None
        page_count = 0
        while True:
            query = {key: value for key, value in params.items() if value is not None}
            query["limit"] = int(limit)
            if cursor:
                query["cursor"] = cursor
            payload = self._get_json(path, params=query)
            items = payload.get(list_key) if isinstance(payload, dict) else None
            if not isinstance(items, list) or not items:
                return
            for item in items:
                if isinstance(item, dict):
                    yield item
            page_count += 1
            if max_pages is not None and page_count >= int(max_pages):
                return
            next_cursor = payload.get("cursor") if isinstance(payload, dict) else None
            if not next_cursor or str(next_cursor) == str(cursor):
                return
            cursor = str(next_cursor)

    def _get_json(self, path: str, *, params: dict[str, Any]) -> Any:
        url = urljoin(self.base_url, path)
        last_error: Exception | None = None
        for attempt in range(self.config.max_retries + 1):
            try:
                response = self.session.get(url, params=params, timeout=self.config.timeout_seconds)
                if response.status_code in _RETRY_STATUS_CODES:
                    response.raise_for_status()
                response.raise_for_status()
                return response.json()
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                status_code = getattr(getattr(exc, "response", None), "status_code", None)
                retryable = status_code in _RETRY_STATUS_CODES or status_code is None
                if attempt >= self.config.max_retries or not retryable:
                    raise
                delay = min(
                    self.config.backoff_max_seconds,
                    self.config.backoff_base_seconds * (2**attempt) * (1.0 + random.random()),
                )
                time.sleep(delay)
        raise RuntimeError("Kalshi metadata request failed") from last_error
