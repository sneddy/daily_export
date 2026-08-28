from __future__ import annotations

from kalshi_daily_research.client import KalshiMetadataClient, MetadataHttpConfig


class FakeResponse:
    status_code = 200

    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self):
        self.headers = {}
        self.calls = []

    def get(self, url, *, params, timeout):
        self.calls.append((url, params, timeout))
        return FakeResponse({"ticker": "MARKET-1", "candlesticks": []})


def test_historical_candlestick_client_uses_single_market_endpoint() -> None:
    session = FakeSession()
    client = KalshiMetadataClient(
        base_url="https://example.test/trade-api/v2/",
        config=MetadataHttpConfig(max_retries=0),
        session=session,
    )

    payload = client.get_historical_market_candlesticks(
        market_ticker="MARKET-1",
        start_ts=1,
        end_ts=2,
        period_interval=1440,
    )

    assert payload["ticker"] == "MARKET-1"
    assert session.calls == [
        (
            "https://example.test/trade-api/v2/historical/markets/MARKET-1/candlesticks",
            {"start_ts": 1, "end_ts": 2, "period_interval": 1440},
            30.0,
        )
    ]


def test_historical_cutoff_client_reads_partition_timestamp() -> None:
    session = FakeSession()
    session.get = lambda url, *, params, timeout: FakeResponse(
        {"market_settled_ts": "2025-01-01T00:00:00Z"}
    )
    client = KalshiMetadataClient(
        base_url="https://example.test/trade-api/v2/",
        config=MetadataHttpConfig(max_retries=0),
        session=session,
    )

    assert client.get_historical_cutoff() == {
        "market_settled_ts": "2025-01-01T00:00:00Z"
    }
