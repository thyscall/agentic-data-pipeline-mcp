"""
Tests for web_analytics_flow.py
================================

Run only unit tests (fast, no external services):
    pytest tests/test_web_analytics_flow.py -v -m "not integration"

Run integration tests (requires live API + Snowflake credentials):
    pytest tests/test_web_analytics_flow.py -v -m integration

Required environment variables for integration tests:
    API_BASE_URL, SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
    SNOWFLAKE_DATABASE, SNOWFLAKE_WAREHOUSE
"""

import logging
import os
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import requests

# ---------------------------------------------------------------------------
# Path setup — add prefect/ to sys.path so `flows.web_analytics_flow` is
# importable when tests are run from the prefect/ directory.
# ---------------------------------------------------------------------------

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from flows.web_analytics_flow import (  # noqa: E402
    DEFAULT_LOOKBACK_MINUTES,
    STAGE_NAME,
    TARGET_TABLE,
    clean_and_validate,
    extract_max_timestamp,
    fetch_clickstream_events,
    fetch_with_retry,
    get_since_parameter,
    stage_and_load,
    web_analytics_flow,
)

# ---------------------------------------------------------------------------
# Module-level logger used by the mock — replaces prefect's get_run_logger
# ---------------------------------------------------------------------------

_TEST_LOGGER = logging.getLogger("test.web_analytics_flow")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def mock_prefect_logger():
    """
    Patch get_run_logger in the flow module so all tasks can be called via
    their .fn attribute outside of a live Prefect run context.
    """
    with patch(
        "flows.web_analytics_flow.get_run_logger", return_value=_TEST_LOGGER
    ):
        yield


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_event(**overrides) -> dict:
    """Return a minimal valid clickstream event dict matching the API schema."""
    base = {
        "customer_id": 12345,
        "product_id": 776,
        "session_id": "sess_a1b2c3d4e5f6",
        "page_url": "https://adventure-works.com/product/776",
        "event_type": "page_view",
        "timestamp": "2026-03-22T14:30:45.123456Z",
    }
    base.update(overrides)
    return base


@contextmanager
def mock_snowflake(fetchone_return=None, fetchall_return=None):
    """
    Context manager that patches get_snowflake_connection to yield a mock
    Snowflake connection/cursor pair.
    """
    mock_cursor = MagicMock()
    if fetchone_return is not None:
        mock_cursor.fetchone.return_value = fetchone_return
    if fetchall_return is not None:
        mock_cursor.fetchall.return_value = fetchall_return

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    @contextmanager
    def fake_get_connection():
        yield mock_conn

    with patch(
        "flows.web_analytics_flow.get_snowflake_connection", fake_get_connection
    ):
        yield mock_cursor, mock_conn


# ---------------------------------------------------------------------------
# Unit Tests — clean_and_validate
# ---------------------------------------------------------------------------


class TestCleanAndValidate:
    """PRD criteria: null dropping, deduplication, timestamp cast/rename."""

    def test_empty_api_response_returns_empty_dataframe(self):
        """Empty event list returns an empty DataFrame with the correct columns."""
        df = clean_and_validate.fn([])
        assert df.empty
        assert list(df.columns) == [
            "customer_id",
            "product_id",
            "session_id",
            "page_url",
            "event_type",
            "event_timestamp",
        ]

    def test_drops_rows_with_null_customer_id(self):
        """Rows with null customer_id are dropped; the valid row is retained."""
        events = [
            make_event(customer_id=None),
            make_event(customer_id=99999),
        ]
        df = clean_and_validate.fn(events)
        assert len(df) == 1
        assert df.iloc[0]["customer_id"] == 99999

    def test_drops_rows_with_null_session_id(self):
        """Rows with null session_id are dropped; the valid row is retained."""
        events = [
            make_event(session_id=None),
            make_event(session_id="sess_keepme000001"),
        ]
        df = clean_and_validate.fn(events)
        assert len(df) == 1
        assert df.iloc[0]["session_id"] == "sess_keepme000001"

    def test_null_customer_id_and_null_session_id_both_dropped(self):
        """Both null customer_id and null session_id rows are dropped independently."""
        events = [
            make_event(customer_id=None),
            make_event(session_id=None),
            make_event(),  # fully valid
        ]
        df = clean_and_validate.fn(events)
        assert len(df) == 1

    def test_all_null_rows_yields_empty_dataframe(self):
        """When every row is invalid, the result is an empty DataFrame."""
        events = [make_event(customer_id=None), make_event(customer_id=None)]
        df = clean_and_validate.fn(events)
        assert df.empty

    def test_deduplication_removes_exact_duplicates(self):
        """Three identical rows collapse to one row."""
        event = make_event()
        df = clean_and_validate.fn([event, event, event])
        assert len(df) == 1

    def test_deduplication_keeps_distinct_rows(self):
        """Non-duplicate rows are all retained."""
        events = [
            make_event(event_type="page_view"),
            make_event(event_type="click"),
        ]
        df = clean_and_validate.fn(events)
        assert len(df) == 2

    def test_timestamp_column_renamed_to_event_timestamp(self):
        """The `timestamp` field is renamed to `event_timestamp`."""
        df = clean_and_validate.fn([make_event()])
        assert "event_timestamp" in df.columns
        assert "timestamp" not in df.columns

    def test_event_timestamp_is_datetime_dtype(self):
        """event_timestamp is a pandas datetime64 dtype (timezone-naive)."""
        df = clean_and_validate.fn([make_event()])
        assert pd.api.types.is_datetime64_any_dtype(df["event_timestamp"])
        assert df["event_timestamp"].dt.tz is None  # TIMESTAMP_NTZ — no tz

    def test_event_timestamp_value_is_correct(self):
        """Parsed event_timestamp matches the UTC datetime in the raw string."""
        df = clean_and_validate.fn(
            [make_event(timestamp="2026-03-22T14:30:45.123456Z")]
        )
        expected = datetime(2026, 3, 22, 14, 30, 45, 123456)
        assert df.iloc[0]["event_timestamp"] == expected

    def test_customer_id_and_product_id_are_int(self):
        """customer_id and product_id are cast to Python int."""
        df = clean_and_validate.fn([make_event()])
        assert df["customer_id"].dtype == int or pd.api.types.is_integer_dtype(
            df["customer_id"]
        )
        assert pd.api.types.is_integer_dtype(df["product_id"])


# ---------------------------------------------------------------------------
# Unit Tests — extract_max_timestamp
# ---------------------------------------------------------------------------


class TestExtractMaxTimestamp:
    """
    PRD requirement: incremental logic successfully extracts the maximum
    event_timestamp from the current batch to use as the `since` parameter
    for the next run.
    """

    def test_returns_none_for_empty_dataframe(self):
        df = pd.DataFrame(columns=["event_timestamp"])
        assert extract_max_timestamp(df) is None

    def test_returns_none_when_column_missing(self):
        df = pd.DataFrame({"other_col": [1, 2, 3]})
        assert extract_max_timestamp(df) is None

    def test_returns_max_timestamp_as_iso_string(self):
        """Returns the largest event_timestamp as an ISO 8601 Z-suffixed string."""
        ts_early = datetime(2026, 3, 22, 10, 0, 0)
        ts_late = datetime(2026, 3, 22, 14, 30, 45, 123456)
        df = pd.DataFrame({"event_timestamp": [ts_early, ts_late]})
        result = extract_max_timestamp(df)
        assert result == "2026-03-22T14:30:45.123456Z"

    def test_single_row_returns_its_timestamp(self):
        ts = datetime(2026, 1, 15, 8, 0, 0)
        df = pd.DataFrame({"event_timestamp": [ts]})
        result = extract_max_timestamp(df)
        assert result == "2026-01-15T08:00:00.000000Z"

    def test_max_is_chosen_not_first(self):
        """The maximum, not the first row, is returned."""
        df = pd.DataFrame(
            {
                "event_timestamp": [
                    datetime(2026, 3, 22, 14, 0, 0),
                    datetime(2026, 3, 22, 8, 0, 0),
                    datetime(2026, 3, 22, 22, 0, 0),  # latest
                ]
            }
        )
        result = extract_max_timestamp(df)
        assert result == "2026-03-22T22:00:00.000000Z"


# ---------------------------------------------------------------------------
# Unit Tests — get_since_parameter (incremental logic via Snowflake)
# ---------------------------------------------------------------------------


class TestGetSinceParameter:
    """
    PRD assumption: `since` is derived from MAX(event_timestamp) in Snowflake;
    defaults to 60 minutes ago if the table is empty or unavailable.
    """

    def test_uses_max_event_timestamp_from_snowflake(self):
        """Returns the Snowflake max event_timestamp formatted as ISO 8601."""
        max_ts = datetime(2026, 3, 22, 14, 30, 45, 0)
        with mock_snowflake(fetchone_return=(max_ts,)):
            since = get_since_parameter.fn()
        assert since.startswith("2026-03-22T14:30:45")
        assert since.endswith("Z")

    def test_falls_back_to_60_minutes_ago_when_table_is_empty(self):
        """Returns ~60 minutes ago when Snowflake MAX returns None (empty table)."""
        with mock_snowflake(fetchone_return=(None,)):
            before = datetime.now(timezone.utc) - timedelta(
                minutes=DEFAULT_LOOKBACK_MINUTES + 1
            )
            since = get_since_parameter.fn()
            after = datetime.now(timezone.utc) - timedelta(
                minutes=DEFAULT_LOOKBACK_MINUTES - 1
            )

        since_dt = datetime.strptime(since, "%Y-%m-%dT%H:%M:%S.%fZ").replace(
            tzinfo=timezone.utc
        )
        assert before <= since_dt <= after

    def test_falls_back_to_60_minutes_ago_when_snowflake_fails(self):
        """Returns ~60 minutes ago when the Snowflake connection raises."""

        @contextmanager
        def failing_connection():
            raise Exception("Snowflake is unreachable")
            yield  # unreachable; required for @contextmanager to accept the function

        with patch(
            "flows.web_analytics_flow.get_snowflake_connection", failing_connection
        ):
            before = datetime.now(timezone.utc) - timedelta(
                minutes=DEFAULT_LOOKBACK_MINUTES + 1
            )
            since = get_since_parameter.fn()
            after = datetime.now(timezone.utc) - timedelta(
                minutes=DEFAULT_LOOKBACK_MINUTES - 1
            )

        since_dt = datetime.strptime(since, "%Y-%m-%dT%H:%M:%S.%fZ").replace(
            tzinfo=timezone.utc
        )
        assert before <= since_dt <= after


# ---------------------------------------------------------------------------
# Unit Tests — fetch_clickstream_events
# ---------------------------------------------------------------------------


class TestFetchClickstreamEvents:
    """PRD criterion: incremental API pull; empty response handled gracefully."""

    def test_returns_event_list_from_api(self):
        """fetch_clickstream_events returns exactly what the API returns."""
        mock_events = [make_event(), make_event(event_type="click")]
        with patch("flows.web_analytics_flow.fetch_with_retry", return_value=mock_events):
            events = fetch_clickstream_events.fn("2026-01-01T00:00:00.000000Z")
        assert events == mock_events

    def test_empty_api_response_is_returned_without_error(self):
        """An empty API response is returned as an empty list — no exception."""
        with patch("flows.web_analytics_flow.fetch_with_retry", return_value=[]):
            events = fetch_clickstream_events.fn("2026-01-01T00:00:00.000000Z")
        assert events == []


# ---------------------------------------------------------------------------
# Unit Tests — fetch_with_retry (HTTP error handling)
# ---------------------------------------------------------------------------


class TestFetchWithRetry:
    """PRD criteria: 429 uses Retry-After; 5xx uses exponential back-off."""

    def test_returns_json_on_200(self):
        with patch("requests.get") as mock_get:
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_resp.json.return_value = [make_event()]
            mock_resp.raise_for_status = MagicMock()
            mock_get.return_value = mock_resp

            result = fetch_with_retry("http://test/analytics/clickstream")

        assert result == [make_event()]

    def test_retries_on_429_using_retry_after_header(self):
        """A 429 response triggers a sleep using the Retry-After header value."""
        with (
            patch("requests.get") as mock_get,
            patch("time.sleep") as mock_sleep,
        ):
            rate_limited = MagicMock()
            rate_limited.status_code = 429
            rate_limited.headers = {"Retry-After": "3"}
            rate_limited.raise_for_status = MagicMock(
                side_effect=requests.HTTPError("429")
            )

            success = MagicMock()
            success.status_code = 200
            success.json.return_value = [make_event()]
            success.raise_for_status = MagicMock()

            mock_get.side_effect = [rate_limited, success]

            result = fetch_with_retry("http://test/analytics/clickstream")

        mock_sleep.assert_called_once_with(3)
        assert result == [make_event()]

    def test_retries_on_5xx_with_exponential_backoff(self):
        """A 500 response triggers an exponential back-off retry."""
        with (
            patch("requests.get") as mock_get,
            patch("time.sleep") as mock_sleep,
        ):
            server_error = MagicMock()
            server_error.status_code = 500
            server_error.raise_for_status = MagicMock(
                side_effect=requests.HTTPError("500")
            )

            success = MagicMock()
            success.status_code = 200
            success.json.return_value = [make_event()]
            success.raise_for_status = MagicMock()

            mock_get.side_effect = [server_error, success]

            result = fetch_with_retry("http://test/analytics/clickstream")

        assert mock_sleep.call_count == 1
        assert result == [make_event()]

    def test_raises_after_max_retries_exceeded(self):
        """Raises an exception after API_MAX_RETRIES consecutive 5xx responses."""
        from flows.web_analytics_flow import API_MAX_RETRIES

        with (
            patch("requests.get") as mock_get,
            patch("time.sleep"),
        ):
            error_resp = MagicMock()
            error_resp.status_code = 503
            error_resp.raise_for_status = MagicMock(
                side_effect=requests.HTTPError("503")
            )
            mock_get.return_value = error_resp

            with pytest.raises(requests.HTTPError):
                fetch_with_retry("http://test/analytics/clickstream")

        assert mock_get.call_count == API_MAX_RETRIES + 1


# ---------------------------------------------------------------------------
# Unit Tests — stage_and_load (Snowflake staging logic)
# ---------------------------------------------------------------------------


class TestStageAndLoad:
    """PRD criteria: CSV → stage → COPY INTO → REMOVE only on success."""

    def _make_clean_df(self) -> pd.DataFrame:
        """Return a minimal cleaned DataFrame ready for loading."""
        return pd.DataFrame(
            {
                "customer_id": [12345],
                "product_id": [776],
                "session_id": ["sess_a1b2c3d4e5f6"],
                "page_url": ["https://adventure-works.com/product/776"],
                "event_type": ["page_view"],
                "event_timestamp": [datetime(2026, 3, 22, 14, 30, 45, 123456)],
            }
        )

    def test_executes_put_copy_and_remove_on_success(self):
        """On success: PUT, COPY INTO, and REMOVE are all executed in order."""
        # COPY INTO result: (file, status, rows_parsed, rows_loaded, ...)
        copy_result = [("web_analytics_test.csv", "LOADED", 1, 1, 0, 0, None, None, None, None)]

        with mock_snowflake(fetchall_return=copy_result) as (mock_cursor, _):
            rows = stage_and_load.fn(self._make_clean_df())

        assert rows == 1
        calls = [str(c) for c in mock_cursor.execute.call_args_list]
        assert any("PUT" in c for c in calls)
        assert any("COPY INTO" in c for c in calls)
        assert any("REMOVE" in c for c in calls)

    def test_remove_is_skipped_when_copy_fails(self):
        """REMOVE must NOT be called when COPY INTO reports a failure."""
        copy_result = [("web_analytics_test.csv", "LOAD_FAILED", 1, 0, 1, 1, "bad", 2, 1, "col1")]

        with (
            mock_snowflake(fetchall_return=copy_result) as (mock_cursor, _),
            pytest.raises(RuntimeError, match="COPY INTO failed"),
        ):
            stage_and_load.fn(self._make_clean_df())

        calls = [str(c) for c in mock_cursor.execute.call_args_list]
        assert not any("REMOVE" in c for c in calls)

    def test_copy_into_targets_correct_table(self):
        """COPY INTO SQL references RAW_EXT.web_analytics_raw."""
        copy_result = [("web_analytics_test.csv", "LOADED", 1, 1, 0, 0, None, None, None, None)]

        with mock_snowflake(fetchall_return=copy_result) as (mock_cursor, _):
            stage_and_load.fn(self._make_clean_df())

        copy_call = next(
            str(c) for c in mock_cursor.execute.call_args_list if "COPY INTO" in str(c)
        )
        assert TARGET_TABLE in copy_call

    def test_stage_referenced_in_put_and_copy(self):
        """Both PUT and COPY INTO reference the correct stage name."""
        copy_result = [("web_analytics_test.csv", "LOADED", 1, 1, 0, 0, None, None, None, None)]

        with mock_snowflake(fetchall_return=copy_result) as (mock_cursor, _):
            stage_and_load.fn(self._make_clean_df())

        all_calls = " ".join(str(c) for c in mock_cursor.execute.call_args_list)
        # STAGE_NAME is "@WEB_ANALYTICS_STAGE"
        assert "WEB_ANALYTICS_STAGE" in all_calls

    def test_skipped_status_treated_as_success_and_file_removed(self):
        """A SKIPPED status (file already loaded) is treated as success; file is removed."""
        copy_result = [("web_analytics_test.csv", "SKIPPED", 0, 0, 0, 0, None, None, None, None)]

        with mock_snowflake(fetchall_return=copy_result) as (mock_cursor, _):
            rows = stage_and_load.fn(self._make_clean_df())

        assert rows == 0
        calls = [str(c) for c in mock_cursor.execute.call_args_list]
        assert any("REMOVE" in c for c in calls)


# ---------------------------------------------------------------------------
# Integration Tests
# ---------------------------------------------------------------------------

_INTEGRATION_ENV_VARS = [
    "API_BASE_URL",
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PASSWORD",
    "SNOWFLAKE_DATABASE",
    "SNOWFLAKE_WAREHOUSE",
]

_SKIP_INTEGRATION = pytest.mark.skipif(
    not all(os.getenv(v) for v in _INTEGRATION_ENV_VARS),
    reason=(
        "Integration tests require live credentials. "
        "Set: " + ", ".join(_INTEGRATION_ENV_VARS)
    ),
)


@pytest.mark.integration
@_SKIP_INTEGRATION
class TestIntegration:
    """
    Live end-to-end tests against the real API and Snowflake.
    These tests are skipped unless all required environment variables are set.
    """

    def test_api_connection_returns_at_least_one_event(self):
        """
        PRD integration test: flow connects to API and retrieves at least one batch.
        Validates the API response shape matches the documented schema.
        """
        url = f"{os.getenv('API_BASE_URL')}/analytics/clickstream"
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        events = response.json()

        assert isinstance(events, list), "API must return a JSON array"
        assert len(events) >= 1, "API must return at least one event"

        required_keys = {
            "customer_id",
            "product_id",
            "session_id",
            "page_url",
            "event_type",
            "timestamp",
        }
        assert required_keys.issubset(set(events[0].keys())), (
            f"API response missing fields: {required_keys - set(events[0].keys())}"
        )

    def test_data_lands_in_snowflake_with_correct_types(self):
        """
        PRD integration test: data lands in RAW_EXT.web_analytics_raw with the
        correct column types after a full flow run.
        """
        from flows.web_analytics_flow import get_snowflake_connection

        web_analytics_flow()

        with get_snowflake_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"DESCRIBE TABLE {TARGET_TABLE}")
            columns = {row[0].lower(): row[1].lower() for row in cursor.fetchall()}

        assert "customer_id" in columns
        assert "int" in columns["customer_id"] or "number" in columns["customer_id"]

        assert "event_timestamp" in columns
        assert "timestamp_ntz" in columns["event_timestamp"]

        assert "session_id" in columns
        assert "product_id" in columns

    def test_stage_is_empty_after_successful_flow_run(self):
        """
        PRD integration test: after a successful COPY INTO, the internal stage
        contains no CSV files from the completed batch.
        """
        from flows.web_analytics_flow import get_snowflake_connection

        web_analytics_flow()

        with get_snowflake_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"LIST {STAGE_NAME}")
            remaining = cursor.fetchall()

        csv_files = [r[0] for r in remaining if str(r[0]).endswith(".csv")]
        assert csv_files == [], (
            f"Stage {STAGE_NAME} still contains files after successful load: {csv_files}"
        )
