"""
Web Analytics Prefect Flow
==========================

Ingests clickstream data from the Adventure Works Web Analytics REST API into
Snowflake incrementally, then cleans and stages it for downstream dbt modeling.

Flow steps:
    1. Determine the incremental `since` parameter (max event_timestamp from
       RAW_EXT.web_analytics_raw, or a 60-minute default lookback).
    2. Pull events from GET /analytics/clickstream?since=<ts>
    3. Clean: drop null customer_id / session_id rows, deduplicate, cast and
       rename `timestamp` → `event_timestamp` (TIMESTAMP_NTZ).
    4. Write cleaned data to a temp CSV.
    5. PUT CSV to Snowflake internal stage @WEB_ANALYTICS_STAGE.
    6. COPY INTO RAW_EXT.web_analytics_raw.
    7. REMOVE staged file — only executed after a successful COPY INTO.

Environment variables (all required unless noted):
    API_BASE_URL                   (default: http://localhost:8000)
    SNOWFLAKE_ACCOUNT
    SNOWFLAKE_USER
    SNOWFLAKE_PASSWORD
    SNOWFLAKE_DATABASE
    SNOWFLAKE_SCHEMA               (default: RAW_EXT)
    SNOWFLAKE_WAREHOUSE
    SNOWFLAKE_ROLE                 (optional)
    FLOW_SCHEDULE_INTERVAL_MINUTES (default: 60)
"""
# mypy: disable-error-code=import-untyped
# mypy: disable-error-code=import-not-found
# mypy: disable-error-code=attr-defined

import logging
import os
import tempfile
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd
import requests
import snowflake.connector
from prefect import flow, get_run_logger, task

# ---------------------------------------------------------------------------
# Configuration from environment variables
# ---------------------------------------------------------------------------

API_BASE_URL: str = os.getenv("API_BASE_URL", "http://localhost:8000")

SNOWFLAKE_ACCOUNT: Optional[str] = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER: Optional[str] = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD: Optional[str] = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_DATABASE: Optional[str] = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA: str = os.getenv("SNOWFLAKE_SCHEMA", "RAW_EXT")
SNOWFLAKE_WAREHOUSE: Optional[str] = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_ROLE: Optional[str] = os.getenv("SNOWFLAKE_ROLE")

FLOW_SCHEDULE_INTERVAL_MINUTES: int = int(
    os.getenv("FLOW_SCHEDULE_INTERVAL_MINUTES", "60")
)

TARGET_TABLE: str = "RAW_EXT.web_analytics_raw"
STAGE_NAME: str = "@WEB_ANALYTICS_STAGE"
DEFAULT_LOOKBACK_MINUTES: int = 60

# Retry parameters for the API client
API_MAX_RETRIES: int = 5
API_BASE_DELAY_SECONDS: float = 1.0
API_MAX_DELAY_SECONDS: float = 60.0
API_REQUEST_TIMEOUT_SECONDS: int = 30

_LOG = logging.getLogger(__name__)

# Ordered list of columns written to CSV and loaded into Snowflake
_TARGET_COLUMNS = [
    "customer_id",
    "product_id",
    "session_id",
    "page_url",
    "event_type",
    "event_timestamp",
]

# Statuses returned by Snowflake COPY INTO that indicate the file is accounted for
_COPY_SUCCESS_STATUSES = {"LOADED", "SKIPPED"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@contextmanager
def get_snowflake_connection():
    """Yield an open Snowflake connection built from environment variables."""
    conn = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        warehouse=SNOWFLAKE_WAREHOUSE,
        role=SNOWFLAKE_ROLE,
    )
    try:
        yield conn
    finally:
        conn.close()


def fetch_with_retry(url: str, params: Optional[dict] = None) -> list:
    """
    HTTP GET with targeted retry handling:
      - HTTP 429: honour the ``Retry-After`` response header.
      - HTTP 5xx: exponential back-off starting at ``API_BASE_DELAY_SECONDS``.
      - Timeout / ConnectionError: exponential back-off.
    Raises on any non-retriable HTTP error or after exceeding ``API_MAX_RETRIES``.
    """
    attempt = 0
    delay = API_BASE_DELAY_SECONDS

    while True:
        try:
            response = requests.get(
                url, params=params, timeout=API_REQUEST_TIMEOUT_SECONDS
            )

            # --- Rate limited ---
            if response.status_code == 429:
                if attempt >= API_MAX_RETRIES:
                    response.raise_for_status()
                retry_after = int(response.headers.get("Retry-After", 5))
                _LOG.warning(
                    "HTTP 429 Rate Limited. Waiting %d s (attempt %d/%d).",
                    retry_after,
                    attempt + 1,
                    API_MAX_RETRIES,
                )
                time.sleep(retry_after)
                attempt += 1
                continue

            # --- Server error ---
            if response.status_code >= 500:
                if attempt >= API_MAX_RETRIES:
                    response.raise_for_status()
                _LOG.warning(
                    "HTTP %d Server Error. Retrying in %.1f s (attempt %d/%d).",
                    response.status_code,
                    delay,
                    attempt + 1,
                    API_MAX_RETRIES,
                )
                time.sleep(delay)
                delay = min(delay * 2, API_MAX_DELAY_SECONDS)
                attempt += 1
                continue

            response.raise_for_status()
            return response.json()

        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
            if attempt >= API_MAX_RETRIES:
                raise
            _LOG.warning(
                "Request error: %s. Retrying in %.1f s (attempt %d/%d).",
                exc,
                delay,
                attempt + 1,
                API_MAX_RETRIES,
            )
            time.sleep(delay)
            delay = min(delay * 2, API_MAX_DELAY_SECONDS)
            attempt += 1


def extract_max_timestamp(df: pd.DataFrame) -> Optional[str]:
    """
    Return the maximum ``event_timestamp`` from *df* as an ISO 8601 UTC string
    (e.g. ``"2026-03-22T14:30:45.123456Z"``), or ``None`` when *df* is empty or
    the column is absent.

    Used to derive the ``since`` parameter for the next incremental run.
    """
    if df.empty or "event_timestamp" not in df.columns:
        return None
    max_ts = df["event_timestamp"].max()
    if pd.isna(max_ts):
        return None
    return max_ts.strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task(name="get_since_parameter")
def get_since_parameter() -> str:
    """
    Determine the incremental ``since`` parameter for the API pull.

    Queries ``RAW_EXT.web_analytics_raw`` for the maximum ``event_timestamp``.
    Falls back to ``DEFAULT_LOOKBACK_MINUTES`` ago when the table is empty or
    the Snowflake connection is unavailable.
    """
    logger = get_run_logger()

    try:
        with get_snowflake_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT MAX(event_timestamp) FROM {TARGET_TABLE}")
            row = cursor.fetchone()
            if row and row[0] is not None:
                max_ts: datetime = row[0]
                since = max_ts.strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"
                logger.info("Incremental load — since=%s (from Snowflake max).", since)
                return since
            logger.info(
                "No existing data in %s. Using default %d-minute lookback.",
                TARGET_TABLE,
                DEFAULT_LOOKBACK_MINUTES,
            )
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Could not query Snowflake for max timestamp (%s). "
            "Falling back to %d-minute lookback.",
            exc,
            DEFAULT_LOOKBACK_MINUTES,
        )

    """Build a Prefect flow that ingests clickstream data from the Adventure Works
    Web Analytics API into Snowflake. See milestone-2-instructions.md for
    requirements and the API docs at your API_BASE_URL for the data contract.
    """
    default_since = (
        datetime.now(timezone.utc) - timedelta(minutes=DEFAULT_LOOKBACK_MINUTES)
    ).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"
    logger.info("Default since parameter: %s", default_since)
    return default_since


@task(name="fetch_clickstream_events")
def fetch_clickstream_events(since: str) -> list:
    """
    Call ``GET /analytics/clickstream?since=<since>`` and return the raw event
    list. HTTP 429 and 5xx errors are retried automatically via
    :func:`fetch_with_retry`.
    """
    logger = get_run_logger()
    url = f"{API_BASE_URL}/analytics/clickstream"
    logger.info("Fetching events from %s with since=%s.", url, since)
    events = fetch_with_retry(url, params={"since": since})
    logger.info("Received %d raw event(s).", len(events))
    return events


@task(name="clean_and_validate")
def clean_and_validate(raw_events: list) -> pd.DataFrame:
    """
    Clean and validate raw clickstream events.

    Steps (each invalid row is logged before being dropped — never silently):
      1. Drop rows where ``customer_id`` is null.
      2. Drop rows where ``session_id`` is null.
      3. Remove exact duplicate rows.
      4. Cast ``timestamp`` (ISO 8601 string) to a UTC-naive ``datetime`` and
         rename the column to ``event_timestamp`` to match the Snowflake schema.

    Returns a DataFrame with columns matching ``_TARGET_COLUMNS``.
    """
    logger = get_run_logger()

    if not raw_events:
        logger.info("No raw events to clean.")
        return pd.DataFrame(columns=_TARGET_COLUMNS)

    df = pd.DataFrame(raw_events)
    initial_count = len(df)
    logger.info("Starting data cleaning: %d raw event(s).", initial_count)

    # --- Drop null customer_id ---
    null_cid = df["customer_id"].isna()
    if null_cid.any():
        logger.warning(
            "Dropping %d row(s) with null customer_id.", int(null_cid.sum())
        )
        df = df[~null_cid].copy()

    # --- Drop null session_id ---
    null_sid = df["session_id"].isna()
    if null_sid.any():
        logger.warning(
            "Dropping %d row(s) with null session_id.", int(null_sid.sum())
        )
        df = df[~null_sid].copy()

    # --- Deduplicate exact rows ---
    before_dedup = len(df)
    df = df.drop_duplicates()
    dupes_removed = before_dedup - len(df)
    if dupes_removed:
        logger.info("Removed %d exact duplicate row(s).", dupes_removed)

    if df.empty:
        logger.warning("All rows were dropped during cleaning.")
        return pd.DataFrame(columns=_TARGET_COLUMNS)

    # --- Cast and rename timestamp → event_timestamp ---
    # pd.to_datetime with utc=True parses the "Z"-suffixed ISO string into a
    # UTC-aware Series; tz_convert(None) strips the timezone to produce a
    # naive datetime (TIMESTAMP_NTZ compatible).
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert(None)
    df.rename(columns={"timestamp": "event_timestamp"}, inplace=True)

    # --- Select and enforce target column types ---
    df = df[_TARGET_COLUMNS].copy()
    df["customer_id"] = df["customer_id"].astype(int)
    df["product_id"] = df["product_id"].astype(int)

    logger.info(
        "Cleaning complete: %d valid event(s) (started with %d).",
        len(df),
        initial_count,
    )
    return df


@task(name="stage_and_load")
def stage_and_load(df: pd.DataFrame) -> int:
    """
    Upload a cleaned DataFrame into Snowflake via the internal stage.

    Steps:
      1. Write the DataFrame to a uniquely named temp CSV file.
      2. ``PUT`` the CSV to ``@WEB_ANALYTICS_STAGE`` (no compression).
      3. ``COPY INTO RAW_EXT.web_analytics_raw`` using a SELECT transformation
         that maps CSV columns and captures ``METADATA$FILENAME`` for the
         ``_file_name`` audit column.
      4. ``REMOVE`` the staged file **only** after a successful ``COPY INTO``.
         On failure the file is left in the stage so the next run can retry.

    Returns the number of rows loaded into Snowflake.
    """
    logger = get_run_logger()

    file_name = (
        f"web_analytics_"
        f"{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_"
        f"{uuid.uuid4().hex[:8]}.csv"
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        local_path = os.path.join(tmpdir, file_name)

        # Format event_timestamp without timezone offset for TIMESTAMP_NTZ
        df_out = df.copy()
        df_out["event_timestamp"] = df_out["event_timestamp"].dt.strftime(
            "%Y-%m-%d %H:%M:%S.%f"
        )
        df_out.to_csv(local_path, index=False)
        logger.info("Wrote %d row(s) to temp file: %s.", len(df_out), local_path)

        with get_snowflake_connection() as conn:
            cursor = conn.cursor()

            # ----------------------------------------------------------------
            # PUT — upload CSV to internal stage
            # ----------------------------------------------------------------
            put_sql = (
                f"PUT file://{local_path} {STAGE_NAME} "
                f"AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
            )
            logger.info("Staging file: %s", put_sql)
            cursor.execute(put_sql)
            put_result = cursor.fetchall()
            logger.info("PUT result: %s", put_result)

            # ----------------------------------------------------------------
            # COPY INTO — load from stage with column transformation
            # ----------------------------------------------------------------
            # Reference the specific file in the stage path to avoid picking
            # up files from concurrent runs. METADATA$FILENAME populates the
            # _file_name audit column. ON_ERROR=ABORT_STATEMENT (default)
            # ensures no partial loads occur silently.
            copy_sql = f"""
                COPY INTO {TARGET_TABLE} (
                    customer_id,
                    product_id,
                    session_id,
                    page_url,
                    event_type,
                    event_timestamp,
                    _file_name
                )
                FROM (
                    SELECT
                        $1::INT,
                        $2::INT,
                        $3::VARCHAR,
                        $4::VARCHAR,
                        $5::VARCHAR,
                        TO_TIMESTAMP_NTZ($6),
                        METADATA$FILENAME
                    FROM {STAGE_NAME}/{file_name}
                )
                FILE_FORMAT = (
                    TYPE                         = 'CSV'
                    SKIP_HEADER                  = 1
                    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                    NULL_IF                      = ('')
                )
                ON_ERROR = 'ABORT_STATEMENT'
            """
            logger.info("Executing COPY INTO %s.", TARGET_TABLE)
            cursor.execute(copy_sql)
            copy_results = cursor.fetchall()
            logger.info("COPY INTO result: %s", copy_results)

            # Raise immediately if any file failed — staged files are preserved
            # for the next cycle to retry (PRD assumption).
            failed = [r for r in copy_results if r[1] not in _COPY_SUCCESS_STATUSES]
            if failed:
                logger.error(
                    "COPY INTO reported failure(s) — staged file preserved for retry: %s",
                    failed,
                )
                raise RuntimeError(
                    f"COPY INTO failed for {len(failed)} file(s). "
                    "Staged files have been preserved for the next run."
                )

            rows_loaded = (
                sum(r[3] for r in copy_results if r[1] == "LOADED")
                if copy_results
                else 0
            )
            logger.info(
                "Successfully loaded %d row(s) into %s.", rows_loaded, TARGET_TABLE
            )

            # ----------------------------------------------------------------
            # REMOVE — clean up stage only after confirmed success
            # ----------------------------------------------------------------
            remove_sql = f"REMOVE {STAGE_NAME}/{file_name}"
            logger.info("Removing staged file: %s", remove_sql)
            cursor.execute(remove_sql)
            logger.info("Staged file '%s' removed from %s.", file_name, STAGE_NAME)

            return rows_loaded


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="web_analytics_flow", log_prints=True)
def web_analytics_flow() -> None:
    """
    Adventure Works Web Analytics ingestion flow.
    API documentation endpoints:
    {API_BASE_URL}/          — Overview and endpoint listing
    {API_BASE_URL}/docs      — Swagger UI (interactive)
    {API_BASE_URL}/agent-docs — Agent-friendly markdown (paste into your AI tool)
    {API_BASE_URL}/example   — Single example event (inspect the data shape)
    Pulls clickstream events from the REST API incrementally, validates and
    cleans them, and loads them into the Snowflake raw table for downstream
    dbt modeling.
    """
    logger = get_run_logger()
    logger.info("web_analytics_flow started.")

    since = get_since_parameter()

    raw_events = fetch_clickstream_events(since)
    if not raw_events:
        logger.info("No events returned by the API — nothing to load.")
        return

    df = clean_and_validate(raw_events)
    if df.empty:
        logger.info("No valid events after cleaning — nothing to load.")
        return

    rows_loaded = stage_and_load(df)
    logger.info("web_analytics_flow complete. Loaded %d row(s).", rows_loaded)


# ---------------------------------------------------------------------------
# Entrypoint — schedule via environment variable
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    web_analytics_flow.serve(
        name="web-analytics-scheduled",
        interval=timedelta(minutes=FLOW_SCHEDULE_INTERVAL_MINUTES),
    )
