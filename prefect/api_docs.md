# Adventure Works Web Analytics API — Agent Reference

## Endpoint

```
GET {API_BASE_URL}/analytics/clickstream
GET {API_BASE_URL}/analytics/clickstream?since=2026-03-22T14:00:00Z
```

Returns a JSON **array** of clickstream event objects.

## Query Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| since | string (ISO 8601) | 60 minutes ago | Only return events after this timestamp. Use for incremental extraction. |

The number of events scales with the time window: ~50 events per 60 minutes.
A 5-minute window returns ~4 events; a 30-minute window returns ~25.

## Response Schema

Each object in the array has these fields:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| customer_id | int | Yes | Adventure Works customer ID (range 11000–30118). Joins with Snowflake table stg_adventure_db__customers via customer_id. |
| product_id | int | Yes | Adventure Works product ID (range 707–999). Joins with stg_adventure_db__products via product_id. |
| session_id | string | Yes | Unique browsing session ID (format: "sess_" + 12 hex chars). |
| page_url | string | Yes | URL of the page viewed. |
| event_type | string | Yes | One of: "page_view", "click", "add_to_cart", "purchase". |
| timestamp | string | Yes | ISO 8601 UTC datetime (e.g., "2026-03-22T14:30:45.123456Z"). Falls within the requested window. |

## Example Response

```json
[
  {
    "customer_id": 29825,
    "product_id": 776,
    "session_id": "sess_a1b2c3d4e5f6",
    "page_url": "https://adventure-works.com/product/776",
    "event_type": "page_view",
    "timestamp": "2026-03-22T14:30:45.123456Z"
  }
]
```

## Example Python Code (Incremental)

```python
import requests
import os
from datetime import datetime, timezone

api_url = os.getenv("API_BASE_URL", "http://localhost:8000")

# First call: no since parameter, gets last 60 minutes
response = requests.get(f"{api_url}/analytics/clickstream", timeout=30)
response.raise_for_status()
events = response.json()

# Track the latest timestamp for next call
last_timestamp = max(e["timestamp"] for e in events) if events else None

# Subsequent calls: pass since to get only new events
response = requests.get(
    f"{api_url}/analytics/clickstream",
    params={"since": last_timestamp},
    timeout=30,
)
new_events = response.json()
print(f"Received {len(new_events)} new events")
```

## Error Handling

- **HTTP 200**: Success. Body is a JSON array.
- **HTTP 422**: Invalid query parameter (count out of range).
- **HTTP 429**: Rate limited (unlikely but handle with Retry-After header).
- **HTTP 5xx**: Server error. Retry with exponential backoff.

Always check `response.raise_for_status()` and wrap in try/except.

## Snowflake Target Table

The data should be loaded into this Snowflake raw table:

```sql
CREATE TABLE IF NOT EXISTS RAW_EXT.web_analytics_raw (
    customer_id     INT          NOT NULL,
    product_id      INT          NOT NULL,
    session_id      VARCHAR(255) NOT NULL,
    page_url        VARCHAR(1000),
    event_type      VARCHAR(50),
    event_timestamp TIMESTAMP_NTZ NOT NULL,
    _loaded_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _file_name      VARCHAR(255)
);
```

**Column mapping from API → Snowflake:**
- `customer_id` → `customer_id` (INT)
- `product_id` → `product_id` (INT)
- `session_id` → `session_id` (VARCHAR)
- `page_url` → `page_url` (VARCHAR)
- `event_type` → `event_type` (VARCHAR)
- `timestamp` → `event_timestamp` (TIMESTAMP_NTZ, rename required)
- (auto) → `_loaded_at` (DEFAULT)
- (auto) → `_file_name` (set during COPY INTO)

## Loading Strategy

1. Write cleaned DataFrame to CSV with header row
2. PUT the CSV file to Snowflake internal stage `@WEB_ANALYTICS_STAGE`
3. COPY INTO `RAW_EXT.web_analytics_raw` FROM `@WEB_ANALYTICS_STAGE` with `FILE_FORMAT = (TYPE='CSV', SKIP_HEADER=1)`
4. REMOVE staged files after successful COPY

## Data Quality Notes

- Some events may have `customer_id` values that don't exist in the customer dimension (foreign key test may catch these).
- `event_type` should always be one of the four valid values; the API guarantees this.
- Timestamps are always recent (within last 60 minutes). Source freshness checks should use `event_timestamp` with a 2-4 hour threshold.