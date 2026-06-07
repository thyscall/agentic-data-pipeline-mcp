# Product Requirements Document: [Feature Name]

---

## 1. Problem Statement

Adventure Works is able to see when a purchase happens, but they are not able to see the user behavior or interactions on their website that lead to the purchase. This data is important for them to understand more fully why purchases are happening. Right now, the web data that they are missing is found in a REST API, but that API is not integrated into the data warehouse, and therefore the browsing patterns and user bahavior data is not being used. It can be used if it connected to the warehouse, transformed, and loaded for proper analysis. 

When this happens, analysts at Adventure Works will be able to undertstand what website information, UX/UI design, marketing, and sales materials are contributing most to their users going through the sales funnel and eventually purchasing the Adventure Works product. This is critical for business growth. 

---

## 2. Desired Outcome

The desired outcome of this data pipeline is to have a Prefect flow that runs on a custom schedule that pulls the web analytics data from the API, cleans and validates it, loads that data into Snowflake, and makes it available for analysis using dbt. The data should start at the web API, be passed through to Snowflake, and should then be made available for analysis and modeling in dbt. Here is the desired Prefect flow:

1. Pull events from GET /analytics/clickstream
2. Clean the data (type casting, null handling, deduplication)
3. Stage the cleaned data in a Snowflake internal stage
4. Load into a raw table using COPY INTO


---

## 3. Acceptance Criteria

_How will we know this works? List specific, testable criteria._

- [ ] Criterion 1: Flow pulls clickstream events incrementally using the since parameter.
- [ ] Criterion 2: Flow handles HTTP 429 (Rate Limit) using the Retry-After header.
- [ ] Criterion 3: Flow handles HTTP 5xx (Server Error) with exponential backoff.
- [ ] Criterion 4: Flow drops records with null customer_id values and removes exact duplicate events.
- [ ] Criterion 5: Flow writes a CSV, puts it in the @WEB_ANALYTICS_STAGE internal stage, and executes COPY INTO RAW_EXT.web_analytics_raw
- [ ] Criterion 6: Flow executes REMOVE on the staged files only if the COPY INTO succeeds.

---

## 4. Technical Constraints

_What must the solution adhere to? These are non-negotiable._

- **Orchestration framework:** Prefect 2.0+
- **Target warehouse:** Snowflake (credentials via environment variables)
- **Loading pattern:** Upload to internal stage, then COPY INTO raw table
- **Containerization:** Must run in Docker, integrated with existing Docker Compose
- **Error handling:** Must include logging and graceful failure (no silent drops)
- **Scheduling:** Configurable interval via environment variable
- [Add any additional constraints specific to your implementation]

---

## 5. Data Schema

_What does the data look like? Document the expected fields._

### API Response Schema (Expected)

_Discover the API schema by exploring the documentation endpoints at your API base URL:_
- _`/docs` — Interactive Swagger UI_
- _`/agent-docs` — Agent-friendly markdown description_
- _`/example` — A single example event to inspect_

| Field | Type | Description | Nullable? |
|-------|------|-------------|-----------|
| customer_id | int | Adventure Works customer ID, joins with stg_adventure_db_customers | No |
| product_id | int | Adventure Works product ID, joins with stg_adventure_db_products | No |
| session_id | str | Unique browsing session ID (format: "sess_" + 12 hex chars) | No |
| page_url | str | URL of page viewed | No |
| event_type | str | One of: "page_view", "click", "add_to_cart", "purchase" | No |
| timestamp | str | ISO 8601 UTC datetime | No |


### Target Table Schema (Snowflake)

| Column | Type | Source |
|--------|------|--------|
| customer_id | int | API customer_id |
| product_id | int | API product_id |
| session_id | VARCHAR | API session_id |
| page_url | VARCHAR | API page_url |
| event_type | VARCHAR | API event_type |
| event_timestamp | TIMESTAMP_NTZ | API timestamp* |

*Note for data transformation: The API returns timestamp (string). This must be cast to a datetime object and renamed to event_timestamp to match the Snowflake target schema.
df.rename(columns={'timestamp': 'event_timestamp'}, inplace=True)

---

## 6. Testing Requirements

_How should the agent test its work?_

- [ ] Unit test: Flow functions handle empty API response
- [ ] Unit test: Data cleaning handles null customer_id or null session_id rows by dropping and logging it instead of failing
- [ ] Unit test: Deduplication removes exact duplicates
- [ ] Unit test: Data transformation successfully renames the timestamp field to event_timestamp and casts it to a datetime object
- [ ] Unit test: Incremental logic successfully extracts the maximum event_timestamp from the current batch to use as the since parameter for the next run
- [ ] Integration test: Flow connects to API and retrieves at least one batch
- [ ] Integration test: Data lands in Snowflake raw table with correct types
- [ ] Integration test: Data successfully lands in the Snowflake RAW_EXT.web_analytics_raw table with the correct data types, and the internal stage is verified as empty afterward

---

## 7. Out of Scope

_What should the agent NOT build? Set boundaries._

- Do not build the dbt models for this data. That will be done separately.
- Do not modify existing models or sources in the dbt/ and processor/ directories
- Do not modify compose.yml or dockerfile because these are already configured
- Do not build any dashboards or queries in Snwosight. Stick to ETL strictly

---

## 8. Questions and Assumptions

_What are you unsure about? What assumptions are you making?_

- **Assumption:** The flow will determine the "since" parameter for API pulls by getting the max event_timestamp from the RAW_EXT.web_analytics_raw table in Snowflake, and if not, it will check back in 60 minutes. 
- **Question:** Should we store the raw JSON response or the parsed fields?
- **Assumption:** The API information is documented correctly in the @api_docs.md file
- **Assumption:** The API returns timestamps in UTC, and Snowflake expects TIMESTAMP_NTZ without a time zone. The flow needs to handle this conversion smoothly
- **Assumption:** The script will run in a Docker container where API_BASE_URL, SNOWFLAKE_ACCOUNT, etc. are securely injected as env variables.
- **Question:** If the COPY INTO command fails, should the script leave the CSV files in the Snowflake internal stage for the next cycle to try again? **Assumption**: If the COPY INTO command fails, the script should leave the CSV files in the Snowflake internal stage for the next cycle. Do not execute the REMOVE command unless the data successfully loads into the raw table.
