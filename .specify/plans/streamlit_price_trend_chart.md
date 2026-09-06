# Streamlit Price Trend Chart - Implementation Plan

## Overview

Add a daily closing price trend chart to the Streamlit dashboard (`scripts_for_flow/options/dashboard.py`) that mirrors the existing Looker Studio chart titled **"Closing Price 1-month History"** (reference screenshot: `/home/jedah/screenshots/stock_hist_ss.png`). The Looker chart is a single line plot of the selected ticker's closing price across the last month of trading days, sitting directly above the put candidate summary table.

This plan:

- Renders a line chart of daily `close` from the `stock_hist_data` table for the last month of trading days (~20-22 points).
- Adds a ticker `st.selectbox` fed by the tickers already listed in `put_candidate_tickers` (loaded into `candidates_df` by the existing dashboard code).
- Places the chart **above** the candidate summary table to match the Looker layout.
- Uses `plotly` (via `st.plotly_chart`) for the chart so we get a titled, hover-enabled line plot consistent with the reference.

Architecture:

```
  Morning pipeline (Kestra)
  ─────────────────────────
  stock_hist.py ──yfinance 1mo──→  stock_hist_data (PostgreSQL: option_data)
                                        │
                                        │  SELECT hist_date, close
                                        │  WHERE ticker = selected AND latest as_of_date
                                        ▼
  ┌──────────────────────────────────────────────────────────┐
  │  Streamlit dashboard (port 8501)                         │
  │                                                          │
  │  st.selectbox("Ticker", [tickers from put_candidate_     │
  │       tickers])                                          │
  │  ─────────────────────────────────                       │
  │  Closing Price 1-month History   (NEW - plotly line)     │
  │  ─────────────────────────────────                       │
  │  Put Candidate Dashboard Table   (existing st.dataframe) │
  └──────────────────────────────────────────────────────────┘
```

No changes are needed to the Kestra flows, the ingest script, or the database schema. `stock_hist.py` already writes exactly the columns the chart needs, and `docker-compose.yml` already mounts `./scripts_for_flow/options` into the container and sets `DATABASE_HOST=pgdatabase`.

## Data Sources

### `stock_hist_data` (primary - chart data)

Populated by `scripts_for_flow/options/stock_hist.py`, which pulls `ticker.history(period="1mo")` from yfinance for every ticker in `stock_dim_data` and writes the result with `if_exists='replace'`. The table therefore always contains a **single 1-month pull** per deployment, stamped with the run time.

| Column | Type | Notes |
|---|---|---|
| `ticker` | VARCHAR(20) | e.g. `TSLA`, `AMD` |
| `hist_date` | DateTime | Trading day (yfinance index date) |
| `open` | Float | Not used by the chart |
| `high` | Float | Not used by the chart |
| `low` | Float | Not used by the chart |
| `close` | Float | **Y-axis of the chart** |
| `as_of_date` | DateTime | Timestamp of the ingest run |

Because the ingest uses `if_exists='replace'`, the table normally holds only one `as_of_date`. The query still guards on `MAX(as_of_date)` so the chart stays correct if multiple runs ever accumulate in the table.

The 1-month window is guaranteed by the ingest script (`period="1mo"`); no date-window filter is required in the chart query.

### `put_candidate_tickers` (ticker list for the selectbox)

Already loaded by the existing dashboard code into `candidates_df`. The selectbox options are `sorted(candidates_df['ticker'].unique())`. This matches the goal of "ideally filterable/selectable by ticker (the dashboard already lists tickers from `put_candidate_tickers`)".

### Database connection

Reuse the existing `engine` in `dashboard.py` (`postgresql://root:root@pgdatabase:5432/option_data` via the Docker service name, per project conventions). No new connection string is introduced.

## Implementation Steps

### Step 1: Add `plotly` to `Dockerfile.streamlit`

`Dockerfile.streamlit` is the build target of the `streamlit-dashboard` compose service in this worktree (it layers `streamlit` on top of `options_python_img:latest`). Add `plotly` to the existing pip install:

```dockerfile
FROM options_python_img:latest

# Add Streamlit and plotly (plotly powers the price trend chart)
RUN pip install --no-cache-dir streamlit plotly

# Set working directory
WORKDIR /app/scripts

# Expose Streamlit port
EXPOSE 8501

# Run Streamlit
CMD ["streamlit", "run", "dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]
```

Note: because the Dockerfile changes, the image must be rebuilt (see Testing). `plotly` is a pure-Python package, so no system dependencies or build-tool changes are needed.

### Step 2: Add the price trend chart section to `dashboard.py`

Insert the new section **after** the existing database connection / `candidates_df` load and **before** `st.dataframe(candidates_df)` so the chart renders above the table (matching the Looker screenshot).

Add imports at the top of `dashboard.py` (alongside the existing `pandas` import):

```python
from time import time
import plotly.graph_objects as go
```

New section (follows project conventions: triple-quoted SQL, `pd.read_sql_query`, `time()` for duration logging, try/except with safe defaults):

```python
# ### Daily Price Trend Chart
# - Data source: stock_hist_data (populated by stock_hist.py each morning)
# - Line chart of daily closing price for the last month of trading days
# - Ticker selectable from put_candidate_tickers (already loaded above)
st.subheader("Closing Price 1-month History")

ticker_list = sorted(candidates_df['ticker'].dropna().unique().tolist())

if not ticker_list:
    st.warning("No candidate tickers available to chart.")
else:
    selected_ticker = st.selectbox("Ticker", ticker_list, key="price_trend_ticker")

    price_sql = """
        SELECT
            hist_date,
            close
        FROM stock_hist_data
        WHERE ticker = :ticker
          AND as_of_date = (SELECT MAX(as_of_date) FROM stock_hist_data)
        ORDER BY hist_date
    """

    chart_start_time = time()
    try:
        price_df = pd.read_sql_query(price_sql, con=engine, params={'ticker': selected_ticker})
    except Exception as e:
        st.error(f"Error loading price history for {selected_ticker}: {e}")
        price_df = pd.DataFrame(columns=['hist_date', 'close'])

    if price_df.empty:
        st.info(f"No price history available for {selected_ticker}. Run the stock_hist ingest first.")
    else:
        fig = go.Figure(data=[
            go.Scatter(
                x=price_df['hist_date'],
                y=price_df['close'],
                mode='lines',
                name=selected_ticker
            )
        ])
        fig.update_layout(
            title="Closing Price 1-month History",
            xaxis_title="Trading Day",
            yaxis_title="Close ($)",
            height=350,
            margin=dict(t=40, b=20, l=20, r=20)
        )
        st.plotly_chart(fig, use_container_width=True)

    print(f"Price trend chart query time: {time() - chart_start_time:.3f} seconds")
```

Behavior notes:

- **Ticker selection**: `st.selectbox` defaults to the first (alphabetically) ticker in `put_candidate_tickers`. Changing the selectbox re-runs the query for the new ticker (Streamlit's normal rerun model).
- **Empty state**: a ticker with no rows in `stock_hist_data` (e.g. ingest failure for that symbol) shows an `st.info` notice instead of an error or a blank chart, consistent with the empty-state handling called out in `streamlit_dashboard_revival.md`.
- **Table errors**: wrapped in try/except returning an empty DataFrame and surfacing `st.error`, per the AGENTS.md error-handling convention.
- The existing table (`st.dataframe(candidates_df)`) is untouched and remains below the chart.

### Step 3: Rebuild and restart the Streamlit service

```bash
docker-compose up -d --build streamlit-dashboard
```

The `--build` is required because `Dockerfile.streamlit` changed. The script changes themselves are picked up via the existing `./scripts_for_flow/options:/app/scripts` bind mount.

## Testing

1. Rebuild and start the service:
   ```bash
   docker-compose up -d --build streamlit-dashboard
   docker-compose logs streamlit-dashboard
   ```
   Verify logs show Streamlit starting with no import errors (especially no `ModuleNotFoundError: plotly`).

2. Verify the source table has a recent 1-month pull:
   ```bash
   docker-compose exec pgdatabase psql -U root -d option_data \
     -c "SELECT ticker, COUNT(*), MIN(hist_date)::date AS first_day, MAX(hist_date)::date AS last_day
         FROM stock_hist_data
         WHERE as_of_date = (SELECT MAX(as_of_date) FROM stock_hist_data)
         GROUP BY ticker ORDER BY ticker;"
   ```
   Expect ~20-22 rows per ticker spanning ~1 month (e.g. Jul 28 - Aug 28 as in the reference screenshot).

3. Open the dashboard at `http://localhost:8501` and verify:
   - The "Closing Price 1-month History" line chart renders **above** the candidate summary table.
   - The selectbox lists all tickers from `put_candidate_tickers`; switching tickers switches the line.
   - Hover tooltips show the trading day and closing price; the x-axis shows ~20-22 trading-day labels.
   - The existing candidate table still renders correctly below the chart.

4. Empty-state check: select (or temporarily simulate) a ticker with no rows in `stock_hist_data` and confirm the `st.info` notice appears instead of a crash.

5. Worktree note: the worktree `docker-compose.yml` is byte-identical to the main project's and both use the fixed `container_name: streamlit-dashboard` (and host port 8501). Stop the main project's stack (`cd ~/kestra_options && docker-compose stop streamlit-dashboard`) before starting the worktree one, otherwise the containers will conflict. Running from the worktree is safe because the compose volume mount is relative (`./scripts_for_flow/options`) and `DATABASE_HOST=pgdatabase` resolves within whichever stack is running.

## Risk Assessment

| Risk | Mitigation |
|---|---|
| `stock_hist_data` empty or missing (ingest not yet run in this stack) | Empty-state `st.info` message; table is created by the existing morning flow |
| Multiple `as_of_date` runs accumulate in the table | Query filters on `MAX(as_of_date)` so only the latest pull is charted |
| New `plotly` dependency breaks the image build | `plotly` is pure Python with no system deps; install added to the existing `pip install` line in `Dockerfile.streamlit` |
| Ticker in `put_candidate_tickers` has no history (yfinance failure) | Per-ticker empty-state handling in the chart section |
| Container name / port conflict between main project and worktree stacks | Documented in Testing step 5 - only one stack runs the `streamlit-dashboard` container at a time |

## Estimated Effort

- Step 1 (Dockerfile.streamlit): 2 min
- Step 2 (dashboard.py chart section): 15 min
- Step 3 (rebuild + restart): 2 min
- Testing: 10 min
- **Total: ~30 minutes**

## Files to Create/Modify

1. **Modify**: `scripts_for_flow/options/dashboard.py` - add `time`/`plotly` imports and the "Closing Price 1-month History" chart section (selectbox + plotly line chart) above the existing candidate table
2. **Modify**: `Dockerfile.streamlit` - add `plotly` to the `pip install` line
3. **No changes**: `docker-compose.yml` (existing volume mount + `DATABASE_HOST` already cover the new code), `scripts_for_flow/options/stock_hist.py` (data source already provides the required columns), Kestra flows, database schema
