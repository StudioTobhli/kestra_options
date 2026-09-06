# Streamlit Price Trend Chart - Task List

Companion to `.specify/plans/streamlit_price_trend_chart.md` (approved). All design decisions are baked into the tasks below — do not re-derive anything. Execute T001 → T004 (T001 and T002 may be done in either order; both must complete before T003).

Verified facts (already checked — use as-is, do not re-derive):

- `scripts_for_flow/options/dashboard.py` is 172 lines. The last existing import is line 12: `from sqlalchemy import create_engine`.
- The ticker dataframe variable is **`put_candidates_df`**, loaded from `put_candidate_tickers` at line 46: `put_candidates_df = pd.read_sql_query("SELECT * FROM put_candidate_tickers", engine)`. The plan's shorthand `candidates_df` maps to `put_candidates_df` in this file.
- The candidate summary table the chart must sit **above** is the block starting at line 163: `# Candidate summary table` / `st.subheader("📈 Candidate Summary")` / `st.dataframe(put_candidates_df[summary_cols], ...)`.
- The DB engine is created at line 34: `engine = create_engine(f'postgresql://root:root@{db_host}:5432/option_data')`.
- The `streamlit-dashboard` compose service builds from `Dockerfile.streamlit` and mounts `./scripts_for_flow/options:/app/scripts` with `DATABASE_HOST=pgdatabase` — so `.py` edits need no rebuild, but a Dockerfile change does.

---

## T001 [P] Add plotly to Dockerfile.streamlit

**File**: `Dockerfile.streamlit`
**Time**: 2 min

Edit the pip install line (and its comment). The top of the file currently reads:

```dockerfile
FROM options_python_img:latest

# Add Streamlit
RUN pip install --no-cache-dir streamlit
```

After your edit, the top of the file must read:

```dockerfile
FROM options_python_img:latest

# Add Streamlit and plotly (plotly powers the price trend chart)
RUN pip install --no-cache-dir streamlit plotly
```

Leave the rest of the file (`WORKDIR /app/scripts`, `EXPOSE 8501`, the `CMD` line) unchanged.

**Verification**: `cat Dockerfile.streamlit` shows exactly one line `RUN pip install --no-cache-dir streamlit plotly` and no other changes.

---

## T002 [P] Add the price trend chart section to dashboard.py

**File**: `scripts_for_flow/options/dashboard.py`
**Time**: 15 min

Two edits. Do not modify any existing code — both edits are pure insertions.

**Edit 1 — imports.** Immediately after line 12 (`from sqlalchemy import create_engine`), add:

```python
from time import time
import plotly.graph_objects as go
```

**Edit 2 — chart section.** Insert the block below immediately before the line `# Candidate summary table` (currently line 163, directly above `st.subheader("📈 Candidate Summary")`). This places the chart above the candidate summary table, matching the Looker reference layout. Note the block already uses the correct variable name `put_candidates_df`:

```python
# ### Daily Price Trend Chart
# - Data source: stock_hist_data (populated by stock_hist.py each morning)
# - Line chart of daily closing price for the last month of trading days
# - Ticker selectable from put_candidate_tickers (already loaded above)
st.subheader("Closing Price 1-month History")

ticker_list = sorted(put_candidates_df['ticker'].dropna().unique().tolist())

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

st.markdown("---")
```

Notes:
- Use `put_candidates_df`, **not** `candidates_df` (see verified facts above).
- The trailing `st.markdown("---")` separates the chart from the Candidate Summary section, matching the file's existing section style.
- The existing `st.subheader("📈 Candidate Summary")` block below must remain untouched.

**Verification**:
- `grep -n "from time import time\|plotly.graph_objects" scripts_for_flow/options/dashboard.py` shows both new imports near the top.
- `grep -n "Closing Price 1-month History\|price_trend_ticker\|st.plotly_chart\|Candidate Summary" scripts_for_flow/options/dashboard.py` — the chart lines must have smaller line numbers than the `📈 Candidate Summary` line.
- Syntax check from the repo root: `python3 -m py_compile scripts_for_flow/options/dashboard.py` → exit code 0, no output. (py_compile only compiles, so it works before plotly is installed.)

---

## T003 Rebuild and restart the streamlit-dashboard service

**File**: (none — service operation)
**Time**: 5 min

**Prerequisite**: if the main project's stack is running its own `streamlit-dashboard` container, stop it first — both compose files use the same fixed `container_name: streamlit-dashboard` and host port 8501, so they conflict:

```bash
cd ~/kestra_options && docker-compose stop streamlit-dashboard
cd -   # back to the worktree repo root
```

(If no such container exists, skip this.)

**Rebuild and start** — run from the worktree repo root (the compose volume mount is relative, so this serves the worktree's scripts):

```bash
docker-compose up -d --build streamlit-dashboard
docker-compose logs --tail 50 streamlit-dashboard
```

`--build` is required because T001 changed `Dockerfile.streamlit`. The T002 script edits flow through the existing `./scripts_for_flow/options:/app/scripts` bind mount and do not need a rebuild.

**Verification**:
- `docker-compose ps streamlit-dashboard` shows the container Up/Running.
- `docker-compose logs streamlit-dashboard` shows `You can now view your Streamlit app in your browser` / `Local URL: http://localhost:8501` and **no** `ModuleNotFoundError: plotly` or other traceback.

---

## T004 Manual verification in the browser

**Time**: 10 min

1. Confirm the source data looks right (expect ~20-22 rows per ticker spanning ~1 month):

   ```bash
   docker-compose exec pgdatabase psql -U root -d option_data \
     -c "SELECT ticker, COUNT(*), MIN(hist_date)::date AS first_day, MAX(hist_date)::date AS last_day
         FROM stock_hist_data
         WHERE as_of_date = (SELECT MAX(as_of_date) FROM stock_hist_data)
         GROUP BY ticker ORDER BY ticker;"
   ```

2. Confirm the exact query the chart runs succeeds from inside the container:

   ```bash
   docker exec streamlit-dashboard python -c "
   import os
   from sqlalchemy import create_engine
   import pandas as pd
   db_host = os.environ.get('DATABASE_HOST', 'localhost')
   engine = create_engine(f'postgresql://root:root@{db_host}:5432/option_data')
   tickers = sorted(pd.read_sql_query('SELECT ticker FROM put_candidate_tickers', engine)['ticker'].tolist())
   print('candidate tickers:', tickers)
   t = tickers[0]
   sql = 'SELECT hist_date, close FROM stock_hist_data WHERE ticker = :ticker AND as_of_date = (SELECT MAX(as_of_date) FROM stock_hist_data) ORDER BY hist_date'
   df = pd.read_sql_query(sql, engine, params={'ticker': t})
   print(f'{t} history rows: {len(df)}')
   "
   ```

3. Open `http://localhost:8501` in a browser and check each box:

   - [ ] "Closing Price 1-month History" line chart renders **above** the "📈 Candidate Summary" table.
   - [ ] The "Ticker" selectbox lists all tickers from `put_candidate_tickers`; selecting a different ticker switches the line to that ticker's history.
   - [ ] Hovering the line shows a tooltip with the trading day and closing price; the x-axis shows ~20-22 trading-day labels spanning ~1 month.
   - [ ] No regression: the "🎯 Put Candidate Prices" section and the "📈 Candidate Summary" table below the chart still render correctly.
   - [ ] Empty state: select a candidate ticker with no rows in `stock_hist_data` (if one exists) — an "No price history available for ..." info notice shows instead of a crash.
   - [ ] No red exception box anywhere on the page.

**Verification**: all browser checkboxes pass, and after interacting with the chart, `docker-compose logs streamlit-dashboard` shows a `Price trend chart query time: ...` line (confirms the new section executed).

---

## Execution Order

T001 and T002 touch different files and are independent (`[P]`); T003 needs both; T004 needs T003:

```bash
# Parallel execution:
Task: T001 (Dockerfile.streamlit)
Task: T002 (dashboard.py)

# Sequential:
Task: T003 (rebuild + restart - depends on T001 + T002)
Task: T004 (browser verification - depends on T003)
```

---

## Checkpoints

- **[T001]**: `Dockerfile.streamlit` contains `RUN pip install --no-cache-dir streamlit plotly`
- **[T002]**: `dashboard.py` compiles (`python3 -m py_compile` exit 0) and the chart section sits above `📈 Candidate Summary`
- **[T003]**: `streamlit-dashboard` container is Up with a clean log (Streamlit local URL line, no tracebacks, no plotly import errors)
- **[T004]**: Chart visible at http://localhost:8501 above the summary table, selectbox switches tickers, empty state handled, no regressions in existing sections

---

## Notes

- **Variable name**: the approved plan's snippet uses `candidates_df`; the actual variable in this file is `put_candidates_df` (line 46). The T002 block above already uses the correct name — follow the block.
- **No compose changes needed**: `docker-compose.yml` already mounts `./scripts_for_flow/options:/app/scripts` and sets `DATABASE_HOST=pgdatabase`.
- **No Kestra/pipeline changes**: `stock_hist.py` and the `stock_hist_data` schema are untouched; the chart is a read-only consumer of the DB.
- **Container name collision**: only one stack (main project or this worktree) can run `streamlit-dashboard` at a time (fixed container name + host port 8501). See the T003 prerequisite.
