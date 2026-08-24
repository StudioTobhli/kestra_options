# Streamlit Dashboard Revival - Implementation Plan

## Overview

Revive and update the Streamlit dashboard (`scripts_for_flow/options/dashboard.py`) that surfaces put options candidates to sell. The dashboard was last run 6+ months ago and was shelved because there was no hosting solution. This implementation adds Streamlit as a continuously running Docker service that reads from the existing PostgreSQL database.

## Architecture

```
                    ┌─────────────────────────────────┐
                    │     Morning Pipeline (Kestra)     │
                    │                                   │
  Every morning     │  put_data_ingest  ─→  PostgreSQL │
  6:00 AM ──────────→  stock_dim_ingest ──→  (option_data)│
                    │  holdings_ingest    ─→           │
                    └──────────┬──────────────────────┘
                               │
                    ┌──────────▼──────────────────────┐
                    │     PostgreSQL Database           │
                    │     (running all day)             │
                    └──────────┬──────────────────────┘
                               │  reads data
                               ▼
                    ┌──────────▼──────────────────────┐
                    │     Streamlit Dashboard           │
                    │     (running all day on :8501)    │
                    │                                   │
  Your browser ─────→  http://localhost:8501            │
                    └─────────────────────────────────┘
```

**Two independent services:**
- **Kestra** (runs every morning) = data ingestion, writes to PostgreSQL
- **Streamlit** (runs continuously) = reads from PostgreSQL, serves web dashboard at port 8501

## Data Sources

### Postgres Tables Used (by dashboard.py)
1. **`put_option_data`** - Contains strike, bid, ask, impliedVolatility, exp_date, as_of_date, ticker
2. **`stock_dim_data`** - Contains ticker, current_price, week_52_high, week_52_low, latest_close_date
3. **`current_holdings`** - Contains ticker, shares, avg_cost_basis, account_alias, as_of_date

### Tables Referenced in Dashboard
- Dashboard queries all three tables to display put candidates, filters by price discount and days to strike, and shows candidate summary with 52-week high/low data.

## Problems Identified

| # | Problem | Impact |
|---|---------|--------|
| 1 | `Dockerfile` is missing `streamlit` package | Cannot build image without streamlit |
| 2 | `Dockerfile.streamlit` is a backup, not used in production | Redundant file; main Dockerfile needs the dependency |
| 3 | `docker-compose.yml` references `Dockerfile.streamlit` | Streamlit service won't build from current Dockerfile |
| 4 | Dashboard DB connection may not use env vars | Hardcoded connections won't work in Docker |
| 5 | `Dockerfile.streamlit` depended on `FROM options_python_img:latest` | Unnecessary multi-layer build; can simplify |

## Implementation Steps

### Step 1: Add Streamlit to Main Dockerfile

**File**: `Dockerfile`

Add `streamlit` to the existing pip install block:

```dockerfile
RUN pip install --no-cache-dir \
    yfinance \
    gspread \
    oauth2client \
    gspread_dataframe \
    pandas \
    sqlalchemy \
    psycopg2-binary \
    streamlit          # ← new dependency
```

### Step 2: Update docker-compose.yml Streamlit Service

**File**: `docker-compose.yml`

Update the existing `streamlit-dashboard` service:

```yaml
  streamlit-dashboard:
    build:
      context: .
      dockerfile: Dockerfile        # Changed from Dockerfile.streamlit
    image: streamlit-dashboard:latest
    container_name: streamlit-dashboard
    ports:
      - "8501:8501"
    volumes:
      - ./scripts_for_flow/options:/app/scripts    # Updated path
    environment:
      - DATABASE_HOST=pgdatabase
      - DATABASE_PORT=5432
      - DATABASE_USER=root
      - DATABASE_PASSWORD=root
      - DATABASE_NAME=option_data
    depends_on:
      - pgdatabase
    restart: unless-stopped
```

Key changes:
- Build from `Dockerfile` instead of `Dockerfile.streamlit`
- Mount `./scripts_for_flow/options` (not just `scripts_for_flow`)
- Pass DB connection env vars
- Keep port mapping `8501:8501`

### Step 3: Verify/Update dashboard.py Database Connection

**File**: `scripts_for_flow/options/dashboard.py`

Ensure the dashboard:
1. Uses environment variables for DB connection (or falls back to hardcoded defaults)
2. Connects to `pgdatabase:5432` when running in Docker
3. Handles empty tables gracefully (shows "No data" message instead of crashing)

### Step 4: Clean Up (Optional)

**File**: `Dockerfile.streamlit`

After confirming the new setup works:
- Delete or archive `Dockerfile.streamlit` (it becomes redundant)

## Docker Services Summary (After Implementation)

| Service | Image | Port | Purpose |
|---|---|---|---|
| `pgdatabase` | `postgres:18` | 5432 | PostgreSQL database |
| `kestra` | `kestra/kestra:v1.1` | 8080 | Workflow orchestrator |
| `kestra_postgres` | `postgres:18` | (internal) | Kestra's own metadata DB |
| `streamlit-dashboard` | `streamlit-dashboard:latest` | **8501** | Web dashboard |
| `pgadmin` | `dpage/pgadmin4` | 8085 | Database admin UI |

## Dependencies

| Package | Status |
|---|---|
| `streamlit` | NEW - added to Dockerfile |
| `pandas` | Already present |
| `sqlalchemy` | Already present |
| `psycopg2-binary` | Already present |
| `yfinance` | Already present |
| `numpy` | NEW - required by dashboard.py |

Note: `numpy` is not currently in the Dockerfile but is imported by `dashboard.py`. It will be installed as a dependency of `streamlit` automatically, but should be added explicitly for clarity.

## Testing

1. Build and start all services:
   ```bash
   docker-compose up -d pgdatabase kestra streamlit-dashboard
   ```

2. Verify Postgres is running and has data:
   ```bash
   docker-compose exec pgdatabase psql -U root -d option_data -c "\dt"
   ```

3. Access dashboard at `http://localhost:8501`

4. Verify dashboard displays data from the database

## Risk Assessment

| Risk | Mitigation |
|---|---|
| Streamlit could break the Kestra pipeline | None - they are independent services sharing only a DB |
| New package installs could cause build issues | `streamlit` and `numpy` are well-established packages |
| Dashboard may fail if tables are empty | Add empty-state handling in dashboard.py |
| `Dockerfile.streamlit` removal could affect others | Document the change; it was already a backup file |

## Estimated Effort

- Step 1 (Dockerfile): 5 min
- Step 2 (docker-compose.yml): 10 min
- Step 3 (dashboard.py review): 10 min
- Step 4 (Cleanup): 5 min
- Testing: 10 min
- **Total: ~40 minutes**

## Files to Create/Modify

1. **Modify**: `Dockerfile` - Add `streamlit` and `numpy` to pip install
2. **Modify**: `docker-compose.yml` - Update `streamlit-dashboard` service configuration
3. **Review**: `scripts_for_flow/options/dashboard.py` - Verify DB connection and empty state handling
4. **Delete/Archive**: `Dockerfile.streamlit` - Redundant after implementation
