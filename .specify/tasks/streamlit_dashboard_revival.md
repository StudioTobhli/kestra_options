# Streamlit Dashboard Revival - Task List

## Branch: `streamlit-dashboard-revival`

---

## T001 [P] Add streamlit and numpy to Dockerfile

**File**: `Dockerfile`
**Time**: 5 min

Add `streamlit` and `numpy` to the existing pip install block:

```dockerfile
RUN pip install --no-cache-dir \
    yfinance \
    gspread \
    oauth2client \
    gspread_dataframe \
    pandas \
    sqlalchemy \
    psycopg2-binary \
    streamlit \
    numpy
```

**Verification**: `docker build` succeeds with new packages

---

## T002 [P] Update docker-compose.yml streamlit-dashboard service

**File**: `docker-compose.yml`
**Time**: 10 min

Update the existing `streamlit-dashboard` service:
- Change `dockerfile: Dockerfile.streamlit` → `dockerfile: Dockerfile`
- Update volume mount: `./scripts_for_flow` → `./scripts_for_flow/options`
- Add DB environment variables: `DATABASE_HOST`, `DATABASE_PORT`, `DATABASE_USER`, `DATABASE_PASSWORD`, `DATABASE_NAME`
- Keep port mapping `8501:8501`
- Keep `depends_on: pgdatabase`
- Keep `restart: unless-stopped`

**Verification**: `docker-compose config` validates without errors

---

## T003 [P] Review and update dashboard.py DB connection

**File**: `scripts_for_flow/options/dashboard.py`
**Time**: 10 min

Ensure the dashboard:
1. Uses environment variables for DB connection with fallback defaults
2. Connects to `pgdatabase:5432` when running in Docker
3. Handles empty tables gracefully (no crash on missing data)

Check for:
- Hardcoded connection strings that should use env vars
- Missing error handling for empty query results
- `numpy` import availability (will come from streamlit dependency)

**Verification**: Dashboard loads without errors in Docker

---

## T004 [P] Test the full setup

**Time**: 10 min

```bash
# 1. Build and start services
docker-compose up -d pgdatabase kestra streamlit-dashboard

# 2. Verify Postgres is running
docker-compose exec pgdatabase psql -U root -d option_data -c "\dt"

# 3. Run data pipelines to populate tables (if needed)
#    Trigger Kestra workflow or run ingest scripts manually

# 4. Access dashboard
#    Open http://localhost:8501

# 5. Verify dashboard displays put candidates
```

**Verification**: Dashboard accessible at port 8501 and shows data

---

## T005 [P] Clean up - Archive Dockerfile.streamlit

**File**: `Dockerfile.streamlit`
**Time**: 5 min

After confirming the new setup works:
- Archive or delete `Dockerfile.streamlit` (redundant after Step 1)

**Verification**: No references to `Dockerfile.streamlit` remain

---

## Execution Order

All tasks T001-T004 are independent (`[P]` flag) and can be done in parallel:

```bash
# Parallel execution:
Task: T001 (Dockerfile)
Task: T002 (docker-compose.yml)
Task: T003 (dashboard.py review)

# Sequential:
Task: T004 (Testing - depends on T001-T003)
Task: T005 (Cleanup - depends on T004)
```

---

## Checkpoints

- **[T001]**: Dockerfile builds successfully with new packages
- **[T002]**: docker-compose.yml validates, no syntax errors
- **[T003]**: dashboard.py imports work, DB connection logic reviewed
- **[T004]**: Dashboard running at http://localhost:8501 with data displayed
- **[T005]**: Dockerfile.streamlit removed, no broken references

---

## Notes

- **No risk to existing Kestra pipeline** - Streamlit is a read-only consumer of the database
- **Numpy note**: `numpy` is not explicitly in the current Dockerfile but is a dependency of `streamlit`. Added explicitly for clarity.
- **Dockerfile.streamlit**: This was a backup file created when the dashboard was first implemented. The main Dockerfile now has all other dependencies; we just need to add streamlit.
- **Kestra is NOT involved** in running the dashboard - Kestra runs the data pipeline every morning, Streamlit reads from the DB continuously.
