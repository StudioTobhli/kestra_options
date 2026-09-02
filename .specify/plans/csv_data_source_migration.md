# CSV Data Source Migration - Implementation Plan

## Overview

`gspread` has been unreliable recently. Two ingestion scripts depend on it purely to **read** static-ish reference data:

- `holdings_ingest.py` reads the "Select_Holdings" Google Sheet
- `put_data_ingest.py` reads the "Put_Candidates" Google Sheet

That data already exists as CSVs on disk at `~/Projects/kestra_options/src_files/` (`select_holdings.csv`, `put_candidates.csv`), matching the sheet schemas exactly. This plan cuts Google Sheets out of these two read paths and reads the CSVs directly instead.

**Delivery mechanism**: bind-mount `~/Projects/kestra_options/src_files` into the two Kestra tasks via `taskRunner.volumes`, rather than uploading the CSVs through the Kestra UI as namespace files (the way the Google credentials JSON is handled today). This means editing a CSV on disk is picked up on the next pipeline run automatically — no manual re-upload step.

**Scope**: only `holdings_ingest.py` and `put_data_ingest.py`. `put_to_sheets.py` still writes new candidates back into the Google Sheet "Put_Candidates" — the same sheet `put_data_ingest.py` currently reads — so after this change that write-back becomes functionally dead (nothing reads it anymore). Explicitly out of scope for this change; `put_candidates.csv` will be maintained by hand going forward. `call_to_sheets.py`, `call_data_ingest.py`, `stock_dim_ingest.py` (the latter two have an unused `import gspread` but never call it) are untouched.

## Complexity Verdict

Lopsided. The Python changes are small and low-risk — delete 4 lines of gspread auth/read per script, add a `pd.read_csv()` call plus one bit of currency-string cleanup. The real work and the only real unknown is Kestra-side: bind-mounting a host directory into the ephemeral per-task Docker containers that Kestra's Docker task runner spins up, which requires an explicit server-side opt-in that Kestra disables by default for security. Expect ~15 min for the script edits and up to an hour for the Kestra/Docker config, mostly because it may need one restart-and-retry cycle to get right.

## Architecture

```
Kestra task container (ephemeral, spun up per run)
  ├─ /app/src_files/select_holdings.csv   ◄── bind-mounted, read-only
  └─ /app/src_files/put_candidates.csv    ◄── from ~/Projects/kestra_options/src_files (host)

holdings_ingest.py  ──reads──►  /app/src_files/select_holdings.csv  ──writes──►  current_holdings (Postgres)
put_data_ingest.py  ──reads──►  /app/src_files/put_candidates.csv   ──writes──►  put_option_data  (Postgres)
```

No change to the downstream tables, schemas, or any other task in `ingestion_deploy.yml`.

## Implementation Steps

### Step 1: `scripts_for_flow/options/holdings_ingest.py`

Remove `import gspread` (line 4) and replace the auth/open/read block (lines 26-30):

Before:
```python
gc = gspread.service_account(filename='studiotlanalyticsSvcAccnt-a59159d08cb6.json')
sh = gc.open("Select_Holdings")
wksht = sh.get_worksheet(0)
holdings_df = pd.DataFrame(wksht.get_all_records())
```

After:
```python
HOLDINGS_CSV_PATH = os.environ.get('HOLDINGS_CSV_PATH', '/app/src_files/select_holdings.csv')
holdings_df = pd.read_csv(HOLDINGS_CSV_PATH, encoding='utf-8-sig')  # utf-8-sig strips the CSV's leading BOM

holdings_df['avg_cost_basis'] = (
    holdings_df['avg_cost_basis'].astype(str)
    .str.replace('$', '', regex=False)
    .str.replace(',', '', regex=False)
    .astype(float)
)
```
`avg_cost_basis` is formatted like `$124.29` in the CSV — gspread's `get_all_records()` used to auto-type this; `pd.read_csv` won't. `os` is already imported in this file.

### Step 2: `scripts_for_flow/options/put_data_ingest.py`

Remove `import gspread` (line 4) and replace the auth/open/read block (lines 17-21):

Before:
```python
gc = gspread.service_account(filename='studiotlanalyticsSvcAccnt-a59159d08cb6.json')
sh = gc.open("Put_Candidates")
wksht = sh.get_worksheet(0)
put_candidate_df = pd.DataFrame(wksht.get_all_records())
```

After:
```python
PUT_CANDIDATES_CSV_PATH = os.environ.get('PUT_CANDIDATES_CSV_PATH', '/app/src_files/put_candidates.csv')
put_candidate_df = pd.read_csv(PUT_CANDIDATES_CSV_PATH, encoding='utf-8-sig')
```
No currency cleanup needed — single ticker column; the rest of the script only ever indexes it via `.iloc[index, 0]`.

The `os.environ.get(..., default)` pattern in both scripts lets them be run directly on the host against the real CSV path for a quick sanity check, while defaulting to the in-container mount path Kestra will use.

### Step 3: `flows/options/ingestion_deploy.yml`

Add a `volumes:` entry under `taskRunner` for exactly the `put_data_ingest` and `holdings_ingest` tasks — no other task changes:

```yaml
    taskRunner:
      type: io.kestra.plugin.scripts.runner.docker.Docker
      image: options_python_img:latest
      networkMode: "kestra_options_default"
      volumes:
        - "/home/jedah/Projects/kestra_options/src_files:/app/src_files:ro"
```

Use the absolute host path, not `~` — dockerd won't expand it.

### Step 4: `docker-compose.yml`

Kestra's Docker task runner disables host volume mounts by default; enable it by adding a `plugins:` block inside `KESTRA_CONFIGURATION`, as a sibling of `server`/`repository`/`storage`/`queue`/`tasks`/`url`:

```yaml
          plugins:
            configurations:
              - type: io.kestra.plugin.scripts.runner.docker.Docker
                values:
                  volume-enabled: true
```

Requires restarting the `kestra` service (`docker-compose up -d kestra`) to take effect. **This is the highest-risk step** — if the exact config key differs on `kestra/kestra:v1.1`, the flow execution log (or Kestra startup log) will say so explicitly when a task tries to mount a volume with it disabled; that's the fallback diagnostic path.

Permissions note: `options_python_img:latest` has no `USER` set (inherits root from `python:3.11-slim`), and task containers are created directly by the host's dockerd (the `kestra` service mounts `/var/run/docker.sock`), so reading a directory owned by `jedah` (uid 1000) shouldn't hit permission issues — but worth a glance at logs on first run.

### Step 5: `Dockerfile` — no change

`gspread`, `oauth2client`, `gspread_dataframe` stay in the pip install list — `put_to_sheets.py` and `call_to_sheets.py` still depend on them.

### Step 6: `.gitignore` — optional, not required

The CSVs live outside the repo tree already (`~/Projects/kestra_options/src_files` vs. this repo at `~/kestra_options`), so there's no current exposure risk. Optionally add `src_files/` as a precaution in case they're ever copied in.

## Files to Create/Modify

1. **Modify**: `scripts_for_flow/options/holdings_ingest.py` — swap gspread read for `pd.read_csv`
2. **Modify**: `scripts_for_flow/options/put_data_ingest.py` — swap gspread read for `pd.read_csv`
3. **Modify**: `flows/options/ingestion_deploy.yml` — add `volumes:` to two tasks
4. **Modify**: `docker-compose.yml` — enable Docker task runner volume mounts
5. **Optional**: `.gitignore` — add `src_files/`

## Testing

1. **Local sanity check** (no Docker/Kestra) — run each script directly with the env var pointed at the real host CSV:
   ```bash
   HOLDINGS_CSV_PATH=/home/jedah/Projects/kestra_options/src_files/select_holdings.csv \
     python scripts_for_flow/options/holdings_ingest.py
   ```
   Confirms the BOM/currency parsing works before involving Kestra. Needs `pgdatabase` reachable (`docker-compose up -d pgdatabase`, adjust the engine hostname to `localhost` for a pure local run, or eyeball the dataframe before the `to_sql` call).

2. **Isolate the mount**:
   ```bash
   docker run --rm -v /home/jedah/Projects/kestra_options/src_files:/app/src_files:ro \
     options_python_img:latest \
     python -c "import pandas as pd; print(pd.read_csv('/app/src_files/select_holdings.csv', encoding='utf-8-sig'))"
   ```
   Confirms the path/permissions work independent of Kestra's volume-enablement config.

3. **Full flow test** — after both YAML changes and a `kestra` restart, trigger `ingestion_deploy` manually from the Kestra UI ("Execute") rather than waiting for the `0 7 * * 1-5` cron, and watch the `holdings_ingest`/`put_data_ingest` task logs.

4. **Data check** — confirm `current_holdings` (40 rows) and `put_option_data` (6 tickers) in pgAdmin at http://localhost:8085.

## Risk Assessment

| Risk | Mitigation |
|---|---|
| Kestra rejects/ignores `volume-enabled` config on v1.1 | Task log or Kestra startup log will name the problem explicitly; fall back to checking Kestra v1.1 docs for the exact key |
| Container can't read host-owned files (permission denied) | Container runs as root by default (no `USER` in Dockerfile); verify on first run |
| `avg_cost_basis` currency parsing edge cases (blank rows, stray commas) | Sanity-check step 1 catches this before touching Kestra |
| `put_to_sheets.py` write-back to "Put_Candidates" becomes silently dead | Explicitly accepted tradeoff; `put_candidates.csv` maintained by hand |

## Known Tradeoff (Explicitly Not Being Fixed)

`put_to_sheets.py` still writes new candidate tickers into the "Put_Candidates" Google Sheet, but `put_data_ingest.py` will no longer read that sheet — it reads `put_candidates.csv` instead. This makes `put_to_sheets.py`'s write-back functionally dead for the ingest pipeline. Out of scope for this change.

## Estimated Effort

- Step 1-2 (Python edits): 15 min
- Step 3 (flow YAML): 10 min
- Step 4 (docker-compose.yml + restart/debug cycle): 30-60 min
- Testing: 20 min
- **Total: ~1.5-2 hours**
