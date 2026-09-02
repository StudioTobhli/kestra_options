# CSV Data Source Migration - Task List

## Branch: `dev/gsheets-to-csv-source`

---

## T001 [P] Swap holdings_ingest.py to read from CSV

**File**: `scripts_for_flow/options/holdings_ingest.py`
**Time**: 10 min

- Remove `import gspread` (line 4)
- Replace the auth/open/read block (lines 26-30) with:
  ```python
  HOLDINGS_CSV_PATH = os.environ.get('HOLDINGS_CSV_PATH', '/app/src_files/select_holdings.csv')
  holdings_df = pd.read_csv(HOLDINGS_CSV_PATH, encoding='utf-8-sig')

  holdings_df['avg_cost_basis'] = (
      holdings_df['avg_cost_basis'].astype(str)
      .str.replace('$', '', regex=False)
      .str.replace(',', '', regex=False)
      .astype(float)
  )
  ```

**Verification**: `HOLDINGS_CSV_PATH=/home/jedah/Projects/kestra_options/src_files/select_holdings.csv python scripts_for_flow/options/holdings_ingest.py` runs without error and `avg_cost_basis` prints as float, not string

---

## T002 [P] Swap put_data_ingest.py to read from CSV

**File**: `scripts_for_flow/options/put_data_ingest.py`
**Time**: 5 min

- Remove `import gspread` (line 4)
- Replace the auth/open/read block (lines 17-21) with:
  ```python
  PUT_CANDIDATES_CSV_PATH = os.environ.get('PUT_CANDIDATES_CSV_PATH', '/app/src_files/put_candidates.csv')
  put_candidate_df = pd.read_csv(PUT_CANDIDATES_CSV_PATH, encoding='utf-8-sig')
  ```

**Verification**: `PUT_CANDIDATES_CSV_PATH=/home/jedah/Projects/kestra_options/src_files/put_candidates.csv python scripts_for_flow/options/put_data_ingest.py` runs and processes all 6 tickers

---

## T003 Add taskRunner volume mount to the two ingestion tasks

**File**: `flows/options/ingestion_deploy.yml`
**Time**: 10 min
**Depends on**: T001, T002 (so the flow references scripts that already expect the mount path)

Add to `taskRunner` under both the `put_data_ingest` task and the `holdings_ingest` task (no other tasks change):
```yaml
      volumes:
        - "/home/jedah/Projects/kestra_options/src_files:/app/src_files:ro"
```

**Verification**: YAML is valid (`docker-compose config` won't check this file directly — visually confirm indentation matches the existing `taskRunner` block structure)

---

## T004 Enable Docker task runner volume mounts in Kestra config

**File**: `docker-compose.yml`
**Time**: 10 min

Add inside `KESTRA_CONFIGURATION`, as a sibling of `server`/`repository`/`storage`/`queue`/`tasks`/`url`:
```yaml
          plugins:
            configurations:
              - type: io.kestra.plugin.scripts.runner.docker.Docker
                values:
                  volume-enabled: true
```

Then restart: `docker-compose up -d kestra`

**Verification**: `docker-compose logs kestra` shows no config parse errors on startup

---

## T005 Isolate and test the bind mount directly

**Time**: 10 min
**Depends on**: T004

```bash
docker run --rm -v /home/jedah/Projects/kestra_options/src_files:/app/src_files:ro \
  options_python_img:latest \
  python -c "import pandas as pd; print(pd.read_csv('/app/src_files/select_holdings.csv', encoding='utf-8-sig'))"
```

**Verification**: Prints the holdings dataframe with no `FileNotFoundError` or `PermissionError`

---

## T006 Full Kestra flow test

**Time**: 15 min
**Depends on**: T001-T005

1. Open Kestra UI, navigate to `ingestion_deploy` flow
2. Trigger manually via "Execute" (don't wait for the `0 7 * * 1-5` cron)
3. Watch `holdings_ingest` and `put_data_ingest` task logs specifically
   - If volumes are disabled: expect an explicit Kestra error before the Python code runs
   - If the CSV path is wrong: expect a Python `FileNotFoundError`

**Verification**: Both tasks complete successfully

---

## T007 Verify data in Postgres

**Time**: 5 min
**Depends on**: T006

Check via pgAdmin at http://localhost:8085:
- `current_holdings` has 40 rows matching `select_holdings.csv`
- `put_option_data` was populated for all 6 tickers in `put_candidates.csv`

**Verification**: Row counts and ticker values match the source CSVs

---

## T008 [P] Optional: add src_files/ to .gitignore

**File**: `.gitignore`
**Time**: 2 min

Precautionary only — the CSVs live outside the repo tree already. Add `src_files/` in case they're ever copied in.

**Verification**: N/A (defensive, no functional check)

---

## Execution Order

```
T001, T002        → independent, can run in parallel
T003              → depends on T001, T002
T004              → independent, can run in parallel with T001-T003
T005              → depends on T004
T006              → depends on T003, T004, T005
T007              → depends on T006
T008              → independent, can run any time
```

---

## Checkpoints

- **[T001-T002]**: Both scripts run standalone against the real host CSVs and produce correctly-typed dataframes
- **[T003-T004]**: Flow YAML and docker-compose config updated, `kestra` restarted cleanly
- **[T005]**: Bind mount confirmed working in isolation, independent of Kestra
- **[T006-T007]**: Full pipeline run via Kestra UI populates `current_holdings` and `put_option_data` correctly

---

## Notes

- **Known tradeoff, not fixed here**: `put_to_sheets.py` still writes new candidates back into the "Put_Candidates" Google Sheet, which nothing reads anymore after this change. `put_candidates.csv` will need to be maintained by hand.
- **Highest-risk step is T004**: Kestra's Docker task runner disables host volume mounts by default; the exact config key may need adjustment for `kestra/kestra:v1.1` if the documented one doesn't take effect — check task/startup logs for an explicit error.
- **No Dockerfile change**: `gspread`/`oauth2client`/`gspread_dataframe` stay installed — `put_to_sheets.py` and `call_to_sheets.py` still need them.
- See `.specify/plans/csv_data_source_migration.md` for full context and rationale.
