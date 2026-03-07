# Tasks: Call Options Implementation

**Input**: `.specify/plans/call_options_implementation.md`
**Prerequisites**: Implementation plan (completed)

---

## Phase 1: Create Python Script

**Purpose**: Implement the call_leads.py script with the updated logic

- [ ] T001 Review existing Python scripts in `scripts_for_flow/` for code style and patterns
- [ ] T002 Create `scripts_for_flow/call_leads.py` with database connection
- [ ] T003 Implement Step 2: Fetch holdings data with shares >= 100 filter
- [ ] T004 Implement Step 3-5: Fetch stock history, filter 30 days, calculate max price
- [ ] T005 Implement Step 6: Fetch call options data
- [ ] T006 Implement Step 7: Identify candidates and lead indicator
- [ ] T007 Implement Step 8: Output dataframe structure
- [ ] T008 Implement Step 9: Final filter (call_lead_ind == 1)
- [ ] T009 Implement Step 10: Write to Postgres call_leads table
- [ ] T010 Test script locally with PostgreSQL running

**Checkpoint**: Script runs successfully and outputs to call_leads table

---

## Phase 2: Update Kestra Workflow

**Purpose**: Add call_leads task to the ingestion flow

- [ ] T011 Review `flows/ingestion_deploy.yml` structure
- [ ] T012 Add call_leads task to ingestion_deploy.yml
- [ ] T013 Configure task dependencies (run after holdings, stock_hist, call_data)

**Checkpoint**: YAML is valid and task is ready for deployment

---

## Phase 3: Integration Testing

**Purpose**: Verify end-to-end flow works

- [ ] T014 Deploy updated flow to Kestra
- [ ] T015 Execute flow and verify call_leads task runs
- [ ] T016 Verify data in call_leads table via pgAdmin

---

## Dependencies & Execution Order

- **Phase 1**: Sequential - each step builds on the previous
- **Phase 2**: Can start after T010 (script works)
- **Phase 3**: Can start after Phase 2 complete

---

## Notes

- Parallel opportunities: T001 can run while reviewing other scripts
- Reference `scripts_for_flow/put_leads.py` for similar implementation patterns
- Use SQLAlchemy connection: `postgresql://root:root@pgdatabase:5432/option_data`
