# Call Options Implementation Plan

## Overview

Create a Python script to identify call option candidates based on stock price history analysis and append a new task to the Kestra workflow.

## Data Sources

### Postgres Tables Used
1. **current_holdings** - Contains ticker, shares, avg_cost_basis, account_alias, as_of_date
2. **stock_hist_data** - Contains ticker, hist_date (timestamp), open, high, low, close, as_of_date
3. **call_option_data** - Contains strike, bid, ask, impliedVolatility, exp_date, as_of_date, ticker

## Implementation Steps

### Step 1: Create Python Script (call_leads.py)

Create `scripts_for_flow/call_leads.py` with the following logic:

1. **Database Connection**
   - Use SQLAlchemy with connection string: `postgresql://root:root@pgdatabase:5432/option_data`

2. **Fetch Holdings Data**
   ```python
   holdings_sql = "SELECT ticker, shares, avg_cost_basis FROM current_holdings"
   holdings_df = pd.read_sql_query(holdings_sql, con=engine)
   ```

3. **Inventory Check - Filter for shares >= 100**
   ```python
   holdings_df = holdings_df[holdings_df['shares'] >= 100]
   ```

4. **Fetch Stock History Data**
   ```python
   hist_sql = "SELECT ticker, hist_date, high, low FROM stock_hist_data"
   hist_df = pd.read_sql_query(hist_sql, con=engine)
   ```

5. **Filter Last 30 Days**
   - Convert `hist_date` to datetime if needed
   - Filter to last 30 days from current date

 6. **Fetch Call Option Data**

 7. **Identify Candidates & Lead Indicator**
    - For each ticker, calculate max(price) in last 30 days using `high` column
    - For each ticker in holdings, check if ANY available option meets BOTH criteria:
      - **Strike >= 90% of avg_cost_basis** (from call_option_data strike vs holdings avg_cost_basis)
      - **Max 30-day price <= 60% of avg_cost_basis** (from stock_hist_data vs holdings avg_cost_basis)
    - Create column `call_lead_ind`:
      - Set to `1` if both criteria are met
      - Set to `0` otherwise

 8. **Output DataFrame Structure**
   - ticker, shares, avg_cost_basis, max_30d_price, available_strike, bid, ask, exp_date, call_lead_ind

 9. **Final Filter - Only call_lead_ind == 1**
   ```python
   output_df = output_df[output_df['call_lead_ind'] == 1]
   ```

10. **Write to Postgres**
   - Table name: `call_leads`
   - Use column type dict for explicit typing
   - Use `if_exists='replace'` to create/update table

### Step 2: Update Kestra YAML

Append new task to `flows/ingestion_deploy.yml`:

```yaml
  - id: call_leads
    type: io.kestra.plugin.scripts.python.Commands
    namespaceFiles:
      enabled: true
    taskRunner:
      type: io.kestra.plugin.scripts.runner.docker.Docker
      image: options_python_img:latest
      networkMode: "kestra_options_default"
    containerImage: ghcr.io/kestra-io/pydata:latest
    commands:
      - python call_leads.py
```

## Dependencies

- pandas
- sqlalchemy
- psycopg2-binary (already in Dockerfile)
- No new dependencies required

## Testing

1. Run script locally with PostgreSQL running:
   ```bash
   python scripts_for_flow/call_leads.py
   ```

2. Verify output in pgAdmin:
   - Check `call_leads` table exists
   - Verify data in table

## Task Dependencies

The `call_leads` task should run after:
- `holdings_ingest` - ensures current_holdings has data
- `stock_hist_ingest` - ensures stock_hist_data has data  
- `call_data_ingest` - ensures call_option_data has data

## Files to Create/Modify

1. **Create**: `scripts_for_flow/call_leads.py`
2. **Modify**: `flows/ingestion_deploy.yml` (append new task)

## Estimated Effort

- Script development: 1-2 hours
- Testing: 1 hour
- Integration: 30 minutes
