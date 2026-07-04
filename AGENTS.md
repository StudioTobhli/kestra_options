# AGENTS.md - Agentic Coding Guidelines

This project is a Kestra-based workflow orchestration system for options trading data pipelines. It uses Python scripts with PostgreSQL database and Docker. The repo is structured as a monorepo to support multiple pipelines; each pipeline gets its own subdirectory under `flows/` and `scripts_for_flow/`.

## Project Structure

```
/home/jedah/kestra_options
├── docker-compose.yml              # Main orchestration (Kestra, PostgreSQL, pgadmin, Streamlit)
├── Dockerfile                      # Python pipeline image
├── Dockerfile.streamlit            # Streamlit dashboard image (inactive — dashboard moved to Looker Studio)
├── flows/
│   ├── options/                    # Kestra flow YAMLs for the options pipeline
│   │   ├── ingestion_deploy.yml
│   │   └── analysis_deploy.yml
│   └── jobs/                       # Placeholder for future jobs pipeline flows
├── scripts_for_flow/
│   ├── options/                    # Python scripts executed by the options pipeline flows
│   │   ├── holdings_ingest.py
│   │   ├── stock_dim_ingest.py
│   │   ├── put_data_ingest.py
│   │   ├── call_data_ingest.py
│   │   ├── stock_hist.py
│   │   ├── put_leads.py
│   │   ├── call_leads.py
│   │   ├── put_to_sheets.py
│   │   └── call_to_sheets.py
│   └── jobs/                       # Placeholder for future jobs pipeline scripts
└── dev_notebooks/                  # Jupyter notebooks for development
```

---

## 1. Build/Lint/Test Commands

### Docker Development Environment

```bash
# Start all services
docker-compose up -d

# Build images
docker-compose build

# View logs
docker-compose logs -f [service_name]

# Stop all services
docker-compose down
```

### Running Individual Services

```bash
# PostgreSQL (port 5432)
docker-compose up -d pgdatabase

# Kestra (port 8080)
docker-compose up -d kestra

# Streamlit Dashboard (port 8501)
docker-compose up -d streamlit-dashboard

# pgAdmin (port 8085)
docker-compose up -d pgadmin
```

### Python Scripts

```bash
# Run a script locally (requires PostgreSQL running)
python scripts_for_flow/options/holdings_ingest.py

# Run with Docker
docker-compose run --rm python-pipeline python scripts_for_flow/options/holdings_ingest.py
```

### Testing

This project does not have formal unit tests. For manual testing:
- Use Jupyter notebooks in `dev_notebooks/` for exploratory testing
- Test scripts individually via `docker-compose run`
- Verify data in PostgreSQL via pgAdmin at http://localhost:8085

---

## 2. Code Style Guidelines

### General Conventions

- **Language**: Python 3.11+
- **Encoding**: UTF-8 (include `#!/usr/bin/env python` and `# coding: utf-8` headers)
- **Line Length**: No strict limit, but keep lines readable (under 120 chars preferred)
- **Indentation**: 4 spaces (no tabs)

### Imports

```python
# Standard library first
import os
import pandas as pd
from datetime import datetime
from time import time

# Third-party libraries
import yfinance as yf
import gspread
from sqlalchemy import create_engine
from sqlalchemy.types import Float, Integer, Date, DateTime, VARCHAR
```

- Group imports: stdlib, third-party, local
- Use explicit imports (no `from x import *`)
- One import per line

### Naming Conventions

| Element | Convention | Example |
|---------|------------|---------|
| Variables | snake_case | `holdings_df`, `ticker_symbol` |
| Functions | snake_case | `def get_stock_info(ticker_symbol):` |
| Constants | UPPER_SNAKE_CASE | `MAX_RETRIES`, `DEFAULT_TIMEOUT` |
| Classes | PascalCase | `DataIngestor` (if used) |
| Files | snake_case | `holdings_ingest.py`, `stock_dim_ingest.py` |

### Type Hints

Not currently enforced, but prefer adding type hints for function signatures:

```python
def get_stock_info(ticker_symbol: str) -> dict:
    """Fetch stock information for a given ticker."""
    try:
        # ...
    except Exception as e:
        print(f"Error fetching data for {ticker_symbol}: {e}")
        return {
            'ticker': ticker_symbol,
            'current_price': None,
            # ...
        }
```

### Database Connection

```python
# Use SQLAlchemy create_engine
# Use Docker service name for container-to-container communication
engine = create_engine('postgresql://root:root@pgdatabase:5432/option_data')

# For local development (when not in Docker)
# engine = create_engine('postgresql://root:root@localhost:5432/option_data')
```

- Always specify column types when writing to PostgreSQL with `to_sql()`
- Use `if_exists='replace'` for table creation, `'append'` for data insertion

### Error Handling

```python
try:
    # operation that may fail
except Exception as e:
    print(f"Error message: {e}")
    # Return safe defaults or re-raise if critical
```

- Catch specific exceptions when possible
- Log errors with informative messages
- Return gracefully with default/null values for non-critical failures

### SQL Queries

```python
# Use triple-quoted strings for multi-line SQL
holdings_sql = """
    SELECT
        ticker,
        shares,
        avg_cost_basis
    FROM current_holdings
"""

df = pd.read_sql_query(holdings_sql, con=engine)
```

### DataFrame Operations

- Use method chaining where readable
- Explicitly name columns when creating DataFrames
- Use `.reset_index(drop=True)` after filtering

```python
stock_dim_df = stock_dim_df[
    stock_dim_df['current_price'].notna()
].reset_index(drop=True)
```

### Kestra Flow Development

- Flow files are YAML in `flows/options/` (or `flows/jobs/` for the jobs pipeline)
- Use descriptive task IDs
- Include namespace: `company.team`
- Set `namespaceFiles.enabled: true` for Python tasks
- Use correct Docker network: `kestra_options_default`

### Working with Google Sheets

```python
# Service account authentication
gc = gspread.service_account(filename='[google_credentials].json')
sh = gc.open("Sheet_Name")
wksht = sh.get_worksheet(0)
df = pd.DataFrame(wksht.get_all_records())
```

### Best Practices

1. **Secrets**: Never commit credentials to git. Use environment variables or Kestra secrets
2. **Timing**: Use `time()` to measure operation duration for logging
3. **Comments**: Use markdown-style comments for complex logic blocks
4. **Testing**: Test each script individually before deploying to Kestra
5. **Dependencies**: Document new dependencies in Dockerfile

---

## 3. Common Development Tasks

### Adding a New Ingestion Script

1. Create `scripts_for_flow/options/new_script.py` (or `scripts_for_flow/jobs/` for the jobs pipeline)
2. Add dependencies to `Dockerfile` if needed
3. Test locally with PostgreSQL running
4. Add as task to relevant flow in `flows/options/`

### Modifying Kestra Flows

1. Edit YAML file in `flows/options/`
2. Deploy via Kestra UI or push to git and sync namespace
3. Test with "Execute" button in Kestra UI

### Database Schema Changes

1. Connect to pgAdmin at http://localhost:8085
2. Navigate to option_data database
3. Modify table directly or update script logic to recreate table
