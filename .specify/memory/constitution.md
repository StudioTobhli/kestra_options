# Kestra Options Constitution

## Core Principles

### I. Source: Prioritize Existing Postgres Tables
Always prioritize existing Postgres tables as the data source. Only use external APIs (e.g., yfinance, Google Sheets) after confirming the data is not already available locally. Ask the user before introducing new external API calls.

### II. Logic: Use Pandas for Data Manipulation
Use pandas for all data manipulation. Maintain pandas DataFrames as the intermediate structure before writing to Google Sheets or Postgres. This ensures data consistency and allows for easy debugging and transformation.

### III. Files: Use Python for Dataset Generation
Use Python (.py) files for all dataset generation tasks. Store scripts in `scripts_for_flow/` directory. Python scripts should be self-contained and executable both locally and within the Kestra Docker environment.

### IV. Orchestration: Append to Existing Kestra YAML
When adding new tasks to the Kestra workflow:
- Append new tasks to the end of `flows/ingestion_deploy.yml`
- Mirror the parameters of existing tasks like 'put_leads' and 'put_to_sheets'
- Use the same task structure:
  ```yaml
  - id: [task_name]
    type: io.kestra.plugin.scripts.python.Commands
    namespaceFiles:
      enabled: true
    taskRunner:
      type: io.kestra.plugin.scripts.runner.docker.Docker
      image: options_python_img:latest
      networkMode: "kestra_options_default"
    containerImage: ghcr.io/kestra-io/pydata:latest
    commands:
      - python [script_name].py
  ```

## Development Workflow

### Database-First Approach
1. Query existing Postgres tables before fetching external data
2. Document any new external data sources in script comments
3. Use SQLAlchemy with explicit column types for all writes

### Error Handling
- Return safe defaults (None/empty DataFrame) for non-critical failures
- Log informative error messages with context
- Catch specific exceptions when possible

### Testing
- Test scripts individually with `docker-compose run`
- Verify data in PostgreSQL via pgAdmin at http://localhost:8085
- Use Jupyter notebooks in `dev_notebooks/` for exploratory testing

## Governance

All changes must comply with these principles. New tasks added to Kestra flows must follow the YAML structure defined in Section IV.

**Version**: 1.0 | **Ratified**: 2026-03-01
