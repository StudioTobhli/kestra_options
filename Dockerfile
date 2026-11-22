FROM python:3.11-slim

# Install system dependencies needed for some Python packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python packages
RUN pip install --no-cache-dir \
    yfinance \
    gspread \
    oauth2client \
    gspread_dataframe \
    pandas \
    "sqlalchemy<2.1" \
    psycopg2-binary
