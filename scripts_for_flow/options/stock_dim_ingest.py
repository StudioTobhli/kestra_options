#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import os
import yfinance as yf
import gspread
from datetime import datetime
from time import time
# Use SQLAlchemy to create a connection to postgres
from sqlalchemy import create_engine
from sqlalchemy.types import Float
from sqlalchemy.types import Date
from sqlalchemy.types import DateTime
from sqlalchemy.types import VARCHAR


os.getcwd()


# #### General Logic
# *  Get list of tickers that we need stock dimension data for
# >* Pull distinct tickers from holdings data from postgres
# >* Pull distinct tickers from put candidates data in postgres
# # Create empty dataframe to stock dim data
# * Loop through unique tickers and do the below
# * Pull below fields from API
# >*  Ticker
# >*  Current Price
# >*  52 week high
# >*  52 week low
# >*  Current Date (API or python today's date)
# * Add following fields:
# >* Expiration Date
# >* Current datetime
# >* Ticker
# * Put above in temporary dataframe, then append to main dataframe

# ### Get holdings tickers

# Use postgres docker service name when running this in Kestra docker container
# engine = create_engine('postgresql://root:root@localhost:5432/option_data')
engine = create_engine('postgresql://root:root@pgdatabase:5432/option_data')


holdings_sql = """
    select
    *
    from current_holdings
"""


holdings_df = pd.read_sql_query(holdings_sql, con=engine)


put_sql = """
select * from put_option_data
"""

put_df = pd.read_sql_query(put_sql, con=engine)


# Create unique set of tickers
unique_tickers = pd.concat([holdings_df['ticker'], put_df['ticker']]).unique()


# Function to get stock info
# Returns dictionary with ticker info
# Provides blank info for ticker if there is an error
def get_stock_info(ticker_symbol):
    try:
        ticker = yf.Ticker(ticker_symbol)
        info = ticker.info

        # Get the most recent closing date from historical data
        hist = ticker.history(period="5d")  # Get last 5 days to ensure we have data
        latest_close_date = hist.index[-1].to_pydatetime() if not hist.empty else None
                
        return {
            'ticker': ticker_symbol,
            'current_price': info.get('currentPrice') or info.get('regularMarketPrice'),
            'week_52_high': info.get('fiftyTwoWeekHigh'),
            'week_52_low': info.get('fiftyTwoWeekLow'),
            'latest_close_date': latest_close_date
        }
    except Exception as e:
        print(f"Error fetching data for {ticker_symbol}: {e}")
        return {
            'ticker': ticker_symbol,
            'current_price': None,
            'week_52_high': None,
            'week_52_low': None,
            'latest_close_date': datetime.today()
        }


# Loop through tickers and collect data
data_list = []
for ticker in unique_tickers:
    stock_data = get_stock_info(ticker)
    data_list.append(stock_data)

# Create the final dataframe
stock_dim_df = pd.DataFrame(data_list)

# Keep only records with valid current price (iow did not trigger data fetch error)
# - Only worried about current price for now.  But, use the below to cut out records w/ missing 52-week high/lows
# stock_dim_df = stock_dim_df.dropna(subset=['current_price', 'week_52_high', 'week_52_low']).reset_index(drop=True)
stock_dim_df = stock_dim_df[stock_dim_df['current_price'].notna()].reset_index(drop=True)


# Create table_schema
# Specify the date types.  SQLAlchemy with to_sql doesn't choose the right date types by default
column_typ_dict = {
    'ticker' : VARCHAR(20),
    'current_price' : Float(),
    'week_52_high' : Float(),
    'week_52_low' : Float(),
    'latest_close_date' : DateTime()
}


# Start with creating a table
# Only write the first row without any data.  This creates the table with no data
# not sure if this will work, or if I need to create schema first
stock_dim_df.head(n=0).to_sql(name='stock_dim_data', con=engine, dtype=column_typ_dict, if_exists='replace', index=False)

# write table to postgres
t_start = time()
stock_dim_df.to_sql(name='stock_dim_data', con=engine, if_exists='append', index=False)
t_end = time()
print('Finished inserting call data.  Took %.3f seconds' % (t_end - t_start))

