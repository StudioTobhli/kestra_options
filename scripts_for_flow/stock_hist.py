import pandas as pd
import os
import yfinance as yf
from datetime import datetime
from time import time
# Use SQLAlchemy to create a connection to postgres
from sqlalchemy import create_engine
from sqlalchemy.types import Float
from sqlalchemy.types import Date
from sqlalchemy.types import DateTime
from sqlalchemy.types import VARCHAR


# ### General Logic
# - Gather all tickers in stock dim table
# - Loop through each ticker and append history to a dataset
# - Columns:
#   - ticker
#   - Date
#   - Open, High, Low, Close
#   - As of date (today)

# Use postgres docker service name when running this in Kestra docker container
# engine = create_engine('postgresql://root:root@localhost:5432/option_data')
engine = create_engine('postgresql://root:root@pgdatabase:5432/option_data')

# Get list of tickers.  Pull everything from stock dim table
# This will contain all tickers of interest w/ valid yfinance data
stock_dim_sql = "select * from stock_dim_data"

stock_dim_df = pd.read_sql_query(stock_dim_sql, con=engine)


# Create empty list to hold ticker history
history_list = []

for ticker_symbol in stock_dim_df['ticker']:
    try:
        ticker = yf.Ticker(ticker_symbol)
        hist = ticker.history(period="1mo")
        
        if not hist.empty:
            # Reset index to make 'Date' a column
            hist = hist.reset_index()
            
            # Add ticker column
            hist['ticker'] = ticker_symbol
            
            # Select only the columns we want
            hist = hist[['ticker', 'Date', 'Open', 'High', 'Low', 'Close']]
            
            # Rename columns to lowercase
            hist.columns = ['ticker', 'hist_date', 'open', 'high', 'low', 'close']
            
            history_list.append(hist)
    
    except Exception as e:
        print(f"Error fetching history for {ticker_symbol}: {e}")


stock_hist_df = pd.concat(history_list, ignore_index=True)

# Add today's date to label today's pull
stock_hist_df['as_of_date'] = datetime.now()

# Specify column types
column_typ_dict = {
    'ticker' : VARCHAR(20),
    'hist_date' : DateTime(),
    'open' : Float(),
    'high' : Float(),
    'low' : Float(),
    'close' : Float(),
    'as_of_date' : DateTime()
}

# Write to postgres table
stock_hist_df.to_sql('stock_hist_data', con=engine, dtype=column_typ_dict, if_exists='replace', index=False)











curr_ticker_str = 'AMD'
curr_ticker = yf.Ticker(curr_ticker_str)


hist = curr_ticker.history(period="5d")


hist




