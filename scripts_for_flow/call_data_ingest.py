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


# #### General Logic
# * Pull holdings data from postgres
# * Convert holdings into a unique list or series of tickers
# * Loop through unique tickers and do the below
# * Get list of option expiration dates
# * Sample one date and create an empty dataframe using head(0)
# * For each ticker and expiration date, append the below fields to the temp dataframe
# >*  Strike
# >*  Bid
# >*  Ask
# >* IV
# * Add following fields:
# >* Expiration Date
# >* Current datetime
# >* Ticker
# * Put above in temporary dataframe, then append to main dataframe

# ### Create connection to postgres
# #### Pull holdings from postgres table to get list of call candidates

# Use postgres docker service name when running this in Kestra docker container
# engine = create_engine('postgresql://root:root@localhost:5432/option_data')
engine = create_engine('postgresql://root:root@pgdatabase:5432/option_data')


holdings_sql = """
    select
    *
    from current_holdings
"""


call_candidate_df = pd.read_sql_query(holdings_sql, con=engine)

# Start with grabbing first ticker to get information to build initial dataframe
# curr_ticker_str = call_candidate_df.iloc[0,0]
curr_ticker_str = call_candidate_df['ticker'].iloc[0]


curr_ticker = yf.Ticker(curr_ticker_str)


# returns a tuple of expiration dates for the given ticker
exp_dates = curr_ticker.options


# create a tuple with just the first two items in expiration date tuple for testing
# We will loop through the entire thing if this works
# delete this later so we have the rest of the tuple
exp_dates = exp_dates[0:3]


curr_exp_dt = exp_dates[0]


option_chain_curr = curr_ticker.option_chain(curr_exp_dt)


call_data_df = option_chain_curr.calls


# Create empty dataframe for the call data set
call_data_df = call_data_df.copy()[['strike', 'bid', 'ask', 'impliedVolatility']].head(0)


# Add expiration date, current date, and ticker to dataframe as blank columns
call_data_df['exp_date'] = pd.Series()
call_data_df['as_of_date'] = pd.Series()
call_data_df['ticker'] = pd.Series()


print("Current put_data_df length: {}".format(len(call_data_df)))


# Iterate through the tickers in put candidates
for index, row in call_candidate_df.iterrows():  
    curr_ticker_str = call_candidate_df['ticker'].iloc[index]
    curr_ticker = yf.Ticker(curr_ticker_str)

    print("Pulling option data for: " + curr_ticker_str)

    # returns a tuple of expiration dates for the given ticker
    exp_dates = curr_ticker.options
    # delete this later so we have the rest of the tuple
    exp_dates = exp_dates[0:3]

    # For each expiration, add to the main dataframe
    for i in exp_dates:
        print("Adding call data for ticker: {}, expiration date: {}".format(curr_ticker_str, i))
        option_chain_curr = curr_ticker.option_chain(i)
        curr_call_data_df = option_chain_curr.calls.copy()[['strike', 'bid', 'ask', 'impliedVolatility']]

        # Add expiration date, current date, and ticker to dataframe
        # Expiration Dates coming from options attribute are strings.  Convert to date before storing it in final dataframe
        curr_exp_dt = datetime.strptime(i, "%Y-%m-%d").date()
        curr_call_data_df['exp_date'] = curr_exp_dt
        curr_call_data_df['as_of_date'] = datetime.now()
        curr_call_data_df['ticker'] = curr_ticker_str

        # append to main dataframe
        call_data_df = pd.concat([call_data_df, curr_call_data_df])

        print("Current call_data_df length: {}".format(len(call_data_df)))    


# Specify the date types.  SQLAlchemy with to_sql doesn't choose the right date types by default
column_typ_dict = {
    'strike' : Float(),
    'bid' : Float(),
    'ask' : Float(),
    'impliedVolatility' : Float(),
    'exp_date' : Date(),
    'as_of_date' : DateTime(),
    'ticker' : VARCHAR(20)
}


# #### Write call option data to postgres table

# Start with creating a table
# Only write the first row without any data.  This creates the table with no data
# not sure if this will work, or if I need to create schema first
call_data_df.head(n=0).to_sql(name='call_option_data', con=engine, dtype=column_typ_dict, if_exists='replace', index=False)


# write table to postgres
t_start = time()
call_data_df.to_sql(name='call_option_data', con=engine, if_exists='append', index=False)
t_end = time()
print('Finished inserting call data.  Took %.3f seconds' % (t_end - t_start))




