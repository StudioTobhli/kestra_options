import pandas as pd
import os
import yfinance as yf
import gspread
from datetime import datetime
from time import time
import pytz
# Use SQLAlchemy to create a connection to postgres
from sqlalchemy import create_engine
from sqlalchemy.types import Float
from sqlalchemy.types import Date
from sqlalchemy.types import DateTime
from sqlalchemy.types import VARCHAR

os.getcwd()

# Connect to google sheet that contains all potential tickers we want to look out for a put
gc = gspread.service_account(filename='TLAnalyticsSVCAccnt.json')
sh = gc.open("Put_Candidates")
wksht = sh.get_worksheet(0)
put_candidate_df = pd.DataFrame(wksht.get_all_records())


# ### Test single ticker from Put candidates
# #### General Logic:
# * Sample a ticker from the put candiates csv
# * Get list of option expiration dates
# * Sample one date and create an empty dataframe using head(0)
# * Loop through all tickers and do the below
# * For the given ticker, loop through expiration dates pulling or appending the following
# * Pull following option metadata for given ticker and expiration date
# >*  Strike
# >*  Bid
# >*  Ask
# >* IV
# * Add following fields:
# >* Expiration Date
# >* Current datetime
# >* Ticker
# * Put above in temporary dataframe, then append to main dataframe


# Start with grabbing first ticker to get information to build initial dataframe
curr_ticker_str = put_candidate_df.iloc[0,0]

curr_ticker = yf.Ticker(curr_ticker_str)


# returns a tuple of expiration dates for the given ticker
exp_dates = curr_ticker.options



# create a tuple with just the first two items in expiration date tuple for testing
# We will loop through the entire thing if this works
# delete this later so we have the rest of the tuple
exp_dates = exp_dates[0:3]

curr_exp_dt = exp_dates[0]

option_chain_curr = curr_ticker.option_chain(curr_exp_dt)

put_data_df = option_chain_curr.puts

put_data_df = put_data_df.copy()[['strike', 'bid', 'ask', 'impliedVolatility']].head(0)

# Add expiration date, current date, and ticker to dataframe
put_data_df['exp_date'] = pd.Series()
put_data_df['as_of_date'] = pd.Series()
put_data_df['ticker'] = pd.Series()


print("Current put_data_df length: {}".format(len(put_data_df)))


# Iterate through the tickers in put candidates
for index, row in put_candidate_df.iterrows():  
    curr_ticker_str = put_candidate_df.iloc[index,0]
    curr_ticker = yf.Ticker(curr_ticker_str)

    print("Pulling option data for: " + curr_ticker_str)
    
    # returns a tuple of expiration dates for the given ticker
    exp_dates = curr_ticker.options
    # delete this later so we have the rest of the tuple
    exp_dates = exp_dates[0:3]

    # For each expiration, add to the main dataframe
    for i in exp_dates:
        print("Adding put data for ticker: {}, expiration date: {}".format(curr_ticker_str, i))
        option_chain_curr = curr_ticker.option_chain(i)
        curr_put_data_df = option_chain_curr.puts.copy()[['strike', 'bid', 'ask', 'impliedVolatility']]

        # Add expiration date, current date, and ticker to dataframe
        # Expiration Dates coming from options attribute are strings.  Convert to date before storing it in final dataframe
        # For as_of_date:  Use local time (Los Angeles).
        #   Need to first get utc time, then convert to local since datetime.now() behaves differently on different machines
        curr_exp_dt = datetime.strptime(i, "%Y-%m-%d").date()
        curr_put_data_df['exp_date'] = curr_exp_dt
        # Pull UTC datetime
        utcmoment_naive = datetime.utcnow()
        # make utc naive timezone aware
        utcmoment = utcmoment_naive.replace(tzinfo=pytz.utc)
        # convert to local (Los Angeles)
        local_timezone = pytz.timezone('America/Los_Angeles')
        local_time = utcmoment.astimezone(local_timezone)
        curr_put_data_df['as_of_date'] = local_time
        curr_put_data_df['ticker'] = curr_ticker_str

        # append to main dataframe
        put_data_df = pd.concat([put_data_df, curr_put_data_df])

        print("Current put_data_df length: {}".format(len(put_data_df)))    


put_data_df.groupby('ticker').size()


# ### Create connection to postgres and write to table

# engine = create_engine('postgresql://root:root@localhost:5432/option_data')
# Running this in kestra docker container.  So specifiy pgdatabase instead of localhost
engine = create_engine('postgresql://root:root@pgdatabase:5432/option_data')


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


# Start with creating a table
# Only write the first row without any data.  This creates the table with no data
# not sure if this will work, or if I need to create schema first
put_data_df.head(n=0).to_sql(name='put_option_data', con=engine, dtype=column_typ_dict, if_exists='replace', index=False)


# write table to postgres
t_start = time()
put_data_df.to_sql(name='put_option_data', con=engine, if_exists='append', index=False)
t_end = time()
print('Finished inserting put data.  Took %.3f seconds' % (t_end - t_start))

