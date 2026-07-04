import pandas as pd
import os
import yfinance as yf
import gspread
from datetime import datetime
from time import time
# Use SQLAlchemy to create a connection to postgres
from sqlalchemy import create_engine
from sqlalchemy.types import Float
from sqlalchemy.types import Integer
from sqlalchemy.types import Date
from sqlalchemy.types import DateTime
from sqlalchemy.types import VARCHAR


os.getcwd()


# #### General Logic
# * Import holdings data from Google Sheets
# * add as of date
# * Sample one date and create an empty dataframe using head(0) to create table
# * Specify data types
# * Upload holdings dataframe to postgres

# Connect to google sheet that contains all potential tickers to sell a call for
gc = gspread.service_account(filename='studiotlanalyticsSvcAccnt-a59159d08cb6.json')
sh = gc.open("Select_Holdings")
wksht = sh.get_worksheet(0)
holdings_df = pd.DataFrame(wksht.get_all_records())

holdings_df['as_of_date'] = datetime.now()

# ### Create connection to postgres and write to table
# engine = create_engine('postgresql://root:root@localhost:5432/option_data')
engine = create_engine('postgresql://root:root@pgdatabase:5432/option_data')

# Specify the date types.  SQLAlchemy with to_sql doesn't choose the right date types by default
column_typ_dict = {
    'ticker' : VARCHAR(20),
    'shares' : Integer(),
    'avg_cost_basis' : Float(),
    'account_alias' : VARCHAR(20),
    'as_of_date' : DateTime()
}

# write empty table to create or replace table
holdings_df.head(n=0).to_sql(name='current_holdings', con=engine, dtype=column_typ_dict, if_exists='replace', index=False)


# write table to postgres
t_start = time()
holdings_df.to_sql(name='current_holdings', con=engine, if_exists='append', index=False)
t_end = time()
print('Finished inserting holdings data.  Took %.3f seconds' % (t_end - t_start))




