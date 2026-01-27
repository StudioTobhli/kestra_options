import pandas as pd
from sqlalchemy import create_engine
import gspread
from gspread_dataframe import set_with_dataframe

# Read from Postgres
engine = create_engine('postgresql://root:root@pgdatabase:5432/option_data')
put_options_df = pd.read_sql('SELECT * FROM put_candidate_options', engine)

gc = gspread.service_account(filename='studiotlanalyticsSvcAccnt-a59159d08cb6.json')
sh = gc.open("put_candidate_gs_src")
wksht = sh.get_worksheet(0)

set_with_dataframe(wksht, put_options_df, include_index=False, include_column_header=True, resize=True)

print(f"Wrote {len(put_options_df)} rows to Google Sheets")

# Write stock history of put data
put_stock_hist_qry = """
SELECT sh.ticker
     , sh.hist_date
     , sh.close
  FROM stock_hist_data sh
  JOIN put_candidate_tickers pt
  ON sh.ticker = pt.ticker
"""

put_stock_hist_df = pd.read_sql(put_stock_hist_qry, engine)

# Open target gsheet
hist_sh = gc.open("put_stock_hist_data")
hist_ws = hist_sh.get_worksheet(0)

# Write stock history to gsheet
set_with_dataframe(hist_ws, put_stock_hist_df, include_index=False, include_column_header=True, resize=True)

print(f"Wrote{len(put_stock_hist_df)} rows to put_stock_hist_data")