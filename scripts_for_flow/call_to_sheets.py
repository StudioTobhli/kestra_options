#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
import gspread
from gspread_dataframe import set_with_dataframe


engine = create_engine('postgresql://root:root@pgdatabase:5432/option_data')

call_leads_df = pd.read_sql('SELECT * FROM call_leads', engine)

gc = gspread.service_account(filename='studiotlanalyticsSvcAccnt-a59159d08cb6.json')
sh = gc.open("call_candidate_gs_src")
wksht = sh.get_worksheet(0)

wksht.clear()
set_with_dataframe(wksht, call_leads_df, include_index=False, include_column_header=True, resize=True)

print(f"Wrote {len(call_leads_df)} rows to Google Sheets")

# Write stock history of call data
call_stock_hist_qry = """
SELECT sh.ticker
     , sh.hist_date
     , sh.close
  FROM stock_hist_data sh
  JOIN current_holdings ch
  ON sh.ticker = ch.ticker
"""

call_stock_hist_df = pd.read_sql(call_stock_hist_qry, engine)

# Open target gsheet
hist_sh = gc.open("call_stock_hist_data")
hist_ws = hist_sh.get_worksheet(0)

# Write stock history to gsheet
set_with_dataframe(hist_ws, call_stock_hist_df, include_index=False, include_column_header=True, resize=True)

print(f"Wrote {len(call_stock_hist_df)} rows to call_stock_hist_data")
