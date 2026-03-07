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
