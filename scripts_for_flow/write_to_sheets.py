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
