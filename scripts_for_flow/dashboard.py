"""
Streamlit Dashboard for Put Candidate Prices

Run with: streamlit run dashboard.py
Or via Docker: docker-compose up streamlit-dashboard
"""

import streamlit as st
import pandas as pd
import sys
import os
from sqlalchemy import create_engine

# Add current directory to path to import put_leads
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import the main function from put_leads
from put_leads import main

st.set_page_config(
    page_title="Put Option Candidates",
    page_icon="📊",
    layout="wide"
)

st.title("📊 Put Option Candidates Dashboard")
st.markdown("---")

# Read put_candidate tickers and put_candidate options data from postgres
# Create postgres connection
db_host = os.getenv('DATABASE_HOST', 'pgdatabase')
# db_host = os.getenv('DATABASE_HOST', 'localhost'
print(f"Connecting to database at {db_host}...")
engine = create_engine(f'postgresql://root:root@{db_host}:5432/option_data')

put_option_qry = """
SELECT ticker
     , strike
     , exp_date
     , bid
     , ask
     , mid
     , upfront_premium
     , money_aside
     , raw_return
     , annualized_return
  FROM put_candidate_options
"""

put_candidates_df = pd.read_sql_query("SELECT * FROM put_candidate_tickers", engine)
put_candidate_prices = pd.read_sql_query(put_option_qry, engine)

# Run the analysis
# with st.spinner("Loading data and calculating candidates..."):
#     try:
#         put_candidates_df, put_candidate_prices = main()
#     except Exception as e:
#         st.error(f"Error running analysis: {str(e)}")
#         st.stop()

# Summary statistics
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Total Tickers", len(put_candidates_df))
with col2:
    st.metric("Candidates", put_candidates_df['put_candidate_ind'].sum())
with col3:
    st.metric("Total Options", len(put_candidate_prices))
with col4:
    if len(put_candidate_prices) > 0:
        avg_return = put_candidate_prices['annualized_return'].mean()
        st.metric("Avg Annualized Return", f"{avg_return:.1%}")
    else:
        st.metric("Avg Annualized Return", "N/A")

st.markdown("---")

# Main results table
st.subheader("🎯 Put Candidate Prices")
if len(put_candidate_prices) > 0:
    # Format the dataframe for display
    display_df = put_candidate_prices.copy()
    display_df['annualized_return'] = display_df['annualized_return'].apply(lambda x: f"{x:.2%}")
    display_df['raw_return'] = display_df['raw_return'].apply(lambda x: f"{x:.2%}")
    display_df['upfront_premium'] = display_df['upfront_premium'].apply(lambda x: f"${x:,.2f}")
    display_df['money_aside'] = display_df['money_aside'].apply(lambda x: f"${x:,.2f}")
    
    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True
    )
    
    # Download button
    csv = put_candidate_prices.to_csv(index=False)
    st.download_button(
        label="📥 Download CSV",
        data=csv,
        file_name="put_candidate_prices.csv",
        mime="text/csv"
    )
else:
    st.warning("No put candidates found matching the criteria.")

st.markdown("---")

# Candidate summary table
st.subheader("📈 Candidate Summary")
if len(put_candidates_df) > 0:
    summary_cols = ['ticker', 'current_price', 'week_52_high', 'week_52_low', 'lower_qrt_ind', 
                    'up_vs_pri_day_vs_8day', 'up_vs_pri_wk_vs_8day', 'put_candidate_ind']
    st.dataframe(
        put_candidates_df[summary_cols],
        use_container_width=True,
        hide_index=True
    )
