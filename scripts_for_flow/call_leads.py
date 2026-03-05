"""
Implementation plan as per call_options_implementation.md
"""

import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta

def main():
    # Database connection
    engine = create_engine("postgresql://root:root@pgdatabase:5432/option_data")

    # Fetch holdings >= 100
    holdings_sql = """
        SELECT ticker, shares, avg_cost_basis FROM current_holdings
        WHERE shares >= 100
    """
    holdings_df = pd.read_sql_query(holdings_sql, con=engine)

    # Fetch stock history (last 30 days)
    hist_sql = """
        SELECT ticker, hist_date, high FROM stock_hist_data
        WHERE hist_date >= CURRENT_DATE - INTERVAL '30 days'
    """
    hist_df = pd.read_sql_query(hist_sql, con=engine)

    # Fetch call option data
    options_sql = "SELECT ticker, strike, bid, exp_date FROM call_option_data"
    options_df = pd.read_sql_query(options_sql, con=engine)

    # Main processing logic
    output_df = identify_call_leads(holdings_df, hist_df, options_df)

    # Write to PostgreSQL
    if not output_df.empty:
        output_df.to_sql(
            name='call_leads',
            con=engine,
            index=False,
            if_exists='replace'
        )


def identify_call_leads(holdings_df, hist_df, options_df):
    """Identify call leads based on the criteria set"""
    result = []

    for _, row in holdings_df.iterrows():
        ticker = row['ticker']
        avg_cost_basis = row['avg_cost_basis']

        # Get max 30-day price
        max_30d_price = hist_df[hist_df['ticker'] == ticker]['high'].max() if not hist_df[hist_df['ticker'] == ticker].empty else 0

        # Filter available options, filter for latest expiry only
        valid_options = options_df[(options_df['ticker'] == ticker) & (options_df['strike'] > avg_cost_basis * 0.9)]
    
        for _, opt_row in valid_options.iterrows():
            # Calculate lead indicator
            max_30d_condition = max_30d_price <= avg_cost_basis * 0.6
            strike_condition = opt_row['strike'] >= avg_cost_basis * 0.9

            call_lead_indicator = 1 if max_30d_condition and strike_condition else 0
            result.append({
                'ticker': ticker,
                'shares': row['shares'],
                'avg_cost_basis': avg_cost_basis,
                'max_30d_price': max_30d_price,
                'strike': opt_row['strike'],
                'bid': opt_row['bid'],
                'exp_date': opt_row['exp_date'],
                'call_lead_ind': call_lead_indicator
            })

    return pd.DataFrame(result)


if __name__ == "__main__":
    main()
