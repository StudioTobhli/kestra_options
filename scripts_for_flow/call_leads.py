import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import Float, Integer, DateTime, VARCHAR

def main():
    # 1. Database Setup
    db_host = 'pgdatabase'
    engine = create_engine(f'postgresql://root:root@{db_host}:5432/option_data')

    # Step 2: Fetch holdings (SQL-filtered for >= 100 shares)
    # This reduces the amount of data Python has to process
    print("Fetching qualifying holdings...")
    holdings_sql = "SELECT ticker, shares, avg_cost_basis FROM current_holdings WHERE shares >= 100"
    holdings_df = pd.read_sql_query(holdings_sql, con=engine)
    
    if holdings_df.empty:
        print("No holdings found with 100+ shares. Exiting.")
        return

    # Step 3: Fetch 30-day history (SQL-filtered for date range)
    print("Fetching 30-day price history...")
    hist_sql = "SELECT ticker, high FROM stock_hist_data WHERE hist_date >= CURRENT_DATE - INTERVAL '30 days'"
    hist_df = pd.read_sql_query(hist_sql, con=engine)
    
    # Calculate Max Price over the 30-day window (Vectorized)
    max_price_df = hist_df.groupby('ticker')['high'].max().reset_index()
    max_price_df.columns = ['ticker', 'max_30d_price']

    # Step 4: Calculate Thresholds
    df = holdings_df.merge(max_price_df, on='ticker', how='left')
    df['target_strike'] = df['avg_cost_basis'] * 0.90
    df['price_threshold'] = df['avg_cost_basis'] * 0.60
    
    # Set lead indicator (1 if max price stayed <= 60% of cost basis)
    df['call_lead_ind'] = (df['max_30d_price'] <= df['price_threshold']).astype(int)

    # Step 5: Filter available options
    print("Filtering available call options...")
    options_sql = "SELECT ticker, strike, bid, ask, exp_date FROM call_option_data"
    options_df = pd.read_sql_query(options_sql, con=engine)
    
    # Inner join ensures we only keep tickers that actually have option data
    final_df = df.merge(options_df, on='ticker', how='inner')
    
    # Final Filter: Must be a lead AND the specific strike must be >= 90% of cost basis
    output_df = final_df[
        (final_df['call_lead_ind'] == 1) & 
        (final_df['strike'] >= final_df['target_strike'])
    ].copy()

    # Step 6: Write to Postgres (The source for your next Kestra step)
    print(f"Writing {len(output_df)} candidates to 'call_leads' table...")
    
    column_type_dict = {
        'ticker': VARCHAR(20),
        'shares': Integer(),
        'avg_cost_basis': Float(),
        'max_30d_price': Float(),
        'target_strike': Float(),
        'price_threshold': Float(),
        'strike': Float(),
        'bid': Float(),
        'ask': Float(),
        'exp_date': DateTime(),
        'call_lead_ind': Integer()
    }
    
    output_df.to_sql(
        'call_leads', 
        con=engine, 
        dtype=column_type_dict, 
        if_exists='replace', 
        index=False
    )
    
    print("Done. Postgres table 'call_leads' updated.")

if __name__ == "__main__":
    main()