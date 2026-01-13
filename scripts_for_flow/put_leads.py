"""
Put Leads Analysis Script

This script analyzes put option data to identify potential put selling candidates.
It processes option data, stock dimension data, and historical stock data to:
1. Calculate option metrics (premium, returns, etc.)
2. Identify candidates based on price position and momentum indicators
3. Filter and rank the best put options to sell

Author: Generated from put_leads.ipynb
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import os
from sqlalchemy.types import Float
from sqlalchemy.types import Date
from sqlalchemy.types import DateTime
from sqlalchemy.types import VARCHAR
from sqlalchemy.types import Integer

def calculate_up_vs_pri_day_vs_8day(row, stock_hist_data):
    """
    Calculate up_vs_pri_day_vs_8day indicator.
    
    Returns 1 if:
    - Current price > prior day's price
    - Prior day's price < 8-day moving average (ending day before prior day)
    Otherwise returns 0.
    """
    ticker = row['ticker']
    latest_date = row['latest_close_date']
    current_price = row['current_price']
    
    # Get ticker's historical data
    ticker_hist = stock_hist_data[stock_hist_data['ticker'] == ticker].sort_values('hist_date')
    
    if ticker_hist.empty:
        return 0
    
    # Prior day (most recent before latest_close_date)
    prior_day_data = ticker_hist[ticker_hist['hist_date'] < latest_date]
    if prior_day_data.empty:
        return 0
    prior_day_price = prior_day_data.iloc[-1]['close']
    prior_day_date = prior_day_data.iloc[-1]['hist_date']
    
    # Day before prior day
    day_before_data = ticker_hist[ticker_hist['hist_date'] < prior_day_date]
    if len(day_before_data) < 8:
        return 0
    
    # 8-day moving average ending on day before prior day
    day_before_8day_avg = day_before_data.iloc[-8:]['close'].mean()
    
    # Check conditions
    condition1 = current_price > prior_day_price
    condition2 = prior_day_price < day_before_8day_avg
    
    return 1 if (condition1 and condition2) else 0


def calculate_up_vs_pri_wk_vs_8day(row, stock_hist_data):
    """
    Calculate up_vs_pri_wk_vs_8day indicator.
    
    Returns 1 if:
    - Current price > end of prior week's price
    - End of prior week's price < 8-day moving average (ending week before)
    Otherwise returns 0.
    """
    ticker = row['ticker']
    latest_date = row['latest_close_date']
    current_price = row['current_price']
    
    # Get ticker's historical data
    ticker_hist = stock_hist_data[stock_hist_data['ticker'] == ticker].sort_values('hist_date')
    
    if ticker_hist.empty:
        return 0
    
    # Find end of prior week (last Friday before latest_close_date)
    # Get the weekday of latest_date (0=Monday, 6=Sunday)
    latest_weekday = latest_date.weekday()
    
    # Calculate days back to last Friday
    if latest_weekday == 0:  # Monday
        days_back = 3
    elif latest_weekday == 4:  # Friday - need previous Friday (7 days ago)
        days_back = 7
    elif latest_weekday == 6:  # Sunday
        days_back = 2
    else:  # Tuesday-Thursday, Saturday
        days_back = latest_weekday - 4 if latest_weekday >= 5 else latest_weekday + 3
    
    end_prior_week_target = latest_date - timedelta(days=days_back)
    
    # Find the actual closing price for end of prior week
    prior_week_data = ticker_hist[ticker_hist['hist_date'] <= end_prior_week_target]
    if prior_week_data.empty:
        return 0
    end_prior_week_price = prior_week_data.iloc[-1]['close']
    end_prior_week_date = prior_week_data.iloc[-1]['hist_date']
    
    # Find end of week before that
    week_before_target = end_prior_week_date - timedelta(days=7)
    week_before_data = ticker_hist[ticker_hist['hist_date'] <= week_before_target]
    
    if len(week_before_data) < 8:
        return 0
    
    # 8-day moving average ending the week before
    week_before_8day_avg = week_before_data.iloc[-8:]['close'].mean()
    
    # Check conditions
    condition1 = current_price > end_prior_week_price
    condition2 = end_prior_week_price < week_before_8day_avg
    
    return 1 if (condition1 and condition2) else 0


def main():
    """Main execution function."""
    
    # Connect to Postgres
    # Use environment variable for database host, default to pgdatabase (Docker network)
    # Set DATABASE_HOST=localhost if running locally outside Docker
    db_host = os.getenv('DATABASE_HOST', 'pgdatabase')
    # db_host = os.getenv('DATABASE_HOST', 'localhost'
    print(f"Connecting to database at {db_host}...")
    engine = create_engine(f'postgresql://root:root@{db_host}:5432/option_data')
    
    # 1) Pull the 3 tables into dataframes
    print("Loading data from database...")
    put_option_data = pd.read_sql_query("SELECT * FROM put_option_data", engine)
    stock_dim_data = pd.read_sql_query("SELECT * FROM stock_dim_data", engine)
    stock_hist_data = pd.read_sql_query("SELECT * FROM stock_hist_data", engine)
    
    print(f"Loaded {len(put_option_data)} put option records")
    print(f"Loaded {len(stock_dim_data)} stock dimension records")
    print(f"Loaded {len(stock_hist_data)} historical stock records")
    
    # Ensure date columns are datetime
    print("\nConverting date columns...")
    put_option_data['exp_date'] = pd.to_datetime(put_option_data['exp_date'])
    stock_dim_data['latest_close_date'] = pd.to_datetime(stock_dim_data['latest_close_date'])
    stock_hist_data['hist_date'] = pd.to_datetime(stock_hist_data['hist_date'])
    
    # 2) Add calculated columns to put_option_data
    print("\nCalculating put option metrics...")
    today = datetime.today()
    
    put_option_data['mid'] = (put_option_data['bid'] + put_option_data['ask']) / 2
    put_option_data['upfront_premium'] = put_option_data['mid'] * 100
    # Using as_of_date instead of today for more accurate calculation
    put_option_data['days_til_strike'] = (
        put_option_data['exp_date'].dt.normalize() - 
        put_option_data['as_of_date'].dt.normalize()
    ).dt.days
    put_option_data['money_aside'] = put_option_data['strike'] * 100
    put_option_data['raw_return'] = put_option_data['upfront_premium'] / put_option_data['money_aside']
    put_option_data['annualized_return'] = (
        put_option_data['raw_return'] * 365 / put_option_data['days_til_strike']
    )
    
    # 3) Create put_candidates_df with unique tickers
    print("\nCreating candidate dataframe...")
    put_candidates_df = pd.DataFrame({'ticker': put_option_data['ticker'].unique()})
    
    # Join stock_dim_data
    put_candidates_df = put_candidates_df.merge(stock_dim_data, on='ticker', how='left')
    
    # Calculate lower_qrt_52wk_bound
    print("Calculating lower quartile indicators...")
    put_candidates_df['lower_qrt_52wk_bound'] = (
        put_candidates_df['week_52_low'] + 
        (put_candidates_df['week_52_high'] - put_candidates_df['week_52_low']) * 0.25
    )
    
    # Calculate lower_qrt_ind
    put_candidates_df['lower_qrt_ind'] = (
        put_candidates_df['current_price'] < put_candidates_df['lower_qrt_52wk_bound']
    ).astype(int)
    
    # 4) Calculate up_vs_pri_day_vs_8day
    print("Calculating up_vs_pri_day_vs_8day indicator...")
    put_candidates_df['up_vs_pri_day_vs_8day'] = put_candidates_df.apply(
        lambda row: calculate_up_vs_pri_day_vs_8day(row, stock_hist_data), axis=1
    )
    
    # 5) Calculate up_vs_pri_wk_vs_8day
    print("Calculating up_vs_pri_wk_vs_8day indicator...")
    put_candidates_df['up_vs_pri_wk_vs_8day'] = put_candidates_df.apply(
        lambda row: calculate_up_vs_pri_wk_vs_8day(row, stock_hist_data), axis=1
    )
    
    # 6) Calculate put_candidate_ind
    print("Calculating put_candidate_ind...")
    put_candidates_df['put_candidate_ind'] = (
        (put_candidates_df['lower_qrt_ind'] + 
         put_candidates_df['up_vs_pri_day_vs_8day'] + 
         put_candidates_df['up_vs_pri_wk_vs_8day']) > 0
    ).astype(int)
    
    # 7) Create put_candidate_prices
    print("\nFiltering and ranking put candidate prices...")
    # Set candidates to tickers with passing put_candidate_ind
    # If none, then pass all tickers, to surface the highest annualized return
    # Include put_candidate_ind in dataset
    candidates = put_candidates_df[['ticker', 'put_candidate_ind']]
    
    # Filter put_option_data for these tickers and annualized_return >= 0.15
    # filtered_puts = put_option_data[put_option_data['ticker'].isin(candidates['ticker'])]
    
    # Get top 3 by annualized_return per ticker
    # originally used filtered_puts, but for now we can use all of put_option_data
    put_candidate_prices = (
        put_option_data
        .sort_values('annualized_return', ascending=False)
        .groupby('ticker')
        .head(3)
        .reset_index(drop=True)
    )

    # Add indicator of whether it passed put_candidate_ind test
    put_candidate_prices = put_candidate_prices.merge(candidates, on='ticker')

    # Prepare to write put_candidates_df and put_candidate_prices to postgres
    # Create schema for put_candidates_df table (put_candidates_tickers)
    put_candidate_schema_dc = {
        'ticker' : VARCHAR(20),
        'current_price' : Float(),
        'week_52_high' : Float(),
        'week_52_low' : Float(),
        'latest_close_date' : DateTime(),
        'lower_qrt_52wk_bound' : Float(),
        'lower_qrt_ind' : Integer(),
        'up_vs_pri_day_vs_8day' : Integer(),
        'up_vs_pri_wk_vs_8day' : Integer(),
        'put_candidate_ind' : Integer()
    }

    # Write put candidate on ticker level to postgres
    put_candidates_df.to_sql('put_candidate_tickers', con=engine, dtype=put_candidate_schema_dc, if_exists='replace', index=False)

    # Create schema for put_candidates_prices table (put_candidate_options)
    put_candidate_prc_sc_dc = {
        'strike' : Float(),
        'bid' : Float(),
        'ask' : Float(),
        'impliedVolatility' : Float(),
        'exp_date' : DateTime(),
        'as_of_date' : DateTime(),
        'ticker' : VARCHAR(20),
        'mid' : Float(),
        'upfront_premium' : Float(),
        'days_til_strike' : Integer(),
        'money_aside' : Float(),
        'raw_return' : Float(),
        'annualized_return' : Float(),
        'put_candidate_ind': Integer()
    }

    # write put candidates with option data to postgres
    put_candidate_prices.to_sql('put_candidate_options', con=engine, dtype=put_candidate_prc_sc_dc, if_exists='replace', index=False)

    # Print results
    print("\n" + "="*80)
    print("RESULTS")
    print("="*80)
    print("\nput_candidates_df:")
    print(put_candidates_df)
    print(f"\nTotal candidates: {put_candidates_df['put_candidate_ind'].sum()} out of {len(put_candidates_df)} tickers")
    
    print("\n" + "="*80)
    print("\nput_candidate_prices:")
    print(put_candidate_prices)
    print(f"\nTotal put options selected: {len(put_candidate_prices)}")
    
    # return put_candidates_df, put_candidate_prices


if __name__ == "__main__":
    # put_candidates_df, put_candidate_prices = main()
    main()
