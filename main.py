import yfinance as yf
import pandas as pd
import os

# Setup GCP credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gcp_key.json"
PROJECT_ID = "my-finance-etl" 

# 1. EXTRACT
print("1. Downloading AAPL data...")
ticker = yf.Ticker("AAPL")
df = ticker.history(period="2y")
df = df.reset_index()

# 2. TRANSFORM
print("2. Processing and transforming data...")
df = df.drop(columns=['Dividends', 'Stock Splits'])
df = df.dropna()

df['Daily_Return_Percent'] = df['Close'].pct_change() * 100
df['Moving_Average_50'] = df['Close'].rolling(window=50).mean()
df = df.dropna()

# 3. LOAD
print("3. Loading data to BigQuery...")
table_id = f"{PROJECT_ID}.stock_data.aapl_stock_history"
df.to_gbq(destination_table=table_id, project_id=PROJECT_ID, if_exists='replace')

print("SUCCESS: ETL pipeline completed.")