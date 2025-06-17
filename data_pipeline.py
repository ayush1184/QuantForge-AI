# data_pipeline.py
import yfinance as yf
from supabase import create_client
import os
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime

load_dotenv()  # Load .env variables

# Config
SYMBOLS = ["AAPL", "MSFT", "GOOG", "TSLA", "SPY", "QQQ", "NVDA"]
supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))

def fetch_and_store_data():
    for symbol in SYMBOLS:
        try:
            # Download data
            data = yf.download(symbol, period="5y", interval="1d", progress=False, auto_adjust=True)
            data.reset_index(inplace=True)
            
            # Handle MultiIndex columns by flattening them
            if isinstance(data.columns, pd.MultiIndex):
                # Take the first level (column names) instead of second level (symbol)
                data.columns = [col[0] for col in data.columns]
            
            # Ensure we have clean column names
            data.columns = data.columns.str.lower()
            
            # Add symbol column
            data["symbol"] = symbol
            
            # Select and rename the columns we need
            result_df = data[["date", "symbol", "open", "high", "low", "close", "volume"]].copy()
            
            # Convert date to ISO format for Supabase TIMESTAMPTZ
            result_df["date"] = pd.to_datetime(result_df["date"])
            result_df["date"] = result_df["date"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            
            # Insert into Supabase
            supabase.table("historical_data").insert(result_df.to_dict('records')).execute()
            print(f"✅ {symbol} data inserted: {len(result_df)} records")
            
        except Exception as e:
            print(f"❌ {symbol} failed: {str(e)}")

if __name__ == "__main__":
    fetch_and_store_data()