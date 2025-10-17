import os
import requests
import csv
from datetime import date, datetime
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
load_dotenv()

# ----------------------------
# CONFIG
# ----------------------------
POLYGON_API_KEY = os.getenv('POLYGON_API_KEY')
CSV_FILE = 'tickers_example.csv'
LIMIT = 30  # keep small for free plan
ds = '2025-09-01'
HEADERS = [
    'ticker', 'name', 'market', 'locale', 'primary_exchange', 'type', 'active',
    'currency_name', 'cik', 'composite_figi', 'share_class_figi', 'last_updated_utc', 'ds'
]

# ----------------------------
# FUNCTION: Write to CSV (your version wrapped)
# ----------------------------
def fetch_and_write_tickers_to_csv():
    ds = datetime.now().strftime('%Y-%m-%d')
    url = f'https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&limit={LIMIT}&apiKey={POLYGON_API_KEY}'
    response = requests.get(url)
    data = response.json()
    tickers = []
    for t in data.get('results', []):
        t['ds'] = ds
        tickers.append({
            'ticker': t.get('ticker'),
            'name': t.get('name'),
            'market': t.get('market'),
            'locale': t.get('locale'),
            'primary_exchange': t.get('primary_exchange'),
            'type': t.get('type'),
            'active': t.get('active'),
            'currency_name': t.get('currency_name'),
            'cik': t.get('cik'),
            'composite_figi': t.get('composite_figi'),
            'share_class_figi': t.get('share_class_figi'),
            'last_updated_utc': t.get('last_updated_utc'),
            'ds': date.today()
        })

    # Write to CSV
    with open(CSV_FILE, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=HEADERS)
        writer.writeheader()
        writer.writerows(tickers)

    print(f"{len(tickers)} tickers written to {CSV_FILE}")

# ----------------------------
# FUNCTION: Load to Snowflake
# ----------------------------
def load_data_to_snowflake(csv_file, stock_tickers):
    conn = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA')
    )

    df = pd.read_csv(csv_file)
    print(f"Loaded {len(df)} rows from {csv_file}")

    success, nchunks, nrows, _ = write_pandas(conn, df, stock_tickers.upper(),auto_create_table=False, quote_identifiers=False )

    if success:
        print(f"✅ Uploaded {nrows} rows to Snowflake table '{stock_tickers}'")
    else:
        print("❌ Upload failed.")

    conn.close()

# ----------------------------
# FUNCTION: Main job (Zach-style)
# ----------------------------
def run_stock_job():
    fetch_and_write_tickers_to_csv()
    load_data_to_snowflake(CSV_FILE, 'stock_tickers')
# ----------------------------
# MAIN
# ----------------------------
if __name__ == "__main__":
    run_stock_job()
