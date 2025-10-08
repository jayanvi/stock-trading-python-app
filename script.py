import requests
import csv

POLYGON_API_KEY ='vrh9_dteVoydykkY_RihtcTPNhrIsq3c'
CSV_FILE = 'tickers_example.csv'
LIMIT = 30  # number of tickers to fetch (keep small for free plan)

# CSV headers
headers = ['ticker', 'name', 'market', 'locale', 'primary_exchange', 'type', 'active',
           'currency_name', 'cik', 'composite_figi', 'share_class_figi', 'last_updated_utc']

# Fetch tickers
url = f'https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&limit={LIMIT}&apiKey={POLYGON_API_KEY}'
response = requests.get(url)
data = response.json()

tickers = []
for t in data.get('results', []):
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
        'last_updated_utc': t.get('last_updated_utc')
    })

# Write to CSV
with open(CSV_FILE, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.DictWriter(file, fieldnames=headers)
    writer.writeheader()
    writer.writerows(tickers)

print(f"{len(tickers)} tickers written to {CSV_FILE}")
