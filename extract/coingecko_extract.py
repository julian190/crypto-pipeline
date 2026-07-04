import requests
import pandas as pd
import logging 

logging.basicConfig(level=logging.INFO)

def extract_coingecko_data():
    response = requests.get(
    "https://api.coingecko.com/api/v3/coins/markets",
    params={"vs_currency": "nzd", "per_page": 100}
)
    if response.status_code == 200:
        json_data = response.json()
        df = pd.DataFrame(json_data)
        df['ingested_at'] = pd.Timestamp.now('UTC')
        df['roi.times'] = df['roi'].apply(lambda x: x['times'] if isinstance(x, dict) and 'times' in x else None)
        df['roi.currency'] = df['roi'].apply(lambda x: x['currency'] if isinstance(x, dict) and 'times' in x else None)
        df['roi.percentage'] = df['roi'].apply(lambda x: x['percentage'] if isinstance(x, dict) and 'times' in x else None)
        df.drop(columns=["roi"], inplace=True)
        logging.info(f"Extracted {len(df)} rows")
        return df
    else:
        logging.error(f"Request failed: {response.status_code}")
        
