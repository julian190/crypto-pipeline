from dotenv import load_dotenv
import os 
from load.postgres_load import load_to_postgres
from extract.coingecko_extract import extract_coingecko_data


load_dotenv('.env')
dburl = os.getenv('databaseURL')
print(f"Database URL: {dburl}")

if __name__ == "__main__":
    # Example usage
    df = extract_coingecko_data()
    load_to_postgres(df, "crypto_data","raw",dburl)