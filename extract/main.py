from coingecko_extract import extract_coingecko_data

if __name__ == "__main__":
    df = extract_coingecko_data()
    if df is not None:
        print(df.head())
