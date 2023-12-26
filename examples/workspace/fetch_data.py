# Data taken from https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

import io

import pandas as pd
import requests

response = requests.get('https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2023-01.parquet')

pd.read_parquet(io.BytesIO(response.content)).to_json('green_tripdata_2023-01.jsonl', orient='records', lines=True)

df = pd.read_parquet(io.BytesIO(response.content))
df.to_csv('green_tripdata_2023-01.csv', index=False)