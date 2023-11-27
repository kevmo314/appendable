# Data taken from https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

import pandas as pd
import requests
import io

response = requests.get('https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2023-01.parquet')
pd.read_parquet(io.BytesIO(response.content)).to_json('yellow_tripdata_2023-01.jsonl', orient='records', lines=True)