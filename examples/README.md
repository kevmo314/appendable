# kevmo314/appendable/examples

These examples are hosted on this repository's GitHub pages.


```
# yellow tripdata
wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet

python3 -c "import pandas; pandas.read_parquet('yellow_tripdata_2023-01.parquet').to_json('yellow_tripdata_2023-01.jsonl', orient='records', lines=True)"
```

To build it locally, download the data and convert it to `jsonl`:

```sh
cd workspace 

# green tripdata
python3 -m pip install -r requirements.txt

# fetch data with .jsonl format
python3 fetch_jsonl.py
```

Then run the indexing process:

```sh
# for jsonl:
npm run build-index-jsonl 

# or for csv:
npm run build-index-csv
```

Copy the `.jsonl` and index file to `/client`

```sh
cp green_tripdata_2023-01.jsonl ../client
cp green_tripdata_2023-01.jsonl.index ../client
```

or for csv:
```sh
cp green_tripdata_2023-01.csv ../client
cp green_tripdata_2023-01.csv.index ../client
```

Build the AppendableDB client library:

```sh
npm run build
```

Copy the Appendable library to `/client`

```sh
cp ../../dist/appendable.min.js ../client
cp ../../dist/appendable.min.js.map ../client
```


Then run the development server:

```sh
npm run example
```


You should see the example built on http://localhost:8080
