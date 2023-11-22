# kevmo314/appendable/examples

These examples are hosted on this repository's GitHub pages.

To build it locally, download the data and convert it to `jsonl`:

```sh
wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet

python -c "import pandas; pandas.read_parquet('yellow_tripdata_2023-01.parquet').to_json('yellow_tripdata_2023-01.jsonl', orient='records', lines=True)"
```

Then run the indexing process:

```sh

```

Build the AppendableDB client library:

```sh
npm run build
```

Then run the development server:
