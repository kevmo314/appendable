# Appendable

Appendable is an append-only\*, schemaless, service-less database.

Appendable doesn't require a conventional server, instead it generates an index
file that you can host on your favorite static hosting site.

Appendable currently supports data files in the following formats:

- [x] [JSON Lines](https://jsonlines.org/) `.jsonl`
- [ ] Parquet
- [ ] CSV
- [ ] TSV
- [ ] RecordIO

with more formats coming soon.

_\* Ok, it's append-preferred._

## Motivation

A smart friend of mine once said

> _The problem with databases is that everybody cares about a different killer feature_

Appendable's primary goals are

- Cost-optimized serving. Leverage your favorite static content host instead of
  maintaining and provisioning resources for a dedicated database server.
- Speed-optimized (O(1)) index updating for appends. Index file updates are
  fast and deterministic, making them suitable for real-time and streaming data
  updates.

## Demonstration

Check out this repository's GitHub pages for an example querying the server.

```ts
import Appendable from "appendable";

const db = Appendable.init("data.jsonl", "index.dat");

const results = await db
  .where("timestamp", ">=", "2023-11-01T00:00:00Z")
  .where("count", "<=", 15)
  .orderBy("count", "DESC")
  .orderBy("timestamp", "ASC")
  .limit(20)
  .get();

console.log(results); // contains data.jsonl queried with the above query.
```

## Getting Started

TODO: implement a mini walkthrough.

## Advanced Usage

### Real-time updates

Appendable indexes are intended to be very cheap to produce incrementally. It is so
cheap that it is not unreasonable to generate the index on demand. That is, you can
run a server such that `index.dat` produces the output from running
`./appendable -i index.dat` and cache the latest version on your CDN. Couple this with
a signalling channel to indicate that a version update has occurred to subscribe to
updates. For example,

```ts
import Appendable from "appendable";

const db = Appendable.init("data.jsonl", "index.dat");

const unsubscribe = db
  .where("timestamp", ">=", "2023-11-01T00:00:00Z")
  .where("count", "<=", 15)
  .orderBy("count", "DESC")
  .orderBy("timestamp", "ASC")
  .limit(20)
  .onSnapshot((results) => {
    console.log(results);
  });

// then elsewhere

db.dirty();
```

Snapshot updates will only occur when the underlying data has changed. Therefore, `.dirty()`
can be called without too much concern.

### Schemas

A schema file is not required to use Appendable, however if you wish to ensure that
your data follows certain types, pass a JSON Schema file with `-s schema.json` and
Appendable will throw an error instead of inferring the type from the data. This
can be useful for detecting consistency issues or enforcing field restrictions.

A word of caution, if you add a non-nullable field to your JSON schema, this will cause
all your previous data to be invalidated requiring an index regeneration. To avoid this,
pass `--imply-nullable` to indicate that previous data is ok to be null but new data
should validate. Be aware that this has implications on the generated types, in particular
your client will see the field as nullable despite the schema saying non-nullable.

### Generated types

Appendable can also emit TypeScript type definitions. Pass `-t output.d.ts` to produce
an inferred type definition file to make your queries type-safe. This can be used with

```ts
import Appendable from "appendable";
import DBTypes from 'output.d.ts';

const db = Appendable.init<DBTypes>("data.jsonl", "index.dat");

...
```

Note that if a schema file is provided, it is guaranteed that the generated type definition
file is stable. That is, if the schema file does not change, the type definition file will
not change.

### Complex queries

The demonstration example uses a simple query, however the query builder is syntactic sugar over
a `.query()` call. If you wish to perform more advanced queries, you can do so by calling `.query()`
directly. For example,

```ts
import Appendable from "appendable";

const db = Appendable.init("data.jsonl", "index.dat");

const results = await db.query({
  where: {
    operation: "AND",
    values: [
      { operation: ">=", key: "timestamp", value: "2023-11-01T00:00:00Z" },
      { operation: "<=", key: "count", value: 15 },
    ],
  },
  orderBy: [
    { key: "count", direction: "DESC" },
    { key: "timestamp", direction: "ASC" },
  ],
  limit: 20,
});
```

### Permissioning

Appendable does not support permissioning because it assumes that the data is publicly
readable. To accommodate permissions, we recomend guarding the access of your data files
via your preferred authentication scheme. That is, create an index file for each user's
data. For example, your static file content may look something like

```
/users/alice/data.jsonl
/users/alice/index.dat
/users/bob/data.jsonl
/users/bob/index.dat
/users/catherine/data.jsonl
/users/catherine/index.dat
```

Where each user has access to their own data and index file.

### Mutating existing data

Appendable is geared towards data that is immutable, however in practice this might not
be ideal. In order to accommodate data mutations, a data integrity hash is maintained so
when data is mutated, the data will be reindexed. Reindexing is O(n) in the age
of the oldest mutation (hence why appending is O(1) for updating the index!) so mutating
data early on in the data file will be more expensive to update.

Mutations must be carefully performed because they will cause the previous index
to be corrupted. Therefore, when updating the files on your server, the data and index
files must be done atomically. This is tricky to do right, however one approach is to
version your data and index files.

### Custom `fetch()` API

For convenience, Appendable uses the browser's `fetch()` for fetching data files if
the data and index files are specified as a string. If you wish to use your own library
or wish to add your own headers, pass a callback.

The callback must correctly return a byte slice representing the start and end parameters.

For example,

```ts
import Appendable from "appendable";

const db = Appendable.init(
  (start: number, end: number) => {
    const response = await fetch("data.jsonl", {
      headers: { Range: `bytes=${start}-${end}` },
    });
    return await response.arrayBuffer();
  },
  (start: number, end: number) => {
    const response = await fetch("index.dat", {
      headers: { Range: `bytes=${start}-${end}` },
    });
    return await response.arrayBuffer();
  }
);
```

## Peanut gallery

### Why not query a SQLite database with range requests?

Dumping all the data into a SQLite database, hosting it, and then querying with
something like [sql.js](https://sql.js.org/) _could_ (and probably would) work,
but I find it particularly elegant that Appendable doesn't change the raw data.
In other words, besides producing an index file the `jsonl` file provided stays
untouched which means updates to the index file can lag behind data changes and
remain valid because the database doesn't need to shuffle the data around.

### My data is never append-only, what's the point of this?

A lot of data isn't actually append-only but can often be restructured as if it
were append-only. For example, creating a sequence of changelogs or deltas lets
you see the history and evolution of a document.

Of course, not all data can be structured like this but Appendable started from
me observing that a decent chunk of my data _was_ written to an appending table
and not mutating any existing data and that it avoided some performance issues,
but the underlying database didn't take advantage of them. For example, with an
append-only data file, Appendable doesn't have to worry about row locking. That
means that there's no tail latency issues when querying and the database can be
scaled horizontally with conventional CDNs. This isn't possible (well ok, it is
but it's [very expensive](https://cloud.google.com/spanner)) with the usual set
of databases.

If you're not convinced, think of Appendable as more geared towards time-series
datasets. Somewhat like a [kdb+](https://en.wikipedia.org/wiki/Kdb%2B) database
but meant for applications instead of more specialized use cases.
