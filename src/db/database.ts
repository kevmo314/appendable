import { BTree, ReferencedValue } from "../btree/btree";
import { LinkedMetaPage, maxUint64 } from "../file/multi";
import { DataFile } from "../file/data-file";
import { VersionedIndexFile } from "../file/index-file";
import { IndexHeader, readIndexMeta } from "../file/meta";
import { QueryBuilder } from "./query-builder";
import { validateQuery, validateSearch } from "./query-validation";
import {
  Query,
  Schema,
  WhereNode,
  handleSelect,
  processWhere,
  Search,
} from "./query-lang";
import { NgramTokenizer } from "../ngram/tokenizer";
import { PriorityTable } from "../ngram/table";
export enum FieldType {
  String = 0,
  Int64 = 1,
  Uint64 = 2,
  Float64 = 3,
  Object = 4,
  Array = 5,
  Boolean = 6,
  Null = 7,
  Trigram = 8,
  Bigram = 9,
  Unigram = 10,
}

export function fieldTypeToString(f: FieldType): string {
  let str;
  switch (f) {
    case FieldType.String:
      str = "String";
      break;

    case FieldType.Int64:
      str = "Int64";
      break;

    case FieldType.Uint64:
      str = "Uint64";
      break;

    case FieldType.Float64:
      str = "Float64";
      break;

    case FieldType.Object:
      str = "Object";
      break;

    case FieldType.Array:
      str = "Array";
      break;

    case FieldType.Boolean:
      str = "Boolean";
      break;
    case FieldType.Null:
      str = "Null";
      break;
    case FieldType.Trigram:
      str = "Trigram";
      break;
    case FieldType.Bigram:
      str = "Bigram";
      break;
    case FieldType.Unigram:
      str = "Unigram";
      break;
  }
  return str;
}

type DataPointer = {
  start: number;
  end: number;
};

export class Database<T extends Schema> {
  private indexHeadersPromise?: Promise<IndexHeader[]>;

  private constructor(
    private dataFile: DataFile,
    private indexFile: VersionedIndexFile<T>,
  ) {}

  static forDataFileAndIndexFile<T extends Schema>(
    dataFile: DataFile,
    indexFile: VersionedIndexFile<T>,
  ) {
    return new Database(dataFile, indexFile);
  }

  async fields() {
    if (!this.indexHeadersPromise) {
      this.indexHeadersPromise = this.indexFile.indexHeaders();
    }

    return this.indexHeadersPromise;
  }

  async *query(query: Query<T>) {
    if (new Set((query.where ?? []).map((where) => where.key)).size > 1) {
      throw new Error("composite indexes not supported... yet");
    }

    const { format, entries } = await this.indexFile.metadata();
    const dfResolver = this.dataFile.getResolver();
    if (!dfResolver) {
      throw new Error("data file is undefined");
    }

    const headers = await this.fields();

    if (query.search) {
      validateSearch(query.search, headers);
      let { key: fieldName, like, config } = query.search;
      let { minGram, maxGram } = config!;

      const tok = new NgramTokenizer(minGram, maxGram);
      const likeToks = NgramTokenizer.shuffle(tok.tokens(like));

      const table = new PriorityTable<string>();
      const metaPageCache = new Map<FieldType, LinkedMetaPage>();

      for (const token of likeToks) {
        const { type: fieldType, valueBuf } = token;
        let mp = metaPageCache.get(fieldType);

        if (!mp) {
          const mps = await this.indexFile.seek(fieldName as string, fieldType);
          if (mps.length !== 1) {
            throw new Error(
              `Expected to find meta page for key: ${fieldName as string} and type: ${fieldTypeToString(fieldType)}`,
            );
          }
          mp = mps[0];

          metaPageCache.set(fieldType, mps[0]);
        }

        const {
          fieldType: mpFieldType,
          width: mpFieldWidth,
          totalFieldValueLength,
        } = await readIndexMeta(await mp.metadata());

        const btree = new BTree(
          this.indexFile.getResolver(),
          mp,
          dfResolver,
          format,
          mpFieldType,
          mpFieldWidth,
        );

        // calculate the IDF for every token
        // N = total # of documents
        // nt = # of documents containing the term

        const valueRef = new ReferencedValue(
          { offset: 0n, length: 0 },
          valueBuf,
        );
        const iter = btree.iter(valueRef);

        // for a given document, the number of times this specific token appears
        let tfMap = new Map<DataPointer, number>();
        while (await iter.next()) {
          const currentKey = iter.getKey();

          if (ReferencedValue.compareBytes(currentKey.value, valueBuf) !== 0) {
            break;
          }
          const mp = iter.getPointer();

          const dp: DataPointer = {
            start: Number(mp.offset),
            end: Number(mp.offset) + mp.length - 1,
          };

          tfMap.set(dp, (tfMap.get(dp) ?? 0) + 1);
        }

        const n = entries;
        const nt = tfMap.size;
        // The inverse document frequency (IDF) involves # of documents (N) and the # of documents containing the token term
        const idf = Math.log(1 + (n - nt + 0.5) / nt + 0.5);

        // a parameter used to limit how much a single query term can affect the score for document D.
        const K1 = 1.2;

        // a parameter used to control the effect of the field length of field t compared to the average field length.
        const B = 0.75;
        for (const [key, tf] of tfMap.entries()) {
          const { start, end } = key;
          const data = await this.dataFile.get(start, end);
          const dataRecord = JSON.parse(data);
          const valueLength = dataRecord[fieldName].length;

          const num = tf * (K1 + 1);
          const den =
            tf +
            K1 *
              (1 -
                B +
                (B * valueLength) /
                  (valueLength / (totalFieldValueLength / entries)));

          const score = idf * (num / den);

          table.insert(data, score);
        }
      }

      const pq = table.iter();
      let next = pq.next();
      while (!next.done) {
        const { key, score } = next.value;
        yield { key: JSON.parse(key), score };
      }
    }

    if (query.where) {
      for (const { key, value, operation } of query.where ?? []) {
        const header = headers.find((header) => header.fieldName === key);
        if (!header) {
          throw new Error("field not found");
        }

        const res = processWhere(value);
        if (res === null) {
          throw new Error(`unable to process key with a type ${typeof value}`);
        }
        const { fieldType, valueBuf } = res;

        const mps = await this.indexFile.seek(key as string, fieldType);
        const mp = mps[0];
        const { fieldType: mpFieldType, width: mpFieldWidth } =
          await readIndexMeta(await mp.metadata());

        let ord: "ASC" | "DESC" = "ASC";
        if (query.orderBy && query.orderBy[0]) {
          ord = query.orderBy[0].direction;
        }

        const btree = new BTree(
          this.indexFile.getResolver(),
          mp,
          dfResolver,
          format,
          mpFieldType,
          mpFieldWidth,
        );

        if (operation === ">") {
          if (ord === "ASC") {
            const valueRef = new ReferencedValue(
              { offset: maxUint64, length: 0 },
              valueBuf,
            );
            const iter = btree.iter(valueRef);

            while (await iter.next()) {
              const mp = iter.getPointer();

              const data = await this.dataFile.get(
                Number(mp.offset),
                Number(mp.offset) + mp.length - 1,
              );

              yield handleSelect(data, query.select);
            }
          } else {
            const lastKey = await btree.last();
            const iter = btree.iter(lastKey);

            while (await iter.prev()) {
              const currentKey = iter.getKey();

              if (
                ReferencedValue.compareBytes(currentKey.value, valueBuf) < 0
              ) {
                break;
              }

              const mp = iter.getPointer();

              const data = await this.dataFile.get(
                Number(mp.offset),
                Number(mp.offset) + mp.length - 1,
              );
              yield handleSelect(data, query.select);
            }
          }
        } else if (operation === ">=") {
          if (ord === "ASC") {
            const valueRef = new ReferencedValue(
              { offset: 0n, length: 0 },
              valueBuf,
            );
            const iter = btree.iter(valueRef);

            while (await iter.next()) {
              const mp = iter.getPointer();

              const data = await this.dataFile.get(
                Number(mp.offset),
                Number(mp.offset) + mp.length - 1,
              );

              yield handleSelect(data, query.select);
            }
          } else {
            const lastKey = await btree.last();
            const iter = btree.iter(lastKey);

            while (await iter.prev()) {
              const currentKey = iter.getKey();

              if (
                ReferencedValue.compareBytes(currentKey.value, valueBuf) < 0
              ) {
                break;
              }

              const mp = iter.getPointer();

              const data = await this.dataFile.get(
                Number(mp.offset),
                Number(mp.offset) + mp.length - 1,
              );

              yield handleSelect(data, query.select);
            }
          }
        } else if (operation === "==") {
          const valueRef = new ReferencedValue(
            { offset: 0n, length: 0 },
            valueBuf,
          );
          const iter = btree.iter(valueRef);

          while (await iter.next()) {
            const currentKey = iter.getKey();

            if (
              ReferencedValue.compareBytes(currentKey.value, valueBuf) !== 0
            ) {
              break;
            }

            const mp = iter.getPointer();

            if (mp === null) {
              throw new Error(`memory pointer is undefined`);
            }

            const data = await this.dataFile.get(
              Number(mp.offset),
              Number(mp.offset) + mp.length - 1,
            );

            yield handleSelect(data, query.select);
          }
        } else if (operation === "<=") {
          if (ord === "DESC") {
            const valueRef = new ReferencedValue(
              { offset: maxUint64, length: 0 },
              valueBuf,
            );
            const iter = btree.iter(valueRef);
            while (await iter.prev()) {
              const mp = iter.getPointer();

              const data = await this.dataFile.get(
                Number(mp.offset),
                Number(mp.offset) + mp.length - 1,
              );

              yield handleSelect(data, query.select);
            }
          } else {
            const firstKey = await btree.first();
            const iter = btree.iter(firstKey);

            while (await iter.next()) {
              const currentKey = iter.getKey();

              if (
                ReferencedValue.compareBytes(currentKey.value, valueBuf) > 0
              ) {
                break;
              }

              const mp = iter.getPointer();

              const data = await this.dataFile.get(
                Number(mp.offset),
                Number(mp.offset) + mp.length - 1,
              );

              yield handleSelect(data, query.select);
            }
          }
        } else if (operation === "<") {
          if (ord === "DESC") {
            const valueRef = new ReferencedValue(
              { offset: 0n, length: 0 },
              valueBuf,
            );
            const iter = btree.iter(valueRef);
            while (await iter.prev()) {
              const mp = iter.getPointer();

              const data = await this.dataFile.get(
                Number(mp.offset),
                Number(mp.offset) + mp.length - 1,
              );

              yield handleSelect(data, query.select);
            }
          } else {
            const firstKey = await btree.first();
            const iter = btree.iter(firstKey);

            while (await iter.next()) {
              const currentKey = iter.getKey();

              if (
                ReferencedValue.compareBytes(currentKey.value, valueBuf) >= 0
              ) {
                break;
              }

              const mp = iter.getPointer();

              const data = await this.dataFile.get(
                Number(mp.offset),
                Number(mp.offset) + mp.length - 1,
              );

              yield handleSelect(data, query.select);
            }
          }
        }
      }
    }
  }

  where(
    key: keyof T,
    operation: WhereNode<T>["operation"],
    value: T[keyof T],
  ): QueryBuilder<T> {
    return new QueryBuilder(this).where(key, operation, value);
  }

  search(
    key: keyof T,
    like: string,
    config?: { minGram: number; maxGram: number },
  ) {
    let { minGram = 1, maxGram = 2 } = config || {};

    const search: Search<T> = {
      key,
      like,
      config: {
        minGram,
        maxGram,
      },
    };

    return this.query({
      search,
    });
  }
}
