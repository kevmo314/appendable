import { BPTree, ReferencedValue } from "../btree/bptree";
import { maxUint64 } from "../btree/multi";
import { DataFile } from "../data-file";
import { VersionedIndexFile } from "../index-file/index-file";
import { readIndexMeta } from "../index-file/meta";
import { QueryBuilder } from "./query-builder";
import { handleSelect, processWhere } from "./query-logic";

export type Schema = {
  [key: string]: {};
};

export type WhereNode<T extends Schema, K extends keyof T = keyof T> = {
  operation: "<" | "<=" | "==" | ">=" | ">";
  key: keyof T;
  value: T[K];
};

export type OrderBy<T extends Schema> = {
  key: keyof T;
  direction: "ASC" | "DESC";
};

export type SelectField<T extends Schema> = keyof T;

export type Query<T extends Schema> = {
  where?: WhereNode<T>[];
  orderBy?: OrderBy<T>[];
  select?: SelectField<T>[];
  limit?: number;
};

export enum FieldType {
  String = 0,
  Int64 = 1,
  Uint64 = 2,
  Float64 = 3,
  Object = 4,
  Array = 5,
  Boolean = 6,
  Null = 7,
}

export class Database<T extends Schema> {
  private constructor(
    private dataFile: DataFile,
    private indexFile: VersionedIndexFile<T>
  ) { }

  static forDataFileAndIndexFile<T extends Schema>(
    dataFile: DataFile,
    indexFile: VersionedIndexFile<T>
  ) {
    return new Database(dataFile, indexFile);
  }

  async fields() {
    return await this.indexFile.indexHeaders();
  }

  async *query(query: Query<T>) {
    if (new Set((query.where ?? []).map((where) => where.key)).size > 1) {
      throw new Error("composite indexes not supported... yet");
    }

    const { format } = await this.indexFile.metadata();
    const dfResolver = this.dataFile.getResolver();

    if (!dfResolver) {
      throw new Error("data file is undefined");
    }

    const headers = await this.indexFile.indexHeaders();

    for (const { key, value, operation } of query.where ?? []) {
      const header = headers.find((header) => header.fieldName === key);
      if (!header) {
        throw new Error("field not found");
      }

      const res = processWhere(value);
      if (res === null) {
        throw new Error(`unable to process key with a type ${typeof value}`)
      }
      const { fieldType, valueBuf } = res;

      const mps = await this.indexFile.seek(key as string, fieldType);
      const mp = mps[0];
      const { fieldType: mpFieldType } = await readIndexMeta(
        await mp.metadata()
      );


      let ord: "ASC" | "DESC" = "ASC";
      if (query.orderBy && query.orderBy[0]) {
        ord = query.orderBy[0].direction
      }

      const bptree = new BPTree(
        this.indexFile.getResolver(),
        mp,
        dfResolver,
        format,
        mpFieldType
      );

      if (operation === ">") {
        if (ord === "ASC") {
          const valueRef = new ReferencedValue({ offset: 0n, length: 0 }, valueBuf)
          const iter = bptree.iter(valueRef);

          while (await iter.next()) {
            const currentKey = iter.getKey();

            if (ReferencedValue.compareBytes(valueBuf, currentKey.value) < 0) {
              const [_, mp] = await bptree.find(currentKey);

              const data = await this.dataFile.get(
                Number(mp.offset),
                Number(mp.offset) + mp.length - 1
              );

              yield handleSelect(data, query.select);
            }
          }
        } else {
          const lastKey = await bptree.last();
          const iter = bptree.iter(lastKey);

          while (await iter.prev()) {
            const currentKey = iter.getKey();

            if (ReferencedValue.compareBytes(valueBuf, currentKey.value) < 0) {
              const [_, mp] = await bptree.find(currentKey);

              const data = await this.dataFile.get(
                Number(mp.offset),
                Number(mp.offset) + mp.length - 1
              );
              yield handleSelect(data, query.select);
            }
          }
        }
      } else if (operation === ">=") {
        if (ord === "ASC") {
          const valueRef = new ReferencedValue({ offset: 0n, length: 0 }, valueBuf)
          const iter = bptree.iter(valueRef);

          while (await iter.next()) {
            const currentKey = iter.getKey();

            if (ReferencedValue.compareBytes(valueBuf, currentKey.value) <= 0) {
              const [_, mp] = await bptree.find(currentKey);

              const data = await this.dataFile.get(
                Number(mp.offset),
                Number(mp.offset) + mp.length - 1
              );

              yield handleSelect(data, query.select);
            }
          }
        } else {
          const lastKey = await bptree.last();
          const iter = bptree.iter(lastKey);

          while (await iter.prev()) {
            const currentKey = iter.getKey();

            if (ReferencedValue.compareBytes(valueBuf, currentKey.value) <= 0) {
              const [_, mp] = await bptree.find(currentKey);

              const data = await this.dataFile.get(
                Number(mp.offset),
                Number(mp.offset) + mp.length - 1
              );

              yield handleSelect(data, query.select);
            }
          }
        }
      } else if (operation === "==") {
        const valueRef = new ReferencedValue({ offset: 0n, length: 0 }, valueBuf)
        const iter = bptree.iter(valueRef);

        while (await iter.next()) {
          const currentKey = iter.getKey();

          if (ReferencedValue.compareBytes(valueBuf, currentKey.value) === 0) {
            const [_, mp] = await bptree.find(currentKey);

            const data = await this.dataFile.get(
              Number(mp.offset),
              Number(mp.offset) + mp.length - 1
            );

            yield handleSelect(data, query.select);
          }
        }
      } else if (operation === "<=") {
        if (ord === "DESC") {
          const valueRef = new ReferencedValue({ offset: maxUint64, length: 0 }, valueBuf)
          const iter = bptree.iter(valueRef);
          while (await iter.prev()) {
            const currentKey = iter.getKey();

            if (ReferencedValue.compareBytes(valueBuf, currentKey.value) >= 0) {
              const [_, mp] = await bptree.find(currentKey);

              const data = await this.dataFile.get(
                Number(mp.offset),
                Number(mp.offset) + mp.length - 1
              );

              yield handleSelect(data, query.select);
            }
          }
        } else {
          const firstKey = await bptree.first();
          const iter = bptree.iter(firstKey);

          while (await iter.next()) {
            const currentKey = iter.getKey();

            if (ReferencedValue.compareBytes(valueBuf, currentKey.value) >= 0) {
              const [_, mp] = await bptree.find(currentKey);

              const data = await this.dataFile.get(
                Number(mp.offset),
                Number(mp.offset) + mp.length - 1
              );

              yield handleSelect(data, query.select);
            }
          }
        }
      } else if (operation === "<") {
        if (ord === "DESC") {
          const valueRef = new ReferencedValue({ offset: maxUint64, length: 0 }, valueBuf)
          const iter = bptree.iter(valueRef);
          while (await iter.prev()) {
            const currentKey = iter.getKey();

            if (ReferencedValue.compareBytes(valueBuf, currentKey.value) === 1) {
              const [_, mp] = await bptree.find(currentKey);

              const data = await this.dataFile.get(
                Number(mp.offset),
                Number(mp.offset) + mp.length - 1
              );

              yield handleSelect(data, query.select);
            }
          }
        } else {
          const firstKey = await bptree.first();
          const iter = bptree.iter(firstKey);

          while (await iter.next()) {
            const currentKey = iter.getKey();

            if (ReferencedValue.compareBytes(valueBuf, currentKey.value) === 1) {
              const [_, mp] = await bptree.find(currentKey);

              const data = await this.dataFile.get(
                Number(mp.offset),
                Number(mp.offset) + mp.length - 1
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
    value: T[keyof T]
  ): QueryBuilder<T> {
    return new QueryBuilder(this).where(key, operation, value);
  }
}
