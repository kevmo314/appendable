import { DataFile } from "./data-file";
import { IndexFile, VersionedIndexFile } from "./index-file";

type Schema = {
  [key: string]: {};
};

type WhereLeafNode<T extends Schema> = {
  operation: "<" | "<=" | "==" | ">=" | ">";
  key: keyof T;
  value: T[typeof this.key];
};

type WhereIntermediateNode<T extends Schema> = {
  operation: "AND" | "OR";
  values: WhereNode<T>[];
};

type WhereNode<T extends Schema> = WhereIntermediateNode<T> | WhereLeafNode<T>;

type OrderBy<T extends Schema> = {
  key: keyof Schema;
  direction: "ASC" | "DESC";
};

type Query<T extends Schema> = {
  where?: WhereNode<T>;
  orderBy?: OrderBy<T>;
  limit?: number;
};

function parseIgnoringSuffix(x: string) {
  // TODO: implement a proper parser.
  try {
    return JSON.parse(x);
  } catch (e) {
    let m = e.message.match(/position\s+(\d+)/);
    if (m) {
      x = x.slice(0, m[1]);
    }
  }
  return JSON.parse(x);
}

function fieldRank(token: any) {
  if (token === null) {
    return 1;
  }
  if (typeof token === "boolean") {
    return 2;
  }
  if (typeof token === "number" || typeof token === "bigint") {
    return 3;
  }
  if (typeof token === "string") {
    return 4;
  }
  throw new Error("unknown type");
}

function cmp(a: any, b: any) {
  const atr = fieldRank(a);
  const btr = fieldRank(b);
  if (atr !== btr) {
    return atr - btr;
  }
  switch (atr) {
    case 1:
      return 0;
    case 2:
      return a ? 1 : -1;
    case 3:
      return a - b;
    case 4:
      return a.localeCompare(b);
    default:
      throw new Error("unknown type");
  }
}

export class Database<T extends Schema> {
  private constructor(
    private dataFile: DataFile,
    private indexFile: VersionedIndexFile<T>
  ) {}

  static forDataFileAndIndexFile<T extends Schema>(
    dataFile: DataFile,
    indexFile: VersionedIndexFile<T>
  ) {
    return new Database(dataFile, indexFile);
  }

  async fields() {
    return await this.indexFile.indexHeaders();
  }

  /**
   * @param op
   * @returns the first index record that is greater than or equal to the query value.
   */
  private async indexRecordsBinarySearch(key: keyof T, value: any) {
    const headers = await this.indexFile.indexHeaders();
    const header = headers.find((header) => header.fieldName === key);
    if (!header) {
      throw new Error("field not found");
    }
    let start = 0;
    let end = Number(header.indexRecordCount);
    while (start + 1 < end) {
      const mid = Math.floor((start + end) / 2);
      const indexRecord = await this.indexFile.indexRecord(key, mid);
      const dataFieldValue = parseIgnoringSuffix(
        await this.dataFile.get(
          indexRecord.fieldStartByteOffset,
          indexRecord.fieldEndByteOffset
        )
      );
      if (cmp(value, dataFieldValue) < 0) {
        end = mid;
      } else if (cmp(value, dataFieldValue) > 0) {
        start = mid + 1;
      } else {
        return indexRecord;
      }
    }
    return await this.indexFile.indexRecord(key, start);
  }

  private async dataRecordsBinarySearch(byteOffset: number) {
    const indexFileHeader = await this.indexFile.indexFileHeader();

    let start = 0;
    let end = indexFileHeader.dataCount;
    // binary search to find the first data record that has an endByteOffset
    // greater than or equal to the byteOffset.
    console.log(byteOffset);
    while (start + 1 < end) {
      const mid = Math.floor((start + end) / 2);
      const dataRecord = await this.indexFile.dataRecord(mid);
      if (dataRecord.endByteOffset < byteOffset) {
        start = mid + 1;
      } else {
        end = mid;
      }
    }
    return await this.indexFile.dataRecord(start);
  }

  private async evaluate(where: WhereNode<T>) {
    if (where.operation === "AND") {
      const results = [];
      return;
    } else if (where.operation === "OR") {
      return;
    }
    const op = where as WhereLeafNode<T>; // ts doesn't seem to pick this up automatically.
    // simple query, hooray!
    // evaluate the query by binary searching on the relevant index finding the
    // first record that matches the query.
    const indexRecord = await this.indexRecordsBinarySearch(op.key, op.value);
    // then, binary search the data file to find the first record that contains
    // the startByteOffset of the index record.
    const dataRecord = await this.dataRecordsBinarySearch(
      indexRecord.fieldStartByteOffset
    );
    // finally, read the data file from the startByteOffset to the endByteOffset
    // of the data record.
    const data = await this.dataFile.get(
      dataRecord.startByteOffset,
      dataRecord.endByteOffset
    );
    console.log(data);
  }

  query(query: Query<any>) {
    // recursively evaluate the where condition
    if (query.where) {
      return this.evaluate(query.where);
    }
  }
}
