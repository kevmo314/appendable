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

export class Database<T extends Schema> {
  private constructor(
    private dataFile: DataFile,
    private indexFile: VersionedIndexFile<T>
  ) {}

  static forDataFileAndIndexFile<T = any>(
    dataFile: DataFile,
    indexFile: VersionedIndexFile<T>
  ) {
    return new Database(dataFile, indexFile);
  }

  async fields() {
    return await this.indexFile.indexHeaders();
  }

  private async evaluate(where: WhereNode<T>) {
    if (where.operation === "AND") {
      const results = [];
      return;
    } else if (where.operation === "OR") {
      return;
    }
    // simple query, hooray!
    // evaluate the query by fetching the corresponding index records
    return this.indexFile.indexRecords(where.key, where.operation, where.value);
  }

  query(query: Query<any>) {
    // recursively evaluate the where condition
  }
}
