import { RangeResolver } from "./resolver";

type IndexRecords = {
  startByteOffset: number;
  endByteOffset: number;
  count: number;
};

export class IndexFile<T> {
  static async forUrl<T = any>(url: string) {
    return await IndexFile.forResolver<T>(
      async (start: number, end: number) => {
        const response = await fetch(url, {
          headers: { Range: `bytes=${start}-${end}` },
        });
        return await response.arrayBuffer();
      }
    );
  }

  static async forResolver<T = any>(
    resolver: RangeResolver
  ): Promise<VersionedIndexFile<T>> {
    const version = new DataView(await resolver(0, 0)).getUint8(0);
    switch (version) {
      case 1:
        return new IndexFileV1<T>(resolver);
      default:
        throw new Error("invalid version");
    }
  }
}

export interface VersionedIndexFile<T> {
  indexFileHeader(): Promise<{
    indexLength: number;
    dataCount: number;
  }>;
  indexHeaders(): Promise<
    {
      fieldName: string;
      fieldType: number;
      indexRecordCount: bigint;
    }[]
  >;
  // indexRecords(
  //   field: keyof T,
  //   condition: "<" | "<=" | "==" | ">=" | ">",
  //   value: any
  // ): Promise<IndexRecords[]>;
}

class IndexFileV1<T> {
  constructor(private resolver: RangeResolver) {}

  async indexFileHeader() {
    const header = new DataView(await this.resolver(1, 16));
    return {
      indexLength: Number(header.getBigUint64(0)),
      dataCount: Number(header.getBigUint64(8)),
    };
  }

  async indexHeaders() {
    const indexFileHeader = await this.indexFileHeader();
    const buffer = await this.resolver(17, indexFileHeader.indexLength + 16);
    const data = new DataView(buffer);
    const headers: {
      fieldName: string;
      fieldType: bigint;
      indexRecordCount: bigint;
    }[] = [];
    let offset = 0;
    while (offset < indexFileHeader.indexLength) {
      const fieldNameLength = data.getUint32(offset);
      offset += 4;
      const fieldName = new TextDecoder("utf-8").decode(
        buffer.slice(offset, offset + fieldNameLength)
      );
      offset += fieldNameLength;
      const fieldType = data.getBigUint64(offset);
      offset += 8;
      const indexRecordCount = data.getBigUint64(offset);
      offset += 8;
      headers.push({
        fieldName,
        fieldType,
        indexRecordCount,
      });
    }
    if (offset !== indexFileHeader.indexLength) {
      throw new Error(
        `Inaccurate header read, offset = ${offset} but indexFileHeader.indexLength = ${indexFileHeader.indexLength}. This could indicate that the index file is corrupt.`
      );
    }
    return headers;
  }

  async indexRecords(
    field: keyof T,
    condition: "<" | "<=" | "==" | ">=" | ">",
    value: any
  ) {
    const headers = await this.indexHeaders();
    const header = headers.find((header) => header.fieldName === field);
    if (!header) {
      throw new Error("field not found");
    }
    const state = {
      startByteOffset: 1,
    };
  }
}
