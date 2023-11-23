import { RangeResolver } from "./resolver";

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
      fieldType: bigint;
      indexRecordCount: bigint;
    }[]
  >;
  indexRecord(
    field: keyof T,
    offset: number
  ): Promise<{
    dataNumber: bigint;
    fieldStartByteOffset: number;
    fieldEndByteOffset: number;
  }>;
  dataRecord(
    offset: number
  ): Promise<{ startByteOffset: number; endByteOffset: number }>;
}

class IndexFileV1<T> implements VersionedIndexFile<T> {
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

  async indexRecord(field: keyof T, offset: number) {
    if (offset < 0) {
      throw new Error("offset out of range");
    }
    const headers = await this.indexHeaders();
    const header = headers.find((header) => header.fieldName === field);
    if (!header) {
      throw new Error("field not found");
    }
    if (offset >= Number(header.indexRecordCount)) {
      throw new Error("offset out of range");
    }
    const indexFileHeader = await this.indexFileHeader();
    const start = 17 + indexFileHeader.indexLength + offset * 16;
    const buffer = await this.resolver(start, start + 16);
    const data = new DataView(buffer);

    const dataNumber = data.getBigUint64(0);
    const fieldStartByteOffset = data.getUint32(8);
    const fieldEndByteOffset = data.getUint32(12);

    const indexRecordsLength = headers.reduce((acc, header) => {
      return acc + Number(header.indexRecordCount) * 16;
    }, 0);

    const dataRecordsStart =
      17 + indexFileHeader.indexLength + indexRecordsLength;

    return {
      dataNumber,
      fieldStartByteOffset: fieldStartByteOffset + dataRecordsStart,
      fieldEndByteOffset: fieldEndByteOffset + dataRecordsStart,
    };
  }

  async dataRecord(offset: number) {
    if (offset < 0) {
      throw new Error("offset out of range");
    }
    const indexFileHeader = await this.indexFileHeader();
    if (offset >= indexFileHeader.dataCount) {
      throw new Error("offset out of range");
    }
    const headers = await this.indexHeaders();
    const indexRecordsLength = headers.reduce((acc, header) => {
      return acc + Number(header.indexRecordCount) * 16;
    }, 0);
    const start = 17 + indexFileHeader.indexLength + indexRecordsLength;
    // fetch the byte offsets. if offset is 0, we can just fetch the first 8 bytes.
    if (offset === 0) {
      const buffer = await this.resolver(
        start + offset * 16,
        start + offset * 16 + 8
      );
      const data = new DataView(buffer);
      const endByteOffset = data.getBigUint64(0);
      return {
        startByteOffset: 0,
        endByteOffset: Number(endByteOffset),
      };
    }

    const buffer = await this.resolver(
      start + (offset - 1) * 16,
      start + offset * 16 + 8
    );
    const data = new DataView(buffer);
    const startByteOffset = data.getBigUint64(0);
    const endByteOffset = data.getBigUint64(8);
    return {
      startByteOffset: Number(startByteOffset),
      endByteOffset: Number(endByteOffset),
    };
  }
}
