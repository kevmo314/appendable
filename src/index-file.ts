import { RangeResolver } from "./resolver";

type IndexRecords = {
  startByteOffset: number;
  endByteOffset: number;
  count: number;
};

export class IndexFile<T> {
  static async forUrl(url: string) {
    return await IndexFile.forResolver(async (start: number, end: number) => {
      const response = await fetch(url, {
        headers: { Range: `bytes=${start}-${end}` },
      });
      return await response.arrayBuffer();
    });
  }

  static async forResolver(resolver: RangeResolver) {
    const version = new DataView(await resolver(0, 0)).getUint8(0);
    switch (version) {
      case 1:
        return new IndexFileV1(resolver);
      default:
        throw new Error("invalid version");
    }
  }
}

class IndexFileV1<T> {
  constructor(private resolver: RangeResolver) {}

  async indexFileHeader() {
    const header = new DataView(await this.resolver(1, 16));
    return {
      indexLength: Number(header.getBigUint64(0)),
      dataCount: Number(header.getBigUint64(4)),
    };
  }

  async indexHeaders() {
    const indexFileHeader = await this.indexFileHeader();
    const buffer = await this.resolver(17, indexFileHeader.indexLength + 16);
    const data = new DataView(buffer);
    const headers: {
      fieldName: string;
      fieldType: number;
      indexRecordCount: bigint;
    }[] = [];
    let offset = 0;
    while (offset < indexFileHeader.indexLength) {
      const fieldNameLength = data.getUint32(offset);
      const fieldName = new TextDecoder("utf-8").decode(
        buffer.slice(offset + 1, offset + 1 + fieldNameLength)
      );
      const fieldType = data.getUint8(offset + 1 + fieldNameLength);
      const indexRecordCount = data.getBigUint64(
        offset + 1 + fieldNameLength + 1
      );
      headers.push({
        fieldName,
        fieldType,
        indexRecordCount,
      });
      offset += 1 + fieldNameLength + 1 + 8;
    }
    if (offset !== indexFileHeader.indexLength) {
      throw new Error("inaccurate header read");
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
