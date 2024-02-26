import { RangeResolver } from "../resolver";

const pageSizeBytes = 4096;

export class PageFile {
  private resolver: RangeResolver;
  private pageSize: number = pageSizeBytes;

  constructor(resolver: RangeResolver) {
    this.resolver = resolver;
  }

  async readPage(idx: number): Promise<ArrayBuffer> {
    if (idx < 0) {
      throw new Error("page cannot be indexed");
    }

    const start = (idx + 1) * this.pageSize;
    const end = start + this.pageSize - 1;

    const res = await this.resolver([{ start, end: end - 1 }]);
    const { data } = res[0];
    return data;
  }

  page(idx: number): bigint {
    if (idx < 0) {
      return BigInt(0);
    }

    return BigInt(idx + 1) * BigInt(this.pageSize);
  }
}
