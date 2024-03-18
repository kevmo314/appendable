import { RangeResolver } from "../../src/resolver";
import { LinkedMetaPage } from "../../src/btree/multi";

const META_SIZE_BYTES = 256;
export const PAGE_SIZE_BYTES = 4096;

export class PageFile {
  private readonly resolver: RangeResolver;
  private readonly offset: bigint;
  private readonly pageData?: ArrayBuffer;
  private pageDataPromise?: Promise<
    { data: ArrayBuffer; totalLength: number }[]
  >;

  constructor(resolver: RangeResolver, offset: bigint, data?: ArrayBuffer) {
    this.offset = offset;
    this.resolver = resolver;
    this.pageData = data;
  }

  private async getPage(): Promise<ArrayBuffer> {
    if (this.pageData) {
      return this.pageData;
    }

    if (!this.pageDataPromise) {
      this.pageDataPromise = this.resolver([
        {
          start: Number(this.offset),
          end: Number(this.offset) + PAGE_SIZE_BYTES - 1,
        },
      ]);
    }

    const res = await this.pageDataPromise;
    const { data } = res[0];
    return data;
  }

  async splitPage(): Promise<LinkedMetaPage[]> {
    const pageData = await this.getPage();

    let mps = [];
    for (
      let slotIdx = 0;
      slotIdx <= PAGE_SIZE_BYTES / META_SIZE_BYTES - 1;
      slotIdx++
    ) {
      const slotOffset = this.offset + BigInt(slotIdx * META_SIZE_BYTES);

      mps.push(
        new LinkedMetaPage(pageData.slice(Number(slotOffset), META_SIZE_BYTES)),
      );
    }

    return mps;
  }
}

export function ReadMultiBPTree(
  resolver: RangeResolver,
  idx: number,
): PageFile {
  let offset = idx < 0 ? BigInt(0) : BigInt(idx + 1) * BigInt(PAGE_SIZE_BYTES);
  return new PageFile(resolver, offset);
}
