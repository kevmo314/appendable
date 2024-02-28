import { RangeResolver } from "../resolver";
import { MemoryPointer } from "./node";
import { PageFile } from "./pagefile";

const N = 16;
const PAGE_SIZE_BYTES = 4096;
export const maxUint64 = 2n ** 64n - 1n;

export class LinkedMetaPage {
  private resolver: RangeResolver;
  private offset: bigint;
  private metaPagePromise?: Promise<
    { data: ArrayBuffer; totalLength: number }[]
  >;

  constructor(resolver: RangeResolver, offset: bigint) {
    this.resolver = resolver;
    this.offset = offset;
  }

  async root(): Promise<MemoryPointer> {
    const res = await this.getMetaPage();
    const { data: pageData } = res[0];

    // we seek by 12 bytes since offset is 8 bytes, length is 4 bytes
    const data = pageData.slice(0, 12);
    const view = new DataView(data);

    const pointerOffset = view.getBigUint64(0, true);
    const lengthOffset = view.getUint32(8, true);

    return {
      offset: pointerOffset,
      length: lengthOffset,
    };
  }

  /**
   * `metadata()` gets the page data. It does the following:
   * 		(1) creates a slice from 24 to the end of the page
   * 		(2) it reads the first four bytes of that slice which gives us the length to seek to
   * 		(3) slices from [24, (24 + dataLength)] which contain metadata
   */
  async metadata(): Promise<ArrayBuffer> {
    const res = await this.getMetaPage();
    const { data: pageData } = res[0];

    const lengthView = new DataView(pageData, 8 * N + 16);

    // read the first four because that represents length
    const metadataLength = lengthView.getUint32(0, true);
    const start = 8 * N + 20;

    return pageData.slice(start, start + metadataLength);
  }

  private async getMetaPage(): Promise<
    { data: ArrayBuffer; totalLength: number }[]
  > {
    if (!this.metaPagePromise) {
      this.metaPagePromise = this.resolver([
        {
          start: Number(this.offset),
          end: Number(this.offset) + PAGE_SIZE_BYTES - 1,
        },
      ]);
    }

    return this.metaPagePromise;
  }

  /**
   * `next()` - returns a new LinkedMetaPage
   */
  async next(): Promise<LinkedMetaPage | null> {
    const res = await this.getMetaPage();
    const { data: pageData } = res[0];

    const view = new DataView(pageData, 12, 8);
    const nextOffset = view.getBigUint64(0, true);

    if (nextOffset === maxUint64) {
      return null;
    }

    return new LinkedMetaPage(this.resolver, nextOffset);
  }

  async nextNOffsets(): Promise<bigint[] | null> {
    const res = await this.getMetaPage();
    const { data: pageData } = res[0];

    const view = new DataView(pageData, 12);

    let offsets: bigint[] = [];

    for (let idx = 0; idx <= N - 1; idx++) {
      const nextOffset = view.getBigUint64(idx * N, true);

      if (nextOffset === maxUint64) {
        break;
      }

      offsets.push(nextOffset);
    }

    return offsets.length > 0 ? offsets : null;
  }

  getOffset(): bigint {
    return this.offset;
  }
}

export function ReadMultiBPTree(
  resolver: RangeResolver,
  pageFile: PageFile,
): LinkedMetaPage {
  const offset = pageFile.page(0);
  return new LinkedMetaPage(resolver, offset);
}
