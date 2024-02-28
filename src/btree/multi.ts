import { RangeResolver } from "../resolver";
import { MemoryPointer } from "./node";

const N = 16;
export const PAGE_SIZE_BYTES = 4096;
export const maxUint64 = 2n ** 64n - 1n;

export class LinkedMetaPage {
  private resolver: RangeResolver;
  private offset: bigint;
  private metaPageData?: ArrayBuffer;

  constructor(resolver: RangeResolver, offset: bigint, data?: ArrayBuffer) {
    this.resolver = resolver;
    this.offset = offset;
    this.metaPageData = data;
  }

  async root(): Promise<MemoryPointer> {
    const pageData = await this.getMetaPage();

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

  async metadata(): Promise<ArrayBuffer> {
    const pageData = await this.getMetaPage();

    const lengthView = new DataView(pageData, 8 * N + 16);

    // read the first four because that represents length
    const metadataLength = lengthView.getUint32(0, true);
    const start = 8 * N + 20;

    console.log(`metadata length: ${metadataLength}`);

    return pageData.slice(start, start + metadataLength);
  }

  public setMetaPage(pageData: ArrayBuffer) {
    this.metaPageData = pageData;
  }

  private async getMetaPage(): Promise<ArrayBuffer> {
    if (!this.metaPageData) {
      const res = await this.resolver([
        {
          start: Number(this.offset),
          end: Number(this.offset) + PAGE_SIZE_BYTES - 1,
        },
      ]);

      const { data } = res[0];
      return data;
    }

    return this.metaPageData;
  }

  async nextNOffsets(): Promise<bigint[]> {
    const pageData = await this.getMetaPage();

    const view = new DataView(pageData, 12);

    let offsets: bigint[] = [];

    for (let idx = 0; idx <= N - 1; idx++) {
      const nextOffset = view.getBigUint64(idx * 8, true);

      if (nextOffset === maxUint64) {
        break;
      }

      offsets.push(nextOffset);
    }

    return offsets;
  }

  getOffset(): bigint {
    return this.offset;
  }
}

export function ReadMultiBPTree(
  resolver: RangeResolver,
  idx: number,
): LinkedMetaPage {
  let offset = idx < 0 ? BigInt(0) : BigInt(idx + 1) * BigInt(PAGE_SIZE_BYTES);
  return new LinkedMetaPage(resolver, offset);
}
