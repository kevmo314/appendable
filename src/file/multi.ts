import { RangeResolver } from "../resolver/resolver";
import { MemoryPointer } from "../btree/node";

export const PAGE_SIZE_BYTES = 4096;
export const maxUint64 = 2n ** 64n - 1n;

export class LinkedMetaPage {
  private readonly metaPageData: ArrayBuffer;

  constructor(data: ArrayBuffer) {
    this.metaPageData = data;
  }

  async root(): Promise<MemoryPointer> {
    const pageData = this.metaPageData;

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
    const pageData = this.metaPageData;

    const lengthView = new DataView(pageData, 24);

    // read the first four because that represents length
    const metadataLength = lengthView.getUint32(0, true);

    return pageData.slice(28, 28 + metadataLength);
  }

  async next(): Promise<bigint | null> {
    const pageData = this.metaPageData;
    const view = new DataView(pageData, 12, 8);
    const nextOffset = view.getBigUint64(0, true);

    if (nextOffset === maxUint64) {
      return null;
    }

    return nextOffset;
  }
}
