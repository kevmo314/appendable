import { MemoryPointer } from "./node";
export const N = 16;
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

    const lengthView = new DataView(pageData, 8 * N + 16);

    // read the first four because that represents length
    const metadataLength = lengthView.getUint32(0, true);
    const start = 8 * N + 20;

    return pageData.slice(start, start + metadataLength);
  }
}
