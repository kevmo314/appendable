import { RangeResolver } from "../resolver/resolver";
import { MemoryPointer } from "../btree/node";

export const PAGE_SIZE_BYTES = 4096;
export const SLOT_SIZE_BYTES = 256;
export const maxUint64 = 2n ** 64n - 1n;
const POINTER_BYTES = 8;
const LENGTH_BYTES = 4;
const COUNT_BYTE = 1;

export class LinkedMetaPage {
  private metaPageDataPromise?: Promise<
    { data: ArrayBuffer; totalLength: number }[]
  >;

  constructor(
    private readonly resolver: RangeResolver,
    private readonly offset: bigint,
    private readonly index: number,
  ) {}

  async root(): Promise<MemoryPointer> {
    const pageData = await this.getMetaPage();

    // we seek by 12 bytes since offset is 8 bytes, length is 4 bytes
    const data = pageData.slice(
      this.rootMemoryPointerPageOffset(),
      this.rootMemoryPointerPageOffset() + POINTER_BYTES + LENGTH_BYTES,
    );

    if (data.byteLength != POINTER_BYTES + LENGTH_BYTES) {
      throw new Error(
        `failed to properly fetch root node. Got ${data.byteLength}`,
      );
    }

    const view = new DataView(data);

    const pointerOffset = view.getBigUint64(0, true);
    const lengthOffset = view.getUint32(POINTER_BYTES, true);

    return {
      offset: pointerOffset,
      length: lengthOffset,
    };
  }

  async metadata(): Promise<ArrayBuffer> {
    const pageData = await this.getMetaPage();
    const rootPointer = POINTER_BYTES + LENGTH_BYTES;
    const metadata = pageData.slice(
      this.rootMemoryPointerPageOffset() + rootPointer,
    );
    const metadataView = new DataView(metadata);
    // we need to seek past the root pointer
    const metadataLength = metadataView.getUint8(0);
    return metadataView.buffer.slice(1, 1 + metadataLength);
  }

  private async getMetaPage(): Promise<ArrayBuffer> {
    if (!this.metaPageDataPromise) {
      this.metaPageDataPromise = this.resolver([
        {
          start: Number(this.offset),
          end: Number(this.offset) + PAGE_SIZE_BYTES - 1,
        },
      ]);
    }

    const res = await this.metaPageDataPromise;
    const { data } = res[0];

    return data;
  }

  async next() {
    const pageData = await this.getMetaPage();
    const view = new DataView(pageData);

    const count = view.getUint8(POINTER_BYTES);

    if (this.index < count - 1) {
      return new LinkedMetaPage(this.resolver, this.offset, this.index + 1);
    }

    const nextOffset = view.getBigUint64(0, true);

    if (nextOffset === maxUint64) {
      return null;
    }

    return new LinkedMetaPage(this.resolver, nextOffset, 0);
  }

  private rootMemoryPointerPageOffset(): number {
    return (
      POINTER_BYTES +
      COUNT_BYTE +
      this.index * (POINTER_BYTES + COUNT_BYTE + SLOT_SIZE_BYTES)
    );
  }
}

export function ReadMultiBTree(
  resolver: RangeResolver,
  idx: number,
): LinkedMetaPage {
  let offset = idx < 0 ? BigInt(0) : BigInt(idx + 1) * BigInt(PAGE_SIZE_BYTES);
  return new LinkedMetaPage(resolver, offset, 0);
}
