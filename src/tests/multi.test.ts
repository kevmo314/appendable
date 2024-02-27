import { LengthIntegrityError, RangeResolver } from "../resolver";
import { PageFile } from "../btree/pagefile";
import { ReadMultiBPTree, maxUint64 } from "../btree/multi";
import { arrayBufferToString, readBinaryFile } from "./test-util";

describe("test metadata", () => {
  let mockMetadata: Uint8Array;

  beforeAll(async () => {
    mockMetadata = await readBinaryFile("filled_metadata.bin");
  });

  it("reads stored metadata", async () => {
    const mockRangeResolver: RangeResolver = async ([
      { start, end, expectedLength },
    ]) => {
      const bufferSize = 4096 * 2;
      const buffer = new ArrayBuffer(bufferSize);
      const view = new Uint8Array(buffer);

      const metadataLength = mockMetadata.byteLength;

      const dataView = new DataView(buffer);
      dataView.setUint32(4096 + 24, metadataLength, true);

      view.set(mockMetadata, 4096 + 24 + 4);
      const slice = view.slice(start, end);

      if (expectedLength !== undefined && slice.byteLength !== expectedLength) {
        throw new LengthIntegrityError();
      }

      return [
        {
          data: slice.buffer,
          totalLength: view.byteLength,
        },
      ];
    };

    const pageFile = new PageFile(mockRangeResolver);
    const tree = ReadMultiBPTree(mockRangeResolver, pageFile);
    const metadata = await tree.metadata();

    expect("hello").toEqual(arrayBufferToString(metadata));
  });
});

describe("traversing multi pages", () => {
  it("traversing pages", async () => {
    const mockRangeResolver: RangeResolver = async ([
      { start, end, expectedLength },
    ]) => {
      const pageSize = 4096;
      const headerSize = 12;
      const bufferSize = pageSize * 5; // 1 for gc, 1 for file meta, 3 pages
      const buffer = new ArrayBuffer(bufferSize);
      const view = new Uint8Array(buffer);

      for (let i = 0; i < 4; i++) {
        let nextPageOffset;
        if (i < 3) {
          nextPageOffset = BigInt((i + 2) * pageSize);
        } else {
          nextPageOffset = maxUint64;
        }

        const offsetPosition = pageSize * (i + 1) + headerSize;

        if (offsetPosition + 8 <= bufferSize) {
          const dataView = new DataView(buffer);
          dataView.setBigUint64(offsetPosition, nextPageOffset, true);
        }
      }

      const slice = view.slice(start, Math.min(end, bufferSize));
      if (expectedLength !== undefined && slice.byteLength !== expectedLength) {
        throw new LengthIntegrityError();
      }

      return [
        {
          data: slice.buffer,
          totalLength: view.byteLength,
        },
      ];
    };

    const pageFile = new PageFile(mockRangeResolver);
    const tree = ReadMultiBPTree(mockRangeResolver, pageFile);

    expect(tree.getOffset()).toEqual(BigInt(4096));

    const page1 = await tree.next();
    if (page1 === null) {
      expect(page1).not.toBeNull();
      return;
    }
    expect(page1.getOffset()).toEqual(BigInt(4096 * 2));

    const page2 = await page1.next();
    if (page2 === null) {
      expect(page2).not.toBeNull();
      return;
    }
    expect(page2.getOffset()).toEqual(BigInt(4096 * 3));

    const page3 = await page2.next();
    if (page3 === null) {
      expect(page3).not.toBeNull();
      return;
    }
    expect(page3.getOffset()).toEqual(BigInt(4096 * 4));

    const page4 = await page3.next();
    expect(page4).toBeNull();
  });
});
