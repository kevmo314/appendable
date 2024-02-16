import { LengthIntegrityError, RangeResolver } from "../resolver";
import { PageFile } from "../btree/pagefile";
import { ReadMultiBPTree } from "../btree/multi";
import { arrayBufferToString, readBinaryFile } from "./test-util";

describe("test multi", () => {
  it("storing metadata works", async () => {
    const mockRangeResolver: RangeResolver = async ({
      start,
      end,
      expectedLength,
    }) => {
      const bufferSize = 4096 * 2; // 2 blocks of 4kb each
      const buffer = new ArrayBuffer(bufferSize);
      const view = new Uint8Array(buffer);

      const metadata = await readBinaryFile("filled_metadata.bin");
      const metadataLength = metadata.byteLength;

      const dataView = new DataView(buffer);
      dataView.setUint32(4096 + 24, metadataLength);

      view.set(metadata, 4096 + 24 + 4);
      const slice = view.slice(start, end);

      if (expectedLength !== undefined && slice.byteLength !== expectedLength) {
        throw new LengthIntegrityError();
      }

      return {
        data: slice.buffer,
        totalLength: view.byteLength,
      };
    };

    const pageFile = new PageFile(mockRangeResolver);
    const tree = ReadMultiBPTree(mockRangeResolver, pageFile);
    const metadata = await tree.metadata();

    expect("hello").toEqual(arrayBufferToString(metadata));
  });

  it("traversing pages", async () => {
    const mockRangeResolver: RangeResolver = async ({
      start,
      end,
      expectedLength,
    }) => {
      const pageSize = 4096;
      const headerSize = 12;
      const bufferSize = pageSize * 5; // 1 for gc, 1 for file meta, 3 pages
      const buffer = new ArrayBuffer(bufferSize);
      const view = new Uint8Array(buffer);
      const maxUint64 = 2n ** 64n - 1n;

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
          dataView.setBigUint64(offsetPosition, nextPageOffset);
        }
      }

      const slice = view.slice(start, Math.min(end, bufferSize));
      if (expectedLength !== undefined && slice.byteLength !== expectedLength) {
        throw new LengthIntegrityError();
      }

      return {
        data: slice.buffer,
        totalLength: view.byteLength,
      };
    };

    const pageFile = new PageFile(mockRangeResolver);
    const tree = ReadMultiBPTree(mockRangeResolver, pageFile);

    // this is the file meta
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
