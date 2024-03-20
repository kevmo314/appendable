import { LengthIntegrityError, RangeResolver } from "../resolver/resolver";
import { N, PAGE_SIZE_BYTES, ReadMultiBPTree } from "../btree/multi";
import { arrayBufferToString, readBinaryFile } from "./test-util";
const maxUint64 = 2n ** 64n - 1n;

describe("test metadata", () => {
  let mockMetadata: Uint8Array;

  beforeAll(async () => {
    mockMetadata = await readBinaryFile("filled_metadata.bin");
  });

  it("reads stored metadata", async () => {
    const mockRangeResolver: RangeResolver = async ([
      { start, end, expectedLength },
    ]) => {
      return [
        {
          data: mockMetadata.buffer.slice(start, end),
          totalLength: end - start + 1,
        },
      ];
    };

    const tree = ReadMultiBPTree(mockRangeResolver, 0);
    const metadata = await tree.metadata();

    expect("hello").toEqual(arrayBufferToString(metadata));
  });
});
