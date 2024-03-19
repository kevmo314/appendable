import { RangeResolver } from "../resolver";
import { arrayBufferToString, readBinaryFile } from "./test-util";
import { ReadMultiBPTree } from "../btree/pagefile";

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
    const ms = await tree.splitPage();
    const metadata = await ms[0].metadata();

    expect("hello").toEqual(arrayBufferToString(metadata));
  });
});
