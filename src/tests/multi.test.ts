import { RangeResolver } from "../resolver/resolver";
import { arrayBufferToString, readBinaryFile } from "./test-util";
import { ReadMultiBPTree } from "../file/multi";

describe("test metadata", () => {
  let mockMetadata: Uint8Array;

  beforeAll(async () => {
    mockMetadata = await readBinaryFile("filled_metadata.bin");
  });

  it("reads stored metadata", async () => {
    const mockRangeResolver: RangeResolver = async ([{ start, end }]) => {
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
