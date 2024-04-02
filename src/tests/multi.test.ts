import { RangeResolver } from "../resolver/resolver";
import { arrayBufferToString, readBinaryFile } from "./test-util";
import { ReadMultiBPTree } from "../file/pagefile";

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
    const ms = await tree.splitPage();
    const metadata = await ms[0].metadata();

    expect("hello").toEqual(arrayBufferToString(metadata));
  });
});

describe("paloalto metadata", () => {
  let mockMetadata: Uint8Array;

  beforeAll(async () => {
    mockMetadata = await readBinaryFile("palo-alto.index");
  });

  it("reads paloalto metadata", async () => {
    const mockRangeResolver: RangeResolver = async ([{ start, end }]) => {
      return [
        {
          data: mockMetadata.buffer.slice(start, end),
          totalLength: end - start + 1,
        },
      ];
    };

    const tree = ReadMultiBPTree(mockRangeResolver, 0);
    const ms = await tree.splitPage();

    expect(ms.length).toBeGreaterThan(1); // expected to have the fol
  });
});
