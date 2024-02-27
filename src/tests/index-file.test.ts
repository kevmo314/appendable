import { IndexFileV1 } from "../index-file/index-file";
import { FileFormat } from "../index-file/meta";
import { RangeResolver } from "../resolver";
import { readBinaryFile } from "./test-util";

describe("test index-file parsing", () => {
  let mockRangeResolver: RangeResolver;
  let indexFileSize: number;
  let indexFile: Uint8Array;

  beforeAll(async () => {
    indexFile = await readBinaryFile("green_tripdata_2023-01.index");
    indexFileSize = indexFile.byteLength;
  });

  beforeEach(() => {
    mockRangeResolver = async ([{ start, end }]) => {
      const slicedPart = indexFile.slice(start, end + 1);

      const arrayBuffer = slicedPart.buffer.slice(
        slicedPart.byteOffset,
        slicedPart.byteOffset + slicedPart.byteLength,
      );

      return [
        {
          data: arrayBuffer,
          totalLength: arrayBuffer.byteLength,
        },
      ];
    };
  });

  it("should read the file meta", async () => {
    const indexFile = new IndexFileV1(mockRangeResolver);
    const fileMeta = await indexFile.metadata();

    expect(fileMeta.format).toEqual(FileFormat.JSONL);
    expect(fileMeta.version).toEqual(1);
  });

  it("should traverse the entire index file and retrieve the index headers", async () => {
    const indexFile = new IndexFileV1(mockRangeResolver);
    const indexMetas = await indexFile.indexHeaders();

    expect(indexMetas.length).toEqual(20);
  });
});
