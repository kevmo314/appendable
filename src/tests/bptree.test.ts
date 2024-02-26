import { BPTree, MetaPage, ReferencedValue } from "../btree/bptree";
import { MemoryPointer } from "../btree/node";
import { FieldType } from "../db/database";
import { FileFormat } from "../index-file/meta";
import { RangeResolver } from "../resolver";
import { readBinaryFile } from "./test-util";

class testMetaPage implements MetaPage {
  private rootMP: MemoryPointer;

  constructor(mp: MemoryPointer) {
    this.rootMP = mp;
  }

  async root(): Promise<MemoryPointer> {
    return this.rootMP;
  }
}

describe("test btree", () => {
  let mockRangeResolver: RangeResolver;
  let mockDataFileResolver: RangeResolver;
  let bptree: BPTree;

  beforeEach(() => {
    mockDataFileResolver = async ([{ start, end }]) => {
      return [
        {
          data: new ArrayBuffer(0),
          totalLength: 0,
        },
      ];
    };

    mockRangeResolver = async ([{ start, end }]) => {
      const indexFile = await readBinaryFile("bptree_1.bin");
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

    const page = new testMetaPage({ offset: 8192n, length: 88 });
    bptree = new BPTree(
      mockRangeResolver,
      page,
      mockDataFileResolver,
      FileFormat.CSV,
      FieldType.Float64,
    );
  });

  it("should read a bptree and find items", async () => {
    let idx = 1;
    for (const value of ["hello", "world", "moooo", "cooow"]) {
      const keyBuf = new TextEncoder().encode(value).buffer;
      const key = new ReferencedValue({ offset: 0n, length: 0 }, keyBuf);

      const [rv, mp] = await bptree.find(key);

      expect(value).toEqual(new TextDecoder().decode(rv.value));
      expect(mp.offset).toEqual(BigInt(idx));
      idx += 1;
    }
  });
});
