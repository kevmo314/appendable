import { BTree, MetaPage } from "../btree/btree";
import { MemoryPointer } from "../btree/node";
import { RangeResolver } from "../resolver/resolver";
import { readBinaryFile } from "./test-util";
import { FileFormat } from "../file/meta";
import { FieldType } from "../db/database";
import { BTreeCursor } from "../btree/cursor";

class testMetaPage implements MetaPage {
  private readonly rootMP: MemoryPointer;

  constructor(mp: MemoryPointer) {
    this.rootMP = mp;
  }

  async root(): Promise<MemoryPointer> {
    return this.rootMP;
  }
}

describe("cursor", () => {
  let mockRangeResolver: RangeResolver;
  let mockDataFileResolver: RangeResolver;
  let btree: BTree;
  let cursor: BTreeCursor;

  beforeEach(() => {
    mockDataFileResolver = async ([]) => {
      return [
        {
          data: new ArrayBuffer(0),
          totalLength: 0,
        },
      ];
    };

    mockRangeResolver = async ([{ start, end }]) => {
      const indexFile = await readBinaryFile("btree_1023.bin");
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
    btree = new BTree(
      mockRangeResolver,
      page,
      mockDataFileResolver,
      FileFormat.CSV,
      FieldType.String,
      9,
    );

    cursor = btree.cursor();
  });

  it("fetches the # of elements", async () => {
    expect(await cursor.uniqueEntries()).toEqual(10);
  });
});
