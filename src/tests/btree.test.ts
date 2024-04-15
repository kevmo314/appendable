import { BTree, MetaPage, ReferencedValue } from "../btree/btree";
import { MemoryPointer } from "../btree/node";
import { FieldType } from "../db/database";
import { FileFormat } from "../file/meta";
import { RangeResolver } from "../resolver/resolver";
import { readBinaryFile } from "./test-util";
import { maxUint64 } from "../file/multi";

class testMetaPage implements MetaPage {
  private readonly rootMP: MemoryPointer;

  constructor(mp: MemoryPointer) {
    this.rootMP = mp;
  }

  async root(): Promise<MemoryPointer> {
    return this.rootMP;
  }
}

describe("test BTree", () => {
  let mockRangeResolver: RangeResolver;
  let mockDataFileResolver: RangeResolver;
  let btree: BTree;

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
      const indexFile = await readBinaryFile("btree_1.bin");
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
      6,
      4,
    );
  });

  it("should read a btree and find items", async () => {
    let idx = 1;
    for (const value of ["hello", "world", "moooo", "cooow"]) {
      const keyBuf = new TextEncoder().encode(value).buffer;
      const key = new ReferencedValue({ offset: 0n, length: 0 }, keyBuf);

      const [rv, mp] = await btree.find(key);

      expect(value).toEqual(new TextDecoder().decode(rv.value));
      expect(mp.offset).toEqual(BigInt(idx));
      idx += 1;
    }
  });
});

describe("test BTree iterator count", () => {
  let mockRangeResolver: RangeResolver;
  let mockDataFileResolver: RangeResolver;
  let btree: BTree;

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
      10,
    );
  });

  it("should count the value 23 10 times", async () => {
    const valueBuf = new ArrayBuffer(8);
    new DataView(valueBuf).setFloat64(0, Number(23));

    const valueRef = new ReferencedValue({ offset: 0n, length: 0 }, valueBuf);

    const iter = btree.iter(valueRef);

    let count = 0;

    while (await iter.next()) {
      const currKey = iter.getKey();
      if (ReferencedValue.compareBytes(valueBuf, currKey.value) === 0) {
        count++;
      }
    }

    expect(count).toEqual(10);
  });

  it("should count the value 23 10 times reverse", async () => {
    const valueBuf = new ArrayBuffer(8);
    new DataView(valueBuf).setFloat64(0, Number(23));

    const valueRef = new ReferencedValue(
      { offset: maxUint64, length: 0 },
      valueBuf,
    );

    const iter = btree.iter(valueRef);
    let count = 0;

    while (await iter.prev()) {
      const currKey = iter.getKey();
      if (ReferencedValue.compareBytes(valueBuf, currKey.value) === 0) {
        count++;
      }
    }

    expect(count).toEqual(10);
  });
});
