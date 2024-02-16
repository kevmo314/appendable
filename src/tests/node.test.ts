import { ReferencedValue } from "../btree/bptree";
import { BPTreeNode, MemoryPointer } from "../btree/node";
import { RangeResolver } from "../resolver";
import { readBinaryFile } from "./test-util";

const PAGE_SIZE_BYTES = 4096;

const strToArrayBuffer = (str: string) => {
  return new Uint8Array(str.split("").map((c) => c.charCodeAt(0))).buffer;
};

describe("test compare bytes", () => {
  beforeEach(() => {});

  const testCases = [
    { a: "", b: "", i: 0 },
    { a: "a", b: "", i: 1 },
    { a: "", b: "a", i: -1 },
    { a: "abc", b: "abc", i: 0 },
    { a: "abd", b: "abc", i: 1 },
    { a: "abc", b: "abd", i: -1 },
    { a: "ab", b: "abc", i: -1 },
    { a: "abc", b: "ab", i: 1 },
    { a: "x", b: "ab", i: 1 },
    { a: "ab", b: "x", i: -1 },
    { a: "x", b: "a", i: 1 },
    { a: "b", b: "x", i: -1 },
    { a: "abcdefgh", b: "abcdefgh", i: 0 },
    { a: "abcdefghi", b: "abcdefghi", i: 0 },
    { a: "abcdefghi", b: "abcdefghj", i: -1 },
    { a: "abcdefghj", b: "abcdefghi", i: 1 },
  ];

  // This test uses the Go test cases for `bytes.Compare` for `compareBytes()`
  // https://cs.opensource.google/go/go/+/refs/tags/go1.21.6:src/bytes/compare_test.go
  testCases.forEach(({ a, b, i }, idx) => {
    it(`test ${idx} compareBytes`, async () => {
      const result = ReferencedValue.compareBytes(
        strToArrayBuffer(a),
        strToArrayBuffer(b),
      );
      expect(result).toBe(i);
    });
  });
});

describe("node functionality", () => {
  it("should read a leaf bptree node", async () => {
    const mockRangeResolver: RangeResolver = async ({ start, end }) => {
      const view = new Uint8Array(new ArrayBuffer(PAGE_SIZE_BYTES));
      const leafnode_data = await readBinaryFile("leafnode.bin");
      view.set(leafnode_data, 0);
      const slice = view.slice(start, end + 1);

      return {
        data: slice.buffer,
        totalLength: view.byteLength,
      };
    };

    // since we're storing the values directly, we can mock
    let dataResolver: RangeResolver = async ({ start, end }) => {
      const mock = new ArrayBuffer(0);
      return {
        data: mock,
        totalLength: 0,
      };
    };

    const { node: leafNode, bytesRead } = await BPTreeNode.fromMemoryPointer(
      { offset: 0n, length: 1 },
      mockRangeResolver,
      dataResolver,
    );

    expect(leafNode.internalPointers.length).toEqual(0);
    expect(leafNode.leafPointers.length).toEqual(3);
    expect(leafNode.keys.length).toEqual(3);

    for (let idx = 0; idx <= leafNode.keys.length - 1; idx++) {
      const rv = leafNode.keys[idx];

      const buffer = new ArrayBuffer(idx + 1);
      const data = new Uint8Array(buffer);

      if (idx === 0) {
        data[0] = 0;
      } else if (idx === 1) {
        data[0] = 1;
        data[1] = 2;
      } else if (idx === 2) {
        data[0] = 3;
        data[1] = 4;
        data[2] = 5;
      }

      expect(rv.value).toEqual(data.buffer);
      expect(rv.value.byteLength).toEqual(idx + 1);

      // evaluating leaf pointers
      const lp = leafNode.leafPointers[idx];
      expect(lp.length).toEqual(idx + 1);
      if (idx === 0) {
        expect(lp.offset).toEqual(0n);
      } else if (idx === 1) {
        expect(lp.offset).toEqual(1n);
      } else if (idx === 2) {
        expect(lp.offset).toEqual(2n);
      }
    }
  });

  it("should read a internal bptree node", async () => {
    const mockRangeResolver: RangeResolver = async ({ start, end }) => {
      const view = new Uint8Array(new ArrayBuffer(PAGE_SIZE_BYTES));
      const internalnode_data = await readBinaryFile("internalnode.bin");
      view.set(internalnode_data, 0);
      const slice = view.slice(start, end + 1);

      return {
        data: slice.buffer,
        totalLength: view.byteLength,
      };
    };

    // since we're storing the values directly, we can mock
    let dataResolver: RangeResolver = async ({ start, end }) => {
      const mock = new ArrayBuffer(0);
      return {
        data: mock,
        totalLength: 0,
      };
    };

    const { node: internalNode, bytesRead } =
      await BPTreeNode.fromMemoryPointer(
        { offset: 0n, length: 1 },
        mockRangeResolver,
        dataResolver,
      );

    expect(internalNode.internalPointers.length).toEqual(4);
    expect(internalNode.leafPointers.length).toEqual(0);
    expect(internalNode.keys.length).toEqual(3);

    console.log(internalNode.internalPointers, internalNode.keys);

    for (let idx = 0; idx <= internalNode.internalPointers.length; idx++) {
      const ip = internalNode.internalPointers[idx];
      if (idx === 0) {
        expect(ip).toEqual(0n);
      } else if (idx === 1) {
        expect(ip).toEqual(1n);
      } else if (idx === 2) {
        expect(ip).toEqual(2n);
      } else if (idx === 3) {
        expect(ip).toEqual(3n);
      }
    }

    for (let idx = 0; idx <= internalNode.keys.length - 1; idx++) {
      const rv = internalNode.keys[idx];

      const buffer = new ArrayBuffer(idx + 1);
      const data = new Uint8Array(buffer);

      if (idx === 0) {
        data[0] = 0;
      } else if (idx === 1) {
        data[0] = 1;
        data[1] = 2;
      } else if (idx === 2) {
        data[0] = 3;
        data[1] = 4;
        data[2] = 5;
      }

      expect(rv.value).toEqual(data.buffer);
      expect(rv.value.byteLength).toEqual(idx + 1);
    }
  });
});
