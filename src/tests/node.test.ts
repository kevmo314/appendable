import { ReferencedValue, binarySearchReferencedValues } from "../btree/bptree";
import { maxUint16 } from "../btree/multi";
import { BPTreeNode } from "../btree/node";
import { FieldType } from "../db/database";
import { FileFormat } from "../index-file/meta";
import { RangeResolver } from "../resolver";
import { readBinaryFile } from "./test-util";

const PAGE_SIZE_BYTES = 4096;

const strToArrayBuffer = (str: string) => {
  return new Uint8Array(str.split("").map((c) => c.charCodeAt(0))).buffer;
};

describe("test compare bytes", () => {
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

  it(`test compareBytes`, async () => {
    // This test uses the Go test cases for `bytes.Compare` for `compareBytes()`
    // https://cs.opensource.google/go/go/+/refs/tags/go1.21.6:src/bytes/compare_test.go
    testCases.forEach(({ a, b, i }) => {
      const result = ReferencedValue.compareBytes(
        strToArrayBuffer(a),
        strToArrayBuffer(b),
      );
      expect(result).toBe(i);
    });
  });
});

describe("compareReferencedValues", () => {
  it("should compare reference values", () => {
    const values: ReferencedValue[] = [
      new ReferencedValue(
        { offset: 0n, length: 10 },
        new Uint8Array([0]).buffer,
      ),
      new ReferencedValue(
        { offset: 10n, length: 20 },
        new Uint8Array([1]).buffer,
      ),
      new ReferencedValue(
        { offset: 20n, length: 30 },
        new Uint8Array([2]).buffer,
      ),
    ];

    const key0: ReferencedValue = new ReferencedValue(
      { offset: 0n, length: 10 },
      new Uint8Array([0]).buffer,
    );

    const [index0, found0] = binarySearchReferencedValues(values, key0);
    expect(index0).toEqual(0);
    expect(found0).toBeTruthy();

    const key1: ReferencedValue = new ReferencedValue(
      { offset: 0n, length: 0 },
      new Uint8Array([1]).buffer,
    );

    const [index1, found1] = binarySearchReferencedValues(values, key1);
    expect(index1).toEqual(1);
    expect(found1).toBeFalsy();

    const keyNeg1: ReferencedValue = new ReferencedValue(
      { offset: 0n, length: 0 },
      new Uint8Array([5]).buffer,
    );

    const [indexNeg1, foundNeg1] = binarySearchReferencedValues(
      values,
      keyNeg1,
    );

    expect(indexNeg1).toEqual(3);
    expect(foundNeg1).toBeFalsy();
  });
});

describe("node functionality", () => {
  let mockLeafNodeData: Uint8Array;
  let mockInternalNodeData: Uint8Array;

  let mockRangeResolver: RangeResolver;
  let mockDataResolver: RangeResolver;

  beforeAll(async () => {
    mockLeafNodeData = await readBinaryFile("leafnode.bin");
    mockInternalNodeData = await readBinaryFile("internalnode.bin");

    mockDataResolver = async ([{ start, end }]) => {
      return [
        {
          data: new ArrayBuffer(0),
          totalLength: 0,
        },
      ];
    };
  });

  it("should read a leaf bptree node", async () => {
    mockRangeResolver = async ([{ start, end }]) => {
      const view = new Uint8Array(new ArrayBuffer(PAGE_SIZE_BYTES));
      view.set(mockLeafNodeData, 0);
      const slice = view.slice(start, end + 1);

      return [
        {
          data: slice.buffer,
          totalLength: view.byteLength,
        },
      ];
    };

    const { node: leafNode } = await BPTreeNode.fromMemoryPointer(
      { offset: 0n, length: 1 },
      mockRangeResolver,
      mockDataResolver,
      FileFormat.CSV,
      FieldType.String,
      maxUint16,
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
    mockRangeResolver = async ([{ start, end }]) => {
      const view = new Uint8Array(new ArrayBuffer(PAGE_SIZE_BYTES));
      view.set(mockInternalNodeData, 0);
      const slice = view.slice(start, end + 1);

      return [
        {
          data: slice.buffer,
          totalLength: view.byteLength,
        },
      ];
    };

    const { node: internalNode } = await BPTreeNode.fromMemoryPointer(
      { offset: 0n, length: 1 },
      mockRangeResolver,
      mockDataResolver,
      FileFormat.CSV,
      FieldType.String,
      maxUint16,
    );

    expect(internalNode.internalPointers.length).toEqual(4);
    expect(internalNode.leafPointers.length).toEqual(0);
    expect(internalNode.keys.length).toEqual(3);

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
