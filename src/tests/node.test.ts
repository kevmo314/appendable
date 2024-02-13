import { ReferencedValue } from "../btree/bptree";
import { BPTreeNode, MemoryPointer } from "../btree/node";

const strToUint8Array = (str: string) => {
	return new Uint8Array(str.split("").map((c) => c.charCodeAt(0)));
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
				strToUint8Array(a),
				strToUint8Array(b)
			);
			expect(result).toBe(i);
		});
	});
});

describe("node functionality", () => {
	it("correctly identifies leaf nodes", async () => {
		const leafKeys = [
			new ReferencedValue({ offset: BigInt(0), length: 0 }, new Uint8Array()),
		];
		const leafPointer = { offset: BigInt(0), length: 0 };
		const leafNode = new BPTreeNode(leafKeys, [leafPointer], [], new ArrayBuffer(4096));
		expect(leafNode.leaf()).toBeTruthy();

		const internalNode = new BPTreeNode(
			[],
			[],
			[BigInt(0)],
			new ArrayBuffer(4096)
		);
		expect(internalNode.leaf()).toBeFalsy();
	});

	it("retrieves the correct pointer for a leaf node", async () => {
		const leafPointers: MemoryPointer[] = [{ offset: BigInt(10), length: 20 }];
		const leafNode = new BPTreeNode(
			[],
			leafPointers,
			[],
			new ArrayBuffer(4096)
		);
		expect(leafNode.pointer(0)).toEqual(leafPointers[0]);
	});
});
