import {
	BPTreeNode,
	MemoryPointer,
	ReferencedValue,
	compareBytes,
} from "../node";
import { RangeResolver } from "../resolver";

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
			const result = compareBytes(strToUint8Array(a), strToUint8Array(b));
			expect(result).toBe(i);
		});
	});
});

describe("test binary search", () => {
	let bpTreeNode: BPTreeNode;
	let keys: ReferencedValue[];
	let pointers: MemoryPointer[];

	beforeEach(() => {
		keys = [
			{
				dataPointer: { offset: 3, length: 1 },
				value: Buffer.from(strToUint8Array("omoplata")),
			},
			{
				dataPointer: { offset: 4, length: 1 },
				value: Buffer.from(strToUint8Array("tarikoplata")),
			},
			{
				dataPointer: { offset: 1, length: 1 },
				value: Buffer.from(strToUint8Array("baratoplata")),
			},
			{
				dataPointer: { offset: 2, length: 1 },
				value: Buffer.from(strToUint8Array("choibar")),
			},
			{
				dataPointer: { offset: 0, length: 1 },
				value: Buffer.from(strToUint8Array("armbar")),
			},
		];

		keys.sort((a, b) => compareBytes(a.value, b.value));

		pointers = new Array(keys.length + 1).fill({ offset: 0, length: 0 });
		bpTreeNode = new BPTreeNode(pointers, keys);
	});

	it("should find the correct position for existing keys", async () => {
		const testKeys = [
			"omoplata",
			"tarikoplata",
			"baratoplata",
			"choibar",
			"armbar",
		];

		for (const key of testKeys) {
			const keyArray = strToUint8Array(key);
			const position = await bpTreeNode.bsearch(keyArray);
			expect(position).toBeGreaterThanOrEqual(0);
			const keyValueBuffer = Buffer.from(keyArray);
			expect(bpTreeNode.keys[position].value).toEqual(keyValueBuffer);
		}
	});

	it("should find the no position for nonexistent keys", async () => {
		const testKeys = ["singlelegx", "xguard", "zguard"];

		for (const key of testKeys) {
			const keyArray = strToUint8Array(key);
			const position = await bpTreeNode.bsearch(keyArray);
			expect(position).toBeLessThan(0);
		}
	});
});

describe("BPTree readNode method", () => {
	let mockRangeResolver: RangeResolver;
	let mockMemoryPointer: MemoryPointer;

	function createMockNodeData(isLeaf: boolean, keys: string[]): Uint8Array {
		const size = isLeaf ? -keys.length : keys.length;
		const sizeBuffer = Buffer.alloc(4);
		sizeBuffer.writeInt32BE(size);

		let totalSize = 4;
		for (const key of keys) {
			totalSize += 4;
			totalSize += key.length;
		}
		const pointerSize = 8;

		if (isLeaf) {
			totalSize += keys.length * pointerSize;
		} else {
			totalSize += (keys.length + 1) * pointerSize; // since n + 1 pointers
		}

		const mockData = Buffer.alloc(totalSize);
		sizeBuffer.copy(mockData, 0);

		let offset = 4;
		for (const key of keys) {
			const keyBuffer = strToUint8Array(key);

			mockData.writeUInt32BE(keyBuffer.length, offset);
			offset += 4;

			keyBuffer.forEach((byte) => {
				mockData[offset] = byte;
				offset++;
			});
		}

		return mockData;
	}

	const mockLeafNodeData = createMockNodeData(true, ["cat", "dog", "wef"]);
	const mockNonLeafNodeData = createMockNodeData(false, [
		"key1",
		"key2",
		"key3",
	]);

	const emptyLeafNodeData = createMockNodeData(true, []);

	it("create leaf BPTreeNode from memory", async () => {
		mockRangeResolver = jest.fn().mockImplementation(async ({ start, end }) => {
			return {
				data: mockLeafNodeData.slice(start, end),
				totalLength: end - start,
			};
		});

		mockMemoryPointer = { offset: 0, length: mockLeafNodeData.length };

		const { node, bytesRead } = await BPTreeNode.fromMemoryPointer(
			mockMemoryPointer,
			mockRangeResolver
		);

		expect(node).not.toBeNull();
		expect(node?.keys).toHaveLength(3);
		expect(node?.pointers).toHaveLength(3);
		expect(bytesRead).toBe(mockMemoryPointer.length);
	});

	it("create non-leaf BPTreeNode from memory", async () => {
		const nonLeafMemoryPointer = {
			offset: 0,
			length: mockNonLeafNodeData.length,
		};

		mockRangeResolver = jest.fn().mockImplementation(async ({ start, end }) => {
			return {
				data: mockNonLeafNodeData.slice(start, end),
				totalLength: end - start,
			};
		});

		const { node, bytesRead } = await BPTreeNode.fromMemoryPointer(
			nonLeafMemoryPointer,
			mockRangeResolver
		);

		expect(bytesRead).toBe(nonLeafMemoryPointer.length);
		expect(node).not.toBeNull();
		expect(node?.keys).toHaveLength(3);
		expect(node?.pointers).toHaveLength(4);
	});

	it("create empty leaf BPTreeNode from memory", async () => {
		const leafMemoryPointer = {
			offset: 0,
			length: emptyLeafNodeData.length,
		};

		mockRangeResolver = jest.fn().mockImplementation(async ({ start, end }) => {
			return {
				data: emptyLeafNodeData.slice(start, end),
				totalLength: end - start,
			};
		});

		const { node } = await BPTreeNode.fromMemoryPointer(
			leafMemoryPointer,
			mockRangeResolver
		);

		expect(node).toBeNull();
	});

});
