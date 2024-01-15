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

	const mockRangeResolver = async () => {
		return { data: new Uint8Array(), totalLength: 0 };
	};

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
		bpTreeNode = new BPTreeNode(mockRangeResolver, pointers, keys);
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

describe("test readFrom", () => {
	let bpTreeNode: BPTreeNode;
	let mockDataHandler: RangeResolver;

	function mockData(start: number, end: number) {
		const keys = ["key1", "key2", "key3"];

		// for every key, (4 bytes for length, 4 bytes for data)
		let buffer = Buffer.alloc(4 + keys.length * 8);

		buffer.writeInt32BE(-keys.length, 0);

		let offset = 4;
		keys.forEach((key, index) => {
			buffer.writeInt32BE(4, offset);
			offset += 4;

			// we're assuming each key is exactly 4 bytes
			// this might not be true for actual data
			buffer.write(key, offset, 4, "utf-8");
			offset += 4;
		});

		return buffer.slice(start, end);
	}

	beforeEach(() => {
		mockDataHandler = jest.fn().mockImplementation(async ({ start, end }) => {
			return { data: mockData(start, end), totalLength: end - start };
		});

		bpTreeNode = new BPTreeNode(mockDataHandler, [], []);
	});

	it("correctly reads and parses data for leaf node", async () => {
		const totalBytesRead = await bpTreeNode.readFrom();

		expect(mockDataHandler).toHaveBeenCalled();

		const expectedKeys = ["key1", "key2", "key3"].map((key) => ({
			dataPointer: { offset: 0, length: 4 },
			value: Buffer.from(key, "utf-8"),
		}));

		expect(bpTreeNode.keys).toEqual(expectedKeys);
		expect(bpTreeNode.pointers.length).toEqual(expectedKeys.length);

		//expect(totalBytesRead).toBe(4 + expectedKeys.length * 8);
	});
});
