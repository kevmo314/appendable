import {
	BPTree,
	MetaPage,
	ReferencedValue,
	binarySearchReferencedValues,
} from "../btree/bptree";
import { MemoryPointer } from "../btree/node";
import { IndexFileV1 } from "../index-file/index-file";
import { RangeResolver } from "../resolver";
import { readBinaryFile } from "./test-util";

class testMetaPage implements MetaPage {
	private rootMP: MemoryPointer;

	constructor(mp: MemoryPointer) {
		this.rootMP = mp;
	}

	async root(): Promise<MemoryPointer | null> {
		return this.rootMP;
	}
}

describe("test compare bytes", () => {
	let mockRangeResolver: RangeResolver;
	let indexFileSize: number;
	let mockDataFileResolver: RangeResolver;

	beforeEach(() => {
		mockRangeResolver = async ({ start, end }) => {
			const indexFile = await readBinaryFile("bptree_1.bin");

			const slicedPart = indexFile.slice(start, end + 1);

			const arrayBuffer = slicedPart.buffer.slice(
				slicedPart.byteOffset,
				slicedPart.byteOffset + slicedPart.byteLength
			);

			return {
				data: arrayBuffer,
				totalLength: arrayBuffer.byteLength,
			};
		};

		mockDataFileResolver = async ({ start, end }) => {
			return {
				data: new ArrayBuffer(0),
				totalLength: 0,
			};
		};
	});

	it("should read a bptree", async () => {
		const page = new testMetaPage({ offset: 0n, length: 1 });
		const bptree = new BPTree(mockRangeResolver, page, mockDataFileResolver);

		const textEncoder = new TextEncoder();
		const helloBuffer = textEncoder.encode("hello");
		const key = new ReferencedValue(
			{ offset: 0n, length: 1 },
			helloBuffer.buffer
		);

		// const [rv, mp] = await bptree.find(key);

		// console.log(rv, mp);
	});

	it("should compareBytes correctly", () => {
		const buffer1 = new Uint8Array([0]).buffer;
		const buffer2 = new Uint8Array([0]).buffer;

		const result = ReferencedValue.compareBytes(buffer1, buffer2);
		console.log(result); 

    expect(result).toEqual(0)
  });

	it("should compare reference values", () => {
		const values: ReferencedValue[] = [
			new ReferencedValue(
				{ offset: 0n, length: 10 },
				new Uint8Array([0]).buffer
			),
			new ReferencedValue(
				{ offset: 10n, length: 20 },
				new Uint8Array([1]).buffer
			),
			new ReferencedValue(
				{ offset: 20n, length: 30 },
				new Uint8Array([2]).buffer
			),
		];

		const key0: ReferencedValue = new ReferencedValue(
			{ offset: 0n, length: 10 },
			new Uint8Array([0]).buffer
		);

		const [index0, found0] = binarySearchReferencedValues(values, key0);
		expect(index0).toEqual(0);
		expect(found0).toBeTruthy();

		const key1: ReferencedValue = new ReferencedValue(
			{ offset: 0n, length: 0 },
			new Uint8Array([1]).buffer
		);

		const [index1, found1] = binarySearchReferencedValues(values, key1);
		expect(index1).toEqual(1);
		expect(found1).toBeFalsy();

		const keyNeg1: ReferencedValue = new ReferencedValue(
			{ offset: 0n, length: 0 },
			new Uint8Array([5]).buffer
		);

		const [indexNeg1, foudnNeg1] = binarySearchReferencedValues(
			values,
			keyNeg1
		);

		expect(indexNeg1).toEqual(3);
		expect(foudnNeg1).toBeFalsy();
	});
});
