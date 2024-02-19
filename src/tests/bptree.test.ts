import { BPTree, MetaPage, ReferencedValue } from "../btree/bptree";
import { MemoryPointer } from "../btree/node";
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

	beforeEach(() => {
		mockDataFileResolver = async ({ start, end }) => {
			return {
				data: new ArrayBuffer(0),
				totalLength: 0,
			};
		};
	});
	it("", () => {});

	/*

	it("should read a bptree and find items", async () => {
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

		const page = new testMetaPage({ offset: 8192n, length: 88 });
		const bptree = new BPTree(mockRangeResolver, page, mockDataFileResolver);

		let idx = 1;
		for (const value of ["hello", "world", "moooo", "cooow"]) {
			const textEncoder = new TextEncoder();
			const keyBuf = textEncoder.encode(value);
			const key = new ReferencedValue({ offset: 0n, length: 0 }, keyBuf.buffer);

			const [rv, mp] = await bptree.find(key);

			expect(value).toEqual(new TextDecoder().decode(rv.value));
			expect(mp.offset).toEqual(BigInt(idx));
			idx += 1;
			console.log(rv, mp);
		}
	});

	it("should find items that were sequentially inserted", async () => {
		mockRangeResolver = async ({ start, end }) => {
			const indexFile = await readBinaryFile("bptree_sequential.bin");
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

		const page = new testMetaPage({ offset: 8192n, length: 256 });
		const bptree = new BPTree(mockRangeResolver, page, mockDataFileResolver);

		for (let idx = 0; idx < 256; idx++) {
			const valueBuf = new ArrayBuffer(8);
			const view = new DataView(valueBuf);
			view.setBigUint64(0, BigInt(idx));

			const key = new ReferencedValue({ offset: 0n, length: 0 }, valueBuf);

			const [rv, mp] = await bptree.find(key);

			if (rv && rv.value) {
				const rvValue = new DataView(rv.value).getBigUint64(0);
				console.log("r: ", rvValue);
			} else {
				console.log("value undefined: ", idx);
				break;
			}
		}
	});
	*/
});

describe("test single page bptree iteration", () => {
	let mockRangeResolver: RangeResolver;
	let mockDataFileResolver: RangeResolver;

	beforeEach(() => {
		mockRangeResolver = async ({ start, end }) => {
			const indexFile = await readBinaryFile("bptree_iterator_single.bin");
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

	it("should iterate forward", async () => {
		const indexFile = await readBinaryFile("bptree_iterator.bin");
		console.log(indexFile.byteLength);

		const page = new testMetaPage({ offset: 8192n, length: 32 });
		const tree = new BPTree(mockRangeResolver, page, mockDataFileResolver);

		const valueBuf = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8]);

		const key = new ReferencedValue({ offset: 0n, length: 0 }, valueBuf.buffer);

		const iter = tree.iter(key);

		for (let idx = 0; await iter.next(); idx++) {
			if (idx > 64) {
				console.log("expected to find %d keys");
			}
			console.log("idx: ", idx)

		}
	});
});
