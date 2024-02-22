import { BPTree, MetaPage, ReferencedValue } from "../btree/bptree";
import { maxUint64 } from "../btree/multi";
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
		mockDataFileResolver = async ({ start, end }) => {
			return {
				data: new ArrayBuffer(0),
				totalLength: 0,
			};
		};

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
		bptree = new BPTree(
			mockRangeResolver,
			page,
			mockDataFileResolver,
			FileFormat.JSONL,
			FieldType.String
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

describe("test single page bptree iteration", () => {
	let mockRangeResolver: RangeResolver;
	let mockDataFileResolver: RangeResolver;

	const valueBuf = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8]);
	const expectedBuf = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8]).buffer;
	let tree: BPTree;

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
				data: new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8]).buffer,
				totalLength: 8,
			};
		};

		const page = new testMetaPage({ offset: 8192n, length: 32 });
		tree = new BPTree(mockRangeResolver, page, mockDataFileResolver, FileFormat.CSV, FieldType.String);
	});

	it("should iterate forward", async () => {
		const key = new ReferencedValue({ offset: 0n, length: 0 }, valueBuf.buffer);
		const iter = tree.iter(key);

		let idx = 0;
		while (await iter.next()) {
			if (idx > 63) {
				expect(idx).toBeLessThan(64);
			}

			const k = iter.getKey();
			expect(k.value).toEqual(expectedBuf);
			const v = iter.getPointer();
			expect(v.offset).toEqual(BigInt(idx));

			idx += 1;
		}
	});
	it("should iterate backwards", async () => {
		const key = new ReferencedValue(
			{ offset: maxUint64, length: 0 },
			valueBuf.buffer
		);

		const iter = tree.iter(key);
		let idx = 63;

		while (await iter.prev()) {
			if (idx < 0) {
				expect(idx).toBeGreaterThan(-1);
			}

			const k = iter.getKey();
			expect(k.value).toEqual(expectedBuf);
			const v = iter.getPointer();
			expect(v.offset).toEqual(BigInt(idx));

			idx -= 1;
		}
	});
});
