import { BPTree, MetaPage, ReferencedValue } from "../btree/bptree";
import { MemoryPointer } from "../btree/node";
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
		const page = new testMetaPage({ offset: 0n, length: 0 });
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
});
