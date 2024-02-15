import { BPTree, MetaPage } from "../btree/bptree";
import { LinkedMetaPage } from "../btree/multi";
import { MemoryPointer } from "../btree/node";
import { RangeResolver } from "../resolver";

function textEncode(phr: string): Uint8Array {
	const encoder = new TextEncoder();

	return encoder.encode(phr);
}

class TestMetaPage implements MetaPage {
	private _root: MemoryPointer;

	constructor(initialRoot: MemoryPointer) {
		this._root = initialRoot;
	}

	setRoot(mp: MemoryPointer): void {
		this._root = mp;
	}

	root(): MemoryPointer {
		return this._root;
	}
}

describe("test compare bytes", () => {
	let buffer: Uint8Array;
	let resolver: RangeResolver;

	beforeEach(() => {
		buffer = new Uint8Array(4096);
	});

	it("generates an empty tree", async() => {
		let mp: MemoryPointer = {
			offset: BigInt(0),
			length: 0,
		};

		const tree = new BPTree(resolver, new TestMetaPage(mp), buffer);

		const res = await tree.find(textEncode("howdy"));

		expect(res).toEqual([{ offset: BigInt(0), length: 0 }, false]);
	});
});
