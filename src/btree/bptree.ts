import { BPTreeNode, MemoryPointer } from "./node";
import { RangeResolver } from "../resolver";
import { LinkedMetaPage } from "./multi";


export interface MetaPage {
	root(): MemoryPointer;
	setRoot(mp: MemoryPointer): void;
}



type RootResponse = {
	rootNode: BPTreeNode;
	pointer: MemoryPointer;
};

export class BPTree {
	private readonly tree: RangeResolver;
	private meta: MetaPage;
	private readonly data: Uint8Array; // RangeResolver for the data-file

	constructor(tree: RangeResolver, meta: MetaPage, data: Uint8Array) {
		this.tree = tree;
		this.meta = meta;
		this.data = data;
	}

	private async root(): Promise<RootResponse | null> {
		const mp = await this.meta.root();
		if (!mp || mp.length === 0) {
			return null;
		}

		const root = await this.readNode(mp);
		if (!root) {
			return null;
		}

		return {
			rootNode: root,
			pointer: mp,
		};
	}

	private async readNode(ptr: MemoryPointer): Promise<BPTreeNode> {
		try {
			const { node, bytesRead } = await BPTreeNode.fromMemoryPointer(
				ptr,
				this.tree,
				this.data
			);

			if (!bytesRead || bytesRead !== ptr.length) {
				throw new Error("bytes read do not line up");
			}

			return node;
		} catch (error) {
			throw new Error(`${error}`);
		}
	}

	private async traverse(
		key: ArrayBuffer,
		node: BPTreeNode,
		pointer: MemoryPointer
	): Promise<TraversalRecord[]> {
		const index = binarySearchReferencedValues(node.keys, key);

		const found = index >= 0;
		const childIndex = found ? index : ~index;

		if (node.leaf()) {
			return [{ node, index: childIndex, found, pointer }];
		} else {
			const childPointer = node.pointer(childIndex);
			const child = await this.readNode(childPointer);
			const path = await this.traverse(key, child, childPointer);

			return [...path, { node, index: childIndex, found, pointer }];
		}
	}

	public async find(key: Uint8Array): Promise<[MemoryPointer, boolean]> {
		const rootResponse = await this.root();

		if (!rootResponse) {
			return [{ offset: BigInt(0), length: 0 }, false];
		}

		let { rootNode, pointer } = rootResponse;

		const path = await this.traverse(key, rootNode, pointer);
		if (!path) {
			return [{ offset: BigInt(0), length: 0 }, false];
		}

		return [path[0].node.pointer(path[0].index), path[0].found];
	}
}

class TraversalRecord {
	public node: BPTreeNode;
	public index: number;
	public found: boolean;
	public pointer: MemoryPointer;

	constructor(
		node: BPTreeNode,
		index: number,
		found: boolean,
		pointer: MemoryPointer
	) {
		this.node = node;
		this.index = index;
		this.found = found;
		this.pointer = pointer;
	}
}

export class ReferencedValue {
	public dataPointer: MemoryPointer;
	public value: ArrayBuffer;

	constructor(dataPointer: MemoryPointer, value: Uint8Array) {
		this.dataPointer = dataPointer;
		this.value = value;
	}

	static compareBytes(aBuffer: ArrayBuffer, bBuffer: ArrayBuffer): number {
		const a = new Uint8Array(aBuffer);
		const b = new Uint8Array(bBuffer);

		const len = Math.min(a.length, b.length);
		for (let idx = 0; idx < len; idx++) {
			if (a[idx] !== b[idx]) {
				return a[idx] < b[idx] ? -1 : 1;
			}
		}

		if (a.length < b.length) {
			return -1;
		}
		if (a.length > b.length) {
			return 1;
		}
		return 0;
	}
}

function binarySearchReferencedValues(
	values: ReferencedValue[],
	target: ArrayBuffer
): number {
	let lo = 0;
	let hi = values.length;

	while (lo < hi) {
		const mid = lo + ((hi - lo) >> 1);

		const cmp = ReferencedValue.compareBytes(values[mid].value, target);

		if (cmp === 0) {
			return mid;
		}

		if (cmp < 0) {
			lo = mid + 1;
		} else {
			hi = mid;
		}
	}

	return ~lo;
}
