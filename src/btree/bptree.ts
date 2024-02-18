import { BPTreeNode, MemoryPointer } from "./node";
import { RangeResolver } from "../resolver";
import { LinkedMetaPage } from "./multi";

export interface MetaPage {
	root(): Promise<MemoryPointer | null>;
}

type RootResponse = {
	rootNode: BPTreeNode;
	pointer: MemoryPointer;
};

export class BPTree {
	private readonly tree: RangeResolver;
	private meta: MetaPage;
	private readonly dataFileResolver: RangeResolver;

	constructor(
		tree: RangeResolver,
		meta: MetaPage,
		dataFileResolver: RangeResolver
	) {
		this.tree = tree;
		this.meta = meta;
		this.dataFileResolver = dataFileResolver;
	}

	private async root(): Promise<RootResponse | null> {
		const mp = await this.meta.root();

		if (!mp || mp.length === 0) {
			return null;
		}

		console.log(mp);

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
				this.dataFileResolver
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
		key: ReferencedValue,
		node: BPTreeNode,
		pointer: MemoryPointer
	): Promise<TraversalRecord[]> {
		let [index, found] = binarySearchReferencedValues(node.keys, key);

		if (node.leaf()) {
			return [{ node, index, pointer }];
		}

		if (found) {
			index += 1;
		}

		const childPointer = node.pointer(index);
		const child = await this.readNode(childPointer);
		const path = await this.traverse(key, child, childPointer);

		return [...path, { node, index, pointer }];
	}

	public async find(
		key: ReferencedValue
	): Promise<[ReferencedValue, MemoryPointer]> {
		const rootResponse = await this.root();

		if (!rootResponse) {
			throw new Error(`invalid root response: ${rootResponse}`);
		}

		let { rootNode, pointer } = rootResponse;

		const path = await this.traverse(key, rootNode, pointer);
		if (!path) {
			throw new Error(`nonexistent path: ${path}`);
		}

		return [
			path[0].node.keys[path[0].index],
			path[0].node.pointer(path[0].index),
		];
	}
}

class TraversalRecord {
	public node: BPTreeNode;
	public index: number;
	public pointer: MemoryPointer;

	constructor(node: BPTreeNode, index: number, pointer: MemoryPointer) {
		this.node = node;
		this.index = index;
		this.pointer = pointer;
	}
}

export class ReferencedValue {
	public dataPointer: MemoryPointer;
	public value: ArrayBuffer;

	constructor(dataPointer: MemoryPointer, value: ArrayBuffer) {
		this.dataPointer = dataPointer;
		this.value = value;
	}

	setDataPointer(mp: MemoryPointer) {
		this.dataPointer = mp;
	}

	setValue(value: ArrayBuffer) {
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
function compareReferencedValues(
	a: ReferencedValue,
	b: ReferencedValue
): number {
	const valueComparison = ReferencedValue.compareBytes(a.value, b.value);
	if (valueComparison !== 0) {
		return valueComparison;
	}

	if (a.dataPointer.offset > b.dataPointer.offset) {
		return 1;
	} else if (a.dataPointer.offset < b.dataPointer.offset) {
		return -1;
	}

	if (a.dataPointer.length > b.dataPointer.length) {
		return 1;
	} else if (a.dataPointer.length < b.dataPointer.length) {
		return -1;
	}

	return 0;
}

export function binarySearchReferencedValues(
	values: ReferencedValue[],
	target: ReferencedValue
): [number, boolean] {
	const n = values.length;

	let i = 0;
	let j = n;

	while (i < j) {
		const h = Math.floor((i + j) / 2);

		if (compareReferencedValues(values[h], target) < 0) {
			i = h + 1;
		} else {
			j = h;
		}
	}

	return [i, i < n && compareReferencedValues(values[i], target) === 0];
}
