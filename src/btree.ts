import { BPTreeNode, MemoryPointer, compareBytes } from "./node";
import { LengthIntegrityError, RangeResolver } from "./resolver";

// taken from `buffer.go`
interface MetaPage {
	root(): Promise<MemoryPointer>;
}

class BPTree {
	private tree: RangeResolver;
	private meta: MetaPage;
	private maxPageSize: number;

	constructor(tree: RangeResolver, meta: MetaPage, maxPageSize: number) {
		this.tree = tree;
		this.meta = meta;
		this.maxPageSize = maxPageSize;
	}

	private async root(): Promise<[BPTreeNode | null, MemoryPointer]> {
		const mp = await this.meta.root();
		if (!mp || mp.length === 0) {
			return [null, mp];
		}

		const root = await this.readNode(mp);
		if (!root) {
			return [null, mp];
		}

		return [root, mp];
	}

	private async readNode(ptr: MemoryPointer): Promise<BPTreeNode | null> {
		try {
			const { node, bytesRead } = await BPTreeNode.fromMemoryPointer(ptr, this.tree);

			if (!bytesRead || bytesRead !== ptr.length) {
				return null;
			}

			return node;
		} catch (error) {
			if (error instanceof LengthIntegrityError) {
				// handle LengthIntegrityError
			}

			return null;
		}
	}

	private async traverse(
		key: Uint8Array,
		node: BPTreeNode
	): Promise<TraversalRecord[] | null> {
		if (await node.leaf()) {
			return [{ node: node, index: 0 }];
		}

		for (const [i, k] of node.keys.entries()) {
			if (compareBytes(key, k.value) < 0) {
				const child = await this.readNode(node.pointers[i]);
				if (!child) {
					return null;
				}

				const path = await this.traverse(key, child);
				if (!path) {
					return null;
				}

				return [...path, { node: node, index: i }];
			}
		}

		const child = await this.readNode(node.pointers[node.pointers.length - 1]);

		if (!child) {
			return null;
		}

		const path = await this.traverse(key, child);
		if (!path) {
			return null;
		}

		return [...path, { node: node, index: node.keys.length }];
	}

	public async find(key: Uint8Array): Promise<[MemoryPointer, boolean]> {
		let [rootNode, _] = await this.root();

		if (!rootNode) {
			return [{ offset: 0, length: 0 }, false];
		}

		const path = await this.traverse(key, rootNode);
		if (!path) {
			return [{ offset: 0, length: 0 }, false];
		}

		const n = path[0].node;

		const i = await n.bsearch(key);

		if (i >= 0) {
			return [n.pointers[i], true];
		}

		return [{ offset: 0, length: 0 }, false];
	}
}

class TraversalRecord {
	public node: BPTreeNode;
	public index: number;

	constructor(node: BPTreeNode, index: number) {
		this.node = node;
		this.index = index;
	}
}
