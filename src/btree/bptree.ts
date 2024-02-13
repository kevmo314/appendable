import { BPTreeNode, MemoryPointer, compareBytes } from "./node";
import { LengthIntegrityError, RangeResolver } from "../resolver";
import { LinkedMetaPage } from "./multi";

type RootResponse = {
	rootNode: BPTreeNode;
	pointer: MemoryPointer;
};

export class BPTree {
	private tree: RangeResolver;
	private meta: LinkedMetaPage;

	constructor(tree: RangeResolver, meta: LinkedMetaPage) {
		this.tree = tree;
		this.meta = meta;
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

	private async readNode(ptr: MemoryPointer): Promise<BPTreeNode | null> {
		try {
			const { node, bytesRead } = await BPTreeNode.fromMemoryPointer(
				ptr,
				this.tree
			);

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
		node: BPTreeNode,
		pointer: MemoryPointer
	): Promise<TraversalRecord[] | null> {
		if (node.leaf()) {
			return [{ node: node, index: 0, pointer: pointer }];
		}

		for (const [i, k] of node.keys.entries()) {
			if (compareBytes(key, k.value) < 0) {
				const child = await this.readNode(node.pointers[i]);
				if (!child) {
					return null;
				}

				const path = await this.traverse(key, child, node.pointers[i]);
				if (!path) {
					return null;
				}

				return [...path, { node: node, index: i, pointer: pointer }];
			}
		}

		const child = await this.readNode(node.pointers[node.pointers.length - 1]);

		if (!child) {
			return null;
		}

		const path = await this.traverse(
			key,
			child,
			node.pointers[node.pointers.length - 1]
		);
		if (!path) {
			return null;
		}

		return [...path, { node: node, index: node.keys.length, pointer: pointer }];
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

		const n = path[0].node;

		const i = await n.bsearch(key);

		if (i >= 0) {
			return [n.pointers[i], true];
		}

		return [{ offset: BigInt(0), length: 0 }, false];
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
