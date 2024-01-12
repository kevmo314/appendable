import { BPTreeNode, MemoryPointer, compareBytes } from "./node";

// taken from `buffer.go`
interface ReadWriteSeekTruncater {
	write(buffer: Uint8Array): Promise<number>;
	seek(offset: number, whence: "start" | "current" | "end"): Promise<number>;
	read(buffer: Uint8Array, offset: number): Promise<number>;
	truncate(size: number): Promise<void>;
}

interface MetaPage {
	root(): Promise<MemoryPointer>;
	setRoot(pointer: MemoryPointer): void;
}

class BPTree {
	private tree: ReadWriteSeekTruncater;
	private meta: MetaPage;
	private maxPageSize: number;

	constructor(
		tree: ReadWriteSeekTruncater,
		meta: MetaPage,
		maxPageSize: number
	) {
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
		const pos = await this.tree.seek(ptr.offset, "start");
		if (!pos || pos !== ptr.offset) {
			return null;
		}

		const buffer = Buffer.alloc(ptr.length);
		await this.tree.read(buffer, 0);

		const node = new BPTreeNode(this.tree, [], []);

		const bytesRead = await node.readFrom(buffer);

		if (!bytesRead || bytesRead !== ptr.length) {
			return null;
		}

		return node;
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
			return [new MemoryPointer(0, 0), false];
		}

		const path = await this.traverse(key, rootNode);
		if (!path) {
			return [new MemoryPointer(0, 0), false];
		}

		const n = path[0].node;

		const [i, found] = await n.bsearch(key);

		if (found) {
			return [n.pointers[i], true];
		}

		return [new MemoryPointer(0, 0), false];
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
