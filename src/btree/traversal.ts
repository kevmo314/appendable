import { BPTree, ReferencedValue } from "./bptree";
import { BPTreeNode, MemoryPointer } from "./node";

export type TraversalRecord = {
	node: BPTreeNode;
	index: number;
	pointer: MemoryPointer;
};

export class TraversalIterator {
	private tree: BPTree;
	private key: ReferencedValue;
	private records: TraversalRecord[];

	constructor(tree: BPTree, key: ReferencedValue) {
		this.tree = tree;
		this.key = key;
		this.records = []; // note this works iff all records are non-empty
	}

	async init(): Promise<boolean> {
		const rootResponse = await this.tree.root();

		if (rootResponse.rootNode === null) {
			return false;
		}

		const root = rootResponse.rootNode;
		const offset = rootResponse.pointer;
		const path = await this.tree.traverse(this.key, root, offset);
		this.records = path;
		return true;
	}

	getKey(): ReferencedValue {
		return this.records[0].node.keys[this.records[0].index];
	}

	getPointer(): MemoryPointer {
		return this.records[0].node.pointer(this.records[0].index);
	}

	async increment(i: number, delta: number): Promise<boolean> {
		if (i === this.records.length) {
			return false;
		}

		this.records[i].index += delta;
		const rolloverLeft = this.records[i].index < 0;
		const rolloverRight =
			this.records[i].index >= this.records[i].node.numPointers();

		if (rolloverLeft || rolloverRight) {
			if (!this.increment(i + 1, delta)) {
				return false;
			}

			if (!this.records[i + 1]) {
				return false;
			}
			const node = await this.tree.readNode(
				this.records[i + 1].node.pointer(this.records[i + 1].index)
			);

			// propagate the rollover
			this.records[i].node = node;

			if (rolloverLeft) {
				this.records[i].index = this.records[i].node.numPointers() - 1;
			} else {
				this.records[i].index = 0;
			}
		}

		return true;
	}

	async next(): Promise<boolean> {
		if (this.records.length === 0) {
			return await this.init();
		}

		return this.increment(0, 1);
	}

	async prev(): Promise<boolean> {
		if (this.records.length === 0) {
			const res = await this.init();
			if (!res) {
				return false;
			}
		}

		return this.increment(0, -1);
	}
}
