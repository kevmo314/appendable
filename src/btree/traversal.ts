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
		console.log("path: ", path.length);
		this.records = path;
		return true;
	}

	getKey(): ReferencedValue {
		return this.records[0].node.keys[this.records[0].index];
	}

	getPointer(): MemoryPointer {
		return this.records[0].node.pointer(this.records[0].index);
	}

	async increment(delta: number): Promise<boolean> {
		if (this.records.length === 1) {
			return false;
		}

		for (let idx = 0; idx <= this.records.length - 1; idx++) {
			this.records[idx].index += delta;
			if (
				this.records[idx].index < 0 ||
				this.records[idx].index > this.records[idx].node.keys.length
			) {
				if (idx === this.records.length - 1) {
					// we're at the end of the tree
					return false;
				}
			} else {
				for (let jdx = idx - 1; jdx >= 0; jdx--) {
					try {
						const node = await this.tree.readNode(
							this.records[jdx + 1].node.pointer(this.records[jdx + 1].index)
						);

						this.records[jdx].node = node;

						if (jdx === 0) {
							this.records[jdx].index =
								(this.records[jdx].index + this.records[jdx].node.keys.length) %
								this.records[jdx].node.keys.length;
						} else {
							this.records[jdx].index =
								(this.records[jdx].index +
									this.records[jdx].node.keys.length +
									1) %
								(this.records[jdx].node.keys.length + 1);
						}
						break;
					} catch {
						return false;
					}
				}
			}
		}

		return (
			this.records[0].index >= 0 &&
			this.records[0].index < this.records[0].node.keys.length
		);
	}

	async next(): Promise<boolean> {
		if (this.records.length === 0) {
			return await this.init();
		}

		return this.increment(1);
	}
}
