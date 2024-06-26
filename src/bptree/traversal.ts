import { BPTree, ReferencedValue } from "./bptree";
import { BPTreeNode, MemoryPointer } from "./node";

export type TraversalRecord = {
  node: BPTreeNode;
  index: number;
  pointer: MemoryPointer;
};

export class TraversalIterator {
  private tree: BPTree;
  private readonly key: ReferencedValue;
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
    this.records = await this.tree.traverse(this.key, root, offset);

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
      if (!(await this.increment(i + 1, delta))) {
        return false;
      }

      if (!this.records[i + 1]) {
        return false;
      }
      // propagate the rollover
      this.records[i].node = await this.records[i + 1].node.child(
        this.records[i + 1].index,
      );

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
      const res = await this.init();

      return (
        res && this.records[0].index !== this.records[0].node.numPointers()
      );
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
