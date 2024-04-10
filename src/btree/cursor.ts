import { BTree, ReferencedValue } from "./btree";
import { BTreeNode } from "./node";

export class BTreeCursor {
  private readonly btree: BTree;

  private uniqueEntriesPromise: Promise<number> | null = null;

  constructor(btree: BTree) {
    this.btree = btree;
  }

  async first(): Promise<ReferencedValue> {
    let { rootNode } = await this.btree.root();
    if (!rootNode) {
      throw new Error("unable to get root node");
    }

    let currNode = await this.btree.readNode(rootNode.pointer(0));

    while (!currNode.leaf()) {
      const childPointer = currNode.pointer(0);
      currNode = await this.btree.readNode(childPointer);
    }

    return currNode.keys[0];
  }

  async last(): Promise<ReferencedValue> {
    let { rootNode } = await this.btree.root();
    if (!rootNode) {
      throw new Error("unable to get root node");
    }

    let currNode = await this.btree.readNode(
      rootNode.pointer(rootNode.numPointers() - 1),
    );

    while (!currNode.leaf()) {
      const childPointer = currNode.pointer(currNode.numPointers() - 1);
      currNode = await this.btree.readNode(childPointer);
    }

    return currNode.keys[currNode.keys.length - 1];
  }

  // Counts the number of unique entries in the datafile
  uniqueEntries(): Promise<number> {
    if (!this.uniqueEntriesPromise) {
      this.uniqueEntriesPromise = this.computeUniqueLeafs();
    }

    return this.uniqueEntriesPromise;
  }

  private async computeUniqueLeafs(): Promise<number> {
    let { rootNode } = await this.btree.root();
    if (!rootNode) {
      return 0;
    }

    const uniqueOffsets = new Set<bigint>();
    await this.traverseLeafs(rootNode, uniqueOffsets);

    return uniqueOffsets.size;
  }

  private async traverseLeafs(node: BTreeNode, uniqueOffsets: Set<bigint>) {
    if (node.leaf()) {
      for (const { offset } of node.leafPointers) {
        uniqueOffsets.add(offset);
      }
    } else {
      for (let idx = 0; idx <= node.numPointers() - 1; idx++) {
        const childPtr = node.pointer(idx);
        const childNode = await this.btree.readNode(childPtr);
        await this.traverseLeafs(childNode, uniqueOffsets);
      }
    }
  }
}
