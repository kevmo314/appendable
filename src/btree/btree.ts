import { BTreeNode, DataPointer, MemoryPointer } from "./node";
import { RangeResolver } from "../resolver/resolver";
import { TraversalIterator, TraversalRecord } from "./traversal";
import { FileFormat } from "../file/meta";
import { FieldType } from "../db/database";

export interface MetaPage {
  root(): Promise<MemoryPointer>;
}

type RootResponse = {
  rootNode: BTreeNode | null;
  pointer: MemoryPointer;
};

export class BTree {
  private readonly tree: RangeResolver;
  private meta: MetaPage;
  private readonly dataFileResolver: RangeResolver;
  private readonly fileFormat: FileFormat;
  private readonly pageFieldType: FieldType;
  private readonly pageFieldWidth: number;

  private rootNodeCache: Promise<BTreeNode> | null = null;
  private rootPointerCache: Promise<MemoryPointer> | null = null;

  // insight attributes below

  private readonly entries: number;
  private readonly tfMap: Map<DataPointer, number> = new Map<
    DataPointer,
    number
  >();

  constructor(
    tree: RangeResolver,
    meta: MetaPage,
    dataFileResolver: RangeResolver,
    fileFormat: FileFormat,
    pageFieldType: FieldType,
    pageFieldWidth: number,
    entries: number,
  ) {
    this.tree = tree;
    this.meta = meta;
    this.dataFileResolver = dataFileResolver;
    this.fileFormat = fileFormat;
    this.pageFieldType = pageFieldType;
    this.pageFieldWidth = pageFieldWidth;
    this.entries = entries;
  }

  async root(): Promise<RootResponse> {
    if (!this.rootNodeCache || !this.rootPointerCache) {
      this.rootPointerCache = this.meta.root();
      this.rootNodeCache = this.rootPointerCache.then(ptr => this.readNode(ptr));
    }

      const rootNode = await this.rootNodeCache;
      const pointer = await this.rootPointerCache;

      if (!rootNode) {
        return {
          rootNode: null,
          pointer,
        }
      }

      return {
        rootNode,
        pointer
      }
  }

  async readNode(ptr: MemoryPointer): Promise<BTreeNode> {
    try {
      const { node, bytesRead } = await BTreeNode.fromMemoryPointer(
        ptr,
        this.tree,
        this.dataFileResolver,
        this.fileFormat,
        this.pageFieldType,
        this.pageFieldWidth,
      );

      if (!bytesRead) {
        throw new Error("bytes read do not line up");
      }

      return node;
    } catch (error) {
      throw new Error(`${error}`);
    }
  }

  public iter(key: ReferencedValue): TraversalIterator {
    return new TraversalIterator(this, key);
  }

  async first(): Promise<ReferencedValue> {
    let { rootNode } = await this.root();
    if (!rootNode) {
      throw new Error("unable to get root node");
    }

    let currNode = await this.readNode(rootNode.pointer(0));

    while (!currNode.leaf()) {
      const childPointer = currNode.pointer(0);
      currNode = await this.readNode(childPointer);
    }

    return currNode.keys[0];
  }

  async last(): Promise<ReferencedValue> {
    let { rootNode } = await this.root();
    if (!rootNode) {
      throw new Error("unable to get root node");
    }

    let currNode = await this.readNode(
      rootNode.pointer(rootNode.numPointers() - 1),
    );

    while (!currNode.leaf()) {
      const childPointer = currNode.pointer(currNode.numPointers() - 1);
      currNode = await this.readNode(childPointer);
    }

    return currNode.keys[currNode.keys.length - 1];
  }

  async traverse(
    key: ReferencedValue,
    node: BTreeNode,
    pointer: MemoryPointer,
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
    key: ReferencedValue,
  ): Promise<[ReferencedValue, MemoryPointer]> {
    const p = this.iter(key);

    if (!(await p.next())) {
      return [
        new ReferencedValue(
          { offset: 0n, length: 0 },
          new Uint8Array(0).buffer,
        ),
        { offset: 0n, length: 0 },
      ];
    }

    return [p.getKey(), p.getPointer()];
  }

  // these traverse through the iterator
  public async termFrequency(key: ReferencedValue) {
    const p = await this.iter(key);

    while (await p.next()) {
      const currentKey = p.getKey();

      if (ReferencedValue.compareBytes(currentKey.value, key.value) !== 0) {
        break;
      }
      const mp = p.getPointer();

      const dp: DataPointer = {
        start: Number(mp.offset),
        end: Number(mp.offset) + mp.length - 1,
      };

      this.tfMap.set(dp, (this.tfMap.get(dp) ?? 0) + 1);
    }

    return this.tfMap;
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
  b: ReferencedValue,
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
  target: ReferencedValue,
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
