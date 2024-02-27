import { LinkedMetaPage } from "../btree/multi";
import { FieldType } from "../db/database";
import { IndexMeta, readIndexMeta } from "./meta";

interface SkipListNode {
  fieldName: string; // fieldName and fieldType form a composite key
  fieldType: FieldType;
  mp: LinkedMetaPage | null;
  next: Array<SkipListNode | null>;
}

export class SkipList {
  private head: SkipListNode;
  private maxLevel: number;

  constructor(maxLevel?: number) {
    this.maxLevel = maxLevel ? maxLevel : 16;
    this.head = {
      fieldName: "",
      fieldType: FieldType.String, // since this value is 0
      mp: null,
      next: new Array(this.maxLevel).fill(null),
    };
  }

  private coinFlip(): number {
    const p = 0.5; // the probability factor
    let level = 1;
    while (Math.random() < p && level < this.maxLevel) {
      level++;
    }
    return level;
  }

  async insert(mpIndexMeta: IndexMeta, mp: LinkedMetaPage): Promise<void> {
    const newNodeLevel = this.coinFlip();

    let updatePath: (SkipListNode | null)[] = new Array(this.maxLevel).fill(
      null,
    );

    let currNode = this.head;

    for (let idx = this.maxLevel - 1; idx >= 0; idx--) {
      while (
        currNode.next[idx] !== null &&
        (currNode.next[idx]!.fieldName < mpIndexMeta.fieldName ||
          (currNode.next[idx]!.fieldName === mpIndexMeta.fieldName &&
            currNode.next[idx]!.fieldType < mpIndexMeta.fieldType))
      ) {
        currNode = currNode.next[idx]!;
      }
      updatePath[idx] = currNode;
    }

    const newNode: SkipListNode = {
      fieldName: mpIndexMeta.fieldName,
      fieldType: mpIndexMeta.fieldType,
      mp: mp,
      next: new Array(newNodeLevel).fill(null),
    };

    for (let idx = 0; idx <= newNodeLevel - 1; idx++) {
      if (updatePath[idx]) {
        newNode.next[idx] = updatePath[idx]!.next[idx];
        updatePath[idx]!.next[idx] = newNode;
      }
    }
  }

  async search(query: IndexMeta): Promise<LinkedMetaPage> {
    let currNode = this.head;
    let count = 0;

    for (let level = this.maxLevel - 1; level >= 0; level--) {
      while (currNode.next[level] !== null) {
        if (
          currNode.next[level]!.fieldName === query.fieldName &&
          currNode.next[level]!.fieldType === query.fieldType
        ) {
          const mp = currNode.next[level]!.mp;
          if (!mp) {
            throw new Error(`metapage found is null`);
          }
          console.log("it took count :", count);
          return mp;
        } else if (
          currNode.next[level]!.fieldName < query.fieldName ||
          (currNode.next[level]!.fieldName === query.fieldName &&
            currNode.next[level]!.fieldType < query.fieldType)
        ) {
          currNode = currNode.next[level]!;
        } else {
          break;
        }

        count += 1;
      }
    }

    throw new Error(
      `metapage not found for ${query.fieldName} and ${query.fieldType}`,
    );
  }
}
