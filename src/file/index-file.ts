import { LinkedMetaPage, PAGE_SIZE_BYTES, ReadMultiBTree } from "./multi";
import { RangeResolver } from "../resolver/resolver";
import {
  IndexHeader,
  IndexMeta,
  collectIndexMetas,
  readIndexMeta,
  readFileMeta,
  FileMeta,
} from "./meta";
import { FieldType } from "../db/database";
import { Config } from "..";
import { requestRanges } from "../resolver/range-request";

export class IndexFile {
  static async forUrl<T = any>(url: string, config: Config) {
    return await IndexFile.forResolver<T>(
      async (ranges) => await requestRanges(url, ranges, config),
    );
  }

  static async forResolver<T = any>(
    resolver: RangeResolver,
  ): Promise<VersionedIndexFile<T>> {
    return new IndexFileV1<T>(resolver);
  }
}

export interface VersionedIndexFile<T> {
  getResolver(): RangeResolver;

  tree(): Promise<LinkedMetaPage>;

  metadata(): Promise<FileMeta>;

  indexHeaders(): Promise<IndexHeader[]>;

  seek(header: string, fieldType: FieldType): Promise<LinkedMetaPage[]>;

  fetchMetaPages(): Promise<void>;
}

export class IndexFileV1<T> implements VersionedIndexFile<T> {
  private _tree?: LinkedMetaPage;

  private linkedMetaPages: LinkedMetaPage[] = [];

  constructor(private resolver: RangeResolver) {}

  getResolver(): RangeResolver {
    return this.resolver;
  }

  async tree(): Promise<LinkedMetaPage> {
    if (this._tree) {
      return this._tree;
    }

    const tree = ReadMultiBTree(this.resolver, 0);

    this._tree = tree;
    return tree;
  }

  async metadata(): Promise<FileMeta> {
    const tree = await this.tree();
    const buffer = await tree.metadata();

    return readFileMeta(buffer);
  }

  async seek(header: string, fieldType: FieldType): Promise<LinkedMetaPage[]> {
    const tree = await this.tree();
    let currMp = await tree.next();

    if (!currMp) {
      throw new Error(`failed to fetch meta pages`);
    }

    let headerMps = [];

    while (true) {
      const indexMeta = readIndexMeta(await currMp.metadata());
      if (indexMeta.fieldName === header) {
        if (fieldType === FieldType.Float64) {
          // if key is a number or bigint, we cast it as a float64 type
          if (
            indexMeta.fieldType === FieldType.Float64 ||
            indexMeta.fieldType === FieldType.Int64 ||
            indexMeta.fieldType === FieldType.Uint64
          ) {
            headerMps.push(currMp);
          }
        } else {
          if (fieldType === indexMeta.fieldType) {
            headerMps.push(currMp);
          }
        }
      }

      const nextMp = await currMp.next();
      if (!nextMp) {
        break;
      }
      currMp = nextMp;
    }

    return headerMps;
  }

  async fetchMetaPages(): Promise<void> {
    const tree = await this.tree();
    let currMp = await tree.next();

    if (!currMp) {
      throw new Error(`failed to fetch meta pages`);
    }

    while (true) {
      this.linkedMetaPages.push(currMp);

      const nextMp = await currMp.next();
      if (!nextMp) {
        break;
      }
      currMp = nextMp;
    }
  }

  async indexHeaders(): Promise<IndexHeader[]> {
    if (this.linkedMetaPages.length === 0) {
      await this.fetchMetaPages();
    }

    let indexMetas: IndexMeta[] = [];
    for (let idx = 0; idx <= this.linkedMetaPages.length - 1; idx++) {
      const currMp = this.linkedMetaPages[idx];
      const im = readIndexMeta(await currMp.metadata());
      indexMetas.push(im);
      const nextMp = await currMp.next();
      if (!nextMp) {
        break;
      }
    }

    return collectIndexMetas(indexMetas);
  }
}
