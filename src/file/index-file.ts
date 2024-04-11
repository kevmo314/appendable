import { LinkedMetaPage, PAGE_SIZE_BYTES, ReadMultiBTree } from "./multi";
import { RangeResolver } from "../resolver/resolver";
import {
  IndexHeader,
  IndexMeta,
  collectIndexMetas,
  readIndexMeta,
  readFileMeta,
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

export type FileMeta = {
  version: number;
  format: number;
  readOffset: bigint;
};

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
    if (this.linkedMetaPages.length === 0) {
      await this.fetchMetaPages();
    }

    let headerMps = [];

    for (let idx = 0; idx <= this.linkedMetaPages.length - 1; idx++) {
      const mp = this.linkedMetaPages[idx];
      const indexMeta = await readIndexMeta(await mp.metadata());
      if (indexMeta.fieldName === header) {
        if (fieldType === FieldType.Float64) {
          // if key is a number or bigint, we cast it as a float64 type
          if (
            indexMeta.fieldType === FieldType.Float64 ||
            indexMeta.fieldType === FieldType.Int64 ||
            indexMeta.fieldType === FieldType.Uint64
          ) {
            headerMps.push(mp);
          }
        } else {
          if (fieldType === indexMeta.fieldType) {
            headerMps.push(mp);
          }
        }
      }
    }

    return headerMps;
  }

  async fetchMetaPages(): Promise<void> {
    let currMp = await this.tree();
    let offsets = await currMp.nextNOffsets();

    while (offsets.length > 0) {
      let ranges = offsets.map((o) => ({
        start: Number(o),
        end: Number(o) + PAGE_SIZE_BYTES - 1,
      }));

      let res = await this.resolver(ranges);
      let idx = 0;
      for (const { data } of res) {
        this.linkedMetaPages.push(
          new LinkedMetaPage(this.resolver, offsets[idx], data),
        );
        idx++;
      }

      currMp = this.linkedMetaPages[this.linkedMetaPages.length - 1];
      offsets = await currMp.nextNOffsets();
    }
  }

  async indexHeaders(): Promise<IndexHeader[]> {
    if (this.linkedMetaPages.length === 0) {
      await this.fetchMetaPages();
    }

    let indexMetas: IndexMeta[] = [];
    for (const mp of this.linkedMetaPages) {
      const im = await readIndexMeta(await mp.metadata());
      indexMetas.push(im);
    }

    return collectIndexMetas(indexMetas);
  }
}
