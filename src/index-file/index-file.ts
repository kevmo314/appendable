import { LinkedMetaPage } from "../btree/multi";
import { RangeResolver } from "../resolver";
import {
  FileFormat,
  IndexHeader,
  IndexMeta,
  collectIndexMetas,
  readIndexMeta,
} from "./meta";
import { FieldType } from "../db/database";
import { requestRanges } from "../range-request";
import { Config } from "..";
import { PageFile, ReadMultiBPTree } from "../../pkg/btree/pagefile";

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

  tree(): Promise<PageFile>;

  metadata(): Promise<FileMeta>;

  indexHeaders(): Promise<IndexHeader[]>;

  seek(header: string, fieldType: FieldType): Promise<LinkedMetaPage[]>;

  fetchMetaPages(): Promise<void>;
}

export class IndexFileV1<T> implements VersionedIndexFile<T> {
  private _tree?: PageFile;

  private fileMeta: LinkedMetaPage | null = null;
  private linkedMetaPages: LinkedMetaPage[] = [];

  constructor(private resolver: RangeResolver) {}

  getResolver(): RangeResolver {
    return this.resolver;
  }

  async tree(): Promise<PageFile> {
    if (this._tree) {
      return this._tree;
    }

    const tree = ReadMultiBPTree(this.resolver, 0);

    this._tree = tree;
    return tree;
  }

  async metadata(): Promise<FileMeta> {
    if (this.fileMeta === null) {
      await this.fetchMetaPages();
    }

    const buffer = await this.fileMeta!.metadata();

    // unmarshall binary for FileMeta
    if (buffer.byteLength < 10) {
      throw new Error(
        `incorrect byte length! Want: 10, got ${buffer.byteLength}`,
      );
    }

    const dataView = new DataView(buffer);
    const version = dataView.getUint8(0);
    const formatByte = dataView.getUint8(1);

    if (Object.values(FileFormat).indexOf(formatByte) === -1) {
      throw new Error(`unexpected file format. Got: ${formatByte}`);
    }

    const readOffset = dataView.getBigUint64(2, true);

    return {
      version: version,
      format: formatByte,
      readOffset: readOffset,
    };
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
          if (indexMeta.fieldType === fieldType) {
            headerMps.push(mp);
          }
        }
      }
    }

    return headerMps;
  }

  async fetchMetaPages(): Promise<void> {
    let currPage = await this.tree();
    let mps = await currPage.splitPage();
    let offsets = await currPage.nextNOffsets();
    let lastoffset = offsets[offsets.length - 1];

    const nextPage = new PageFile(this.resolver, lastoffset);
    if (nextPage) {
      const currMps = await nextPage.splitPage();
      mps = [...mps, ...currMps];
    }

    this.fileMeta = mps[0];
    this.linkedMetaPages = mps.slice(1);
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
