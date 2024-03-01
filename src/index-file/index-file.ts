import {
  LinkedMetaPage,
  PAGE_SIZE_BYTES,
  ReadMultiBPTree,
} from "../btree/multi";
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

export class IndexFile {
  static async forUrl<T = any>(url: string) {
    return await IndexFile.forResolver<T>(
      async (ranges) => await requestRanges(url, ranges),
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

    const tree = ReadMultiBPTree(this.resolver, 0);

    this._tree = tree;
    return tree;
  }

  async metadata(): Promise<FileMeta> {
    const tree = await this.tree();

    const buffer = await tree.metadata();

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
