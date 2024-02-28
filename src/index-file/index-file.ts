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
import { parseMultipartBody } from "../range-request";

export class IndexFile {
  static async forUrl<T = any>(url: string) {
    return await IndexFile.forResolver<T>(async (ranges) => {
      const rangesHeader = ranges
        .map(({ start, end }) => `${start}-${end}`)
        .join(",");

      const response = await fetch(url, {
        headers: {
          Range: `bytes=${rangesHeader}`,
          Accept: "multipart/bytesranges",
        },
      });

      if (response.status !== 206) {
        throw new Error(`Expected 206 Partial Content, got ${response.status}`);
      }

      const contentType = response.headers.get("Content-Type");
      if (!contentType) {
        throw new Error("Missing Content-Type in response");
      }
      if (contentType.includes("multipart/byteranges")) {
        const boundary = contentType.split("boundary=")[1];
        const abuf = await response.arrayBuffer();
        const text = new TextDecoder().decode(abuf);

        const chunks = parseMultipartBody(text, boundary);

        // the last element is null since the final boundary marker is followed by another delim.
        if (chunks[chunks.length - 1].body === undefined) {
          chunks.pop();
        }
        return chunks.map(({ body: cbody, headers: cheaders }) => {
          const enc = new TextEncoder();
          const data = enc.encode(cbody).buffer;
          const totalLengthStr = cheaders["content-range"]?.split("/")[1];
          const totalLength = totalLengthStr ? parseInt(totalLengthStr, 10) : 0;

          return { data, totalLength };
        });
      } else if (response.headers.has("Content-Range")) {
        const abuf = await response.arrayBuffer();
        const totalLength = Number(
          response.headers.get("Content-Range")!.split("/")[1],
        );
        return [
          {
            data: abuf,
            totalLength: totalLength,
          },
        ];
      } else {
        throw new Error(`Unexpected response format: ${contentType}`);
      }
    });
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
    let mp = await this.tree();
    return [];
  }

  async fetchMetaPages(): Promise<void> {
    let currMp = await this.tree();
    while (true) {
      const offsets = await currMp.nextNOffsets();
      if (offsets.length > 0) {
        const ranges = offsets.map((o) => ({
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
      } else {
        break;
      }
    }
  }

  async indexHeaders(): Promise<IndexHeader[]> {
    await this.fetchMetaPages();

    let indexMetas: IndexMeta[] = [];
    for (const mp of this.linkedMetaPages) {
      const im = await readIndexMeta(await mp.metadata());
      indexMetas.push(im);
    }

    return collectIndexMetas(indexMetas);
  }
}
