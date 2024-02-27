import { LinkedMetaPage, ReadMultiBPTree } from "../btree/multi";
import { RangeResolver } from "../resolver";
import { PageFile } from "../btree/pagefile";
import {
  FileFormat,
  IndexHeader,
  IndexMeta,
  collectIndexMetas,
  readIndexMeta,
} from "./meta";
import { FieldType } from "../db/database";
import { parseMultipartBody } from "../range-request";
import { SkipList } from "./skiplist";

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
        console.log(chunks);
        return chunks.map((c) => {
          const enc = new TextEncoder();
          const data = enc.encode(c.body).buffer;

          const totalLengthStr = c.headers["content-range"]?.split("/")[1];
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

  synchronize(): Promise<void>;

  indexHeaders(): Promise<IndexHeader[]>;

  findMetaPage(header: string, fieldType: FieldType): Promise<LinkedMetaPage>;
}

export class IndexFileV1<T> implements VersionedIndexFile<T> {
  private _tree?: LinkedMetaPage;
  private skipList: SkipList = new SkipList();

  constructor(private resolver: RangeResolver) {}

  getResolver(): RangeResolver {
    return this.resolver;
  }

  async tree(): Promise<LinkedMetaPage> {
    if (this._tree) {
      return this._tree;
    }

    const pageFile = new PageFile(this.resolver);
    const tree = ReadMultiBPTree(this.resolver, pageFile);

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

  async synchronize(): Promise<void> {
    let root = await this.tree();

    let mp: LinkedMetaPage | null = await root.next();
    while (mp) {
      const indexMeta = await readIndexMeta(await mp.metadata());
      this.skipList.insert(indexMeta, mp);
      mp = await mp.next();
    }
  }

  async indexHeaders(): Promise<IndexHeader[]> {
    let root = await this.tree();
    let headers: IndexMeta[] = [];

    let mp: LinkedMetaPage | null = await root.next();
    while (mp) {
      const indexMeta = await readIndexMeta(await mp.metadata());
      headers.push(indexMeta);
      mp = await mp.next();
    }

    return collectIndexMetas(headers);
  }

  async findMetaPage(
    header: string,
    fieldType: FieldType,
  ): Promise<LinkedMetaPage> {
    return await this.skipList.search({ fieldName: header, fieldType });
  }
}
