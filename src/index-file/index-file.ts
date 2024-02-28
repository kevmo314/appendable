import { LinkedMetaPage, ReadMultiBPTree } from "../btree/multi";
import { LengthIntegrityError, RangeResolver } from "../resolver";
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

      switch (response.status) {
        case 200:
          // fallback to resolving ranges individually
          const individualRangePromises = ranges.map(
            async ({ start, end, expectedLength }) => {
              const rangeHeader = `${start}-${end}`;
              const res = await fetch(url, {
                headers: { Range: `bytes=${rangeHeader}` },
              });

              const totalLength = Number(
                response.headers.get("Content-Range")!.split("/")[1],
              );
              if (expectedLength && totalLength !== expectedLength) {
                throw new LengthIntegrityError();
              }
              return {
                data: await response.arrayBuffer(),
                totalLength: totalLength,
              };
            },
          );
          return Promise.all(individualRangePromises)
            .then((res) => {
              return res;
            })
            .catch((error) => {
              throw new Error(
                `error occured when fetching for individual range promises: ${error}`,
              );
            });
        case 206:
          const contentType = response.headers.get("Content-Type");
          if (!contentType) {
            throw new Error("Missing Content-Type in response");
          }
          if (contentType.includes("multipart/byteranges")) {
            const boundary = contentType.split("boundary=")[1];
            const text = await response.text();

            const chunks = parseMultipartBody(text, boundary);

            // the last element is null since the final boundary marker is followed by another delim.
            if (chunks[chunks.length - 1].body === undefined) {
              chunks.pop();
            }

            const enc = new TextEncoder();
            return chunks.map((c) => {
              const data = enc.encode(c.body).buffer;

              const totalLengthStr = c.headers["content-range"]?.split("/")[1];
              const totalLength = totalLengthStr
                ? parseInt(totalLengthStr, 10)
                : 0;

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
        default:
          throw new Error(
            `Expected 206 or 200 response, got ${response.status}`,
          );
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

  async seek(header: string, fieldType: FieldType): Promise<LinkedMetaPage[]> {
    let mp = await this.tree();

    let headerMps = [];

    while (mp) {
      const next = await mp.next();
      if (next === null) {
        return headerMps;
      }

      const indexMeta = await readIndexMeta(await next.metadata());
      if (indexMeta.fieldName === header) {
        if (fieldType === FieldType.Float64) {
          // if key is a number or bigint, we cast it as a float64 type
          if (
            indexMeta.fieldType === FieldType.Float64 ||
            indexMeta.fieldType === FieldType.Int64 ||
            indexMeta.fieldType === FieldType.Uint64
          ) {
            headerMps.push(next);
          }
        } else {
          if (indexMeta.fieldType === fieldType) {
            headerMps.push(next);
          }
        }
      }

      mp = next;
    }

    if (headerMps.length === 0) {
      throw new Error(
        `No LinkedMetaPage with ${header} and type ${fieldType} exists`,
      );
    }

    return headerMps;
  }

  async indexHeaders(): Promise<IndexHeader[]> {
    let headers: IndexMeta[] = [];

    let mp = await this.tree();

    while (mp) {
      const next = await mp.next();
      if (next === null) {
        return collectIndexMetas(headers);
      }

      const nextBuffer = await next.metadata();
      const indexMeta = await readIndexMeta(nextBuffer);

      headers.push(indexMeta);

      mp = next;
    }

    return collectIndexMetas(headers);
  }
}
