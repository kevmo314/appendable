import { FieldType } from "../db/database";
import { FileFormat } from "../file/meta";
import { RangeResolver } from "../resolver/resolver";
import { decodeUvarint } from "../util/uvarint";
import { ReferencedValue } from "./btree";

export const pageSizeBytes = 4096;

export type MemoryPointer = { offset: bigint; length: number };
export class BTreeNode {
  public keys: ReferencedValue[];
  public leafPointers: MemoryPointer[];
  public internalPointers: bigint[];
  private readonly dataFileResolver: RangeResolver;
  private readonly fileFormat: FileFormat;
  private readonly pageFieldType: FieldType;

  constructor(
    keys: ReferencedValue[],
    leafPointers: MemoryPointer[],
    internalPointers: bigint[],
    dataFileResolver: RangeResolver,
    fileFormat: FileFormat,
    pageFieldType: FieldType,
  ) {
    this.keys = keys;
    this.leafPointers = leafPointers;
    this.internalPointers = internalPointers;
    this.dataFileResolver = dataFileResolver;
    this.fileFormat = fileFormat;
    this.pageFieldType = pageFieldType;
  }

  leaf(): boolean {
    return this.leafPointers.length > 0;
  }

  pointer(i: number): MemoryPointer {
    if (this.leaf()) {
      return this.leafPointers[i];
    }

    return {
      offset: this.internalPointers[i],
      length: 0, // disregard since this is a zeroed value in golang version
    };
  }

  numPointers(): number {
    return this.internalPointers.length + this.leafPointers.length;
  }

  async unmarshalBinary(buffer: ArrayBuffer, pageFieldWidth: number) {
    let dataView = new DataView(buffer);
    let size = dataView.getUint32(0, true);

    if (size > 2147483647) {
      size = size - 4294967296;
    }

    if (size === 0) {
      throw new Error("empty node");
    }

    const leaf = size < 0;

    if (leaf) {
      this.leafPointers = new Array<MemoryPointer>(-size)
        .fill({ offset: 0n, length: 0 })
        .map(() => ({
          offset: 0n,
          length: 0,
        }));
      this.keys = new Array(-size)
        .fill(null)
        .map(
          () =>
            new ReferencedValue({ offset: 0n, length: 0 }, new ArrayBuffer(0)),
        );
    } else {
      this.internalPointers = Array<bigint>(size + 1)
        .fill(0n)
        .map(() => 0n);
      this.keys = new Array(size)
        .fill(null)
        .map(
          () =>
            new ReferencedValue({ offset: 0n, length: 0 }, new ArrayBuffer(0)),
        );
    }

    let dpRanges = [];
    let dpIndexes: number[] = [];

    let m = 4;
    for (let idx = 0; idx <= this.keys.length - 1; idx++) {
      const { value: dpOffset, bytesRead: oBytes } = decodeUvarint(
        buffer.slice(m),
      );
      const { value: dpLength, bytesRead: lBytes } = decodeUvarint(
        buffer.slice(m + oBytes),
      );
      m += oBytes + lBytes;

      this.keys[idx].setDataPointer({
        offset: BigInt(dpOffset),
        length: dpLength,
      });

      if (pageFieldWidth === 0) {
        const dp = this.keys[idx].dataPointer;

        dpRanges.push({
          start: Number(dp.offset),
          end: Number(dp.offset) + dp.length - 1,
        });

        dpIndexes.push(idx);
      } else {
        // we are storing the values directly in the referenced value
        const value = buffer.slice(m, m + pageFieldWidth - 1);
        this.keys[idx].setValue(value);
        m += value.byteLength;
      }
    }

    if (dpRanges.length > 0) {
      const res = await this.dataFileResolver(dpRanges);
      res.forEach((res, index) => {
        const dpIndex = dpIndexes[index];
        const { data } = res;

        const parsedData = this.parseValue(data);
        this.keys[dpIndex].setValue(parsedData);
      });
    }

    for (let idx = 0; idx <= this.leafPointers.length - 1; idx++) {
      const { value: lpOffset, bytesRead: lBytes } = decodeUvarint(
        buffer.slice(m),
      );
      const { value: lpLength, bytesRead: oBytes } = decodeUvarint(
        buffer.slice(m + lBytes),
      );

      this.leafPointers[idx].offset = BigInt(lpOffset);
      this.leafPointers[idx].length = lpLength;
      m += oBytes + lBytes;
    }

    for (let idx = 0; idx <= this.internalPointers.length - 1; idx++) {
      const { value: ipOffset, bytesRead: oBytes } = decodeUvarint(
        buffer.slice(m),
      );
      this.internalPointers[idx] = BigInt(ipOffset);
      m += oBytes;
    }
  }

  parseValue(incomingData: ArrayBuffer): ArrayBuffer {
    const stringData = new TextDecoder().decode(incomingData);

    switch (this.fileFormat) {
      case FileFormat.JSONL:
        const jValue = JSON.parse(stringData);

        switch (this.pageFieldType) {
          case FieldType.Null:
            if (jValue !== null) {
              throw new Error(`unrecognized value for null type: ${jValue}`);
            }
            return new ArrayBuffer(0);

          case FieldType.Boolean:
            return new Uint8Array([jValue ? 1 : 0]).buffer;

          case FieldType.Float64:
          case FieldType.Int64:
          case FieldType.Uint64:
            const floatBuf = new ArrayBuffer(8);
            let floatBufView = new DataView(floatBuf);
            floatBufView.setFloat64(0, jValue);

            return floatBuf;

          case FieldType.String:
            const e = new TextEncoder().encode(jValue);
            return e.buffer;

          default:
            throw new Error(
              `Unexpected Field Type. Got: ${this.pageFieldType}`,
            );
        }

      case FileFormat.CSV:
        return incomingData;
    }
  }

  static async fromMemoryPointer(
    mp: MemoryPointer,
    resolver: RangeResolver,
    dataFilePointer: RangeResolver,
    fileFormat: FileFormat,
    pageFieldType: FieldType,
    pageFieldWidth: number,
  ): Promise<{ node: BTreeNode; bytesRead: number }> {
    const res = await resolver([
      {
        start: Number(mp.offset),
        end: Number(mp.offset) + 4096 - 1,
      },
    ]);
    const { data: bufferData } = res[0];
    const node = new BTreeNode(
      [],
      [],
      [],
      dataFilePointer,
      fileFormat,
      pageFieldType,
    );

    await node.unmarshalBinary(bufferData, pageFieldWidth);

    return { node, bytesRead: pageSizeBytes };
  }
}
