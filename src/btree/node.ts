import { FieldType } from "../db/database";
import { FileFormat } from "../index-file/meta";
import { RangeResolver } from "../resolver";
import { ReferencedValue } from "./bptree";

export const pageSizeBytes = 4096;

export type MemoryPointer = { offset: bigint; length: number };
export class BPTreeNode {
	public keys: ReferencedValue[];
	public leafPointers: MemoryPointer[];
	public internalPointers: bigint[];
	private readonly dataFileResolver: RangeResolver;
	private fileFormat: FileFormat;
	private pageFieldType: FieldType;

	constructor(
		keys: ReferencedValue[],
		leafPointers: MemoryPointer[],
		internalPointers: bigint[],
		dataFileResolver: RangeResolver,
		fileFormat: FileFormat,
		pageFieldType: FieldType
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
			length: 0, // disregard since this is a free value in golang version
		};
	}

	numPointers(): number {
		return this.internalPointers.length + this.leafPointers.length;
	}

	size(): bigint {
		let size = 4;

		for (let idx = 0; idx <= this.keys.length - 1; idx++) {
			const k = this.keys[idx];
			if (k.dataPointer.length > 0) {
				size += 4 + 12;
			} else {
				size += 4 * k.value.byteLength;
			}
		}

		for (let idx = 0; idx <= this.leafPointers.length - 1; idx++) {
			size += 12;
		}
		for (let idx = 0; idx <= this.internalPointers.length - 1; idx++) {
			size += 8;
		}

		return BigInt(size);
	}

	async unmarshalBinary(buffer: ArrayBuffer) {
		let dataView = new DataView(buffer);
		let size = dataView.getUint32(0);

		if (size > 2147483647) {
			size = size - 4294967296;
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
						new ReferencedValue({ offset: 0n, length: 0 }, new ArrayBuffer(0))
				);
		} else {
			this.internalPointers = Array<bigint>(size + 1)
				.fill(0n)
				.map(() => 0n);
			this.keys = new Array(size)
				.fill(null)
				.map(
					() =>
						new ReferencedValue({ offset: 0n, length: 0 }, new ArrayBuffer(0))
				);
		}

		if (size === 0) {
			throw new Error("empty node");
		}

		let m = 4;
		for (let idx = 0; idx <= this.keys.length - 1; idx++) {
			// this is the case when we store the pointer to the datafile
			dataView = new DataView(buffer, m, 4);
			const l = dataView.getUint32(0);
			if (l === ~0 >>> 0) {
				dataView = new DataView(buffer, m + 4);
				const dpOffset = dataView.getBigUint64(0);
				const dpLength = dataView.getUint32(8);
				this.keys[idx].setDataPointer({ offset: dpOffset, length: dpLength });

				const dp = this.keys[idx].dataPointer;

				const { data } = await this.dataFileResolver({
					start: Number(dp.offset),
					end: Number(dp.offset) + dp.length - 1,
				});

				const parsedData = this.parseValue(data);

				this.keys[idx].setValue(parsedData);
				m += 4 + 12;
			} else {
				// we are storing the values directly in the referenced value
				const value = buffer.slice(m + 4, m + 4 + l);
				this.keys[idx].setValue(value);
				m += 4 + l;
			}
		}

		for (let idx = 0; idx <= this.leafPointers.length - 1; idx++) {
			dataView = new DataView(buffer, m);
			this.leafPointers[idx].offset = dataView.getBigUint64(0);
			this.leafPointers[idx].length = dataView.getUint32(8);

			m += 12;
		}

		for (let idx = 0; idx <= this.internalPointers.length - 1; idx++) {
			dataView = new DataView(buffer, m);
			this.internalPointers[idx] = dataView.getBigUint64(0);

			m += 8;
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
						floatBufView.setFloat64(0, jValue, true);

						return floatBuf;

					case FieldType.String:
						const e = new TextEncoder().encode(jValue);
						return e.buffer;

					default:
						throw new Error(
							`Unexpected Field Type. Got: ${this.pageFieldType}`
						);
				}

			case FileFormat.CSV:
				return incomingData;
		}

		throw new Error("unexpected parsing error occured");
	}

	static async fromMemoryPointer(
		mp: MemoryPointer,
		resolver: RangeResolver,
		dataFilePointer: RangeResolver,
		fileFormat: FileFormat,
		pageFieldType: FieldType
	): Promise<{ node: BPTreeNode; bytesRead: number }> {
		const { data: bufferData } = await resolver({
			start: Number(mp.offset),
			end: Number(mp.offset) + 4096 - 1,
		});
		const node = new BPTreeNode(
			[],
			[],
			[],
			dataFilePointer,
			fileFormat,
			pageFieldType
		);

		await node.unmarshalBinary(bufferData);

		return { node, bytesRead: pageSizeBytes };
	}
}
