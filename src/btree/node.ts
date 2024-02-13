import { RangeResolver } from "../resolver";
import { ReferencedValue } from "./bptree";

export type MemoryPointer = { offset: bigint; length: number };
export class BPTreeNode {
	public keys: ReferencedValue[];
	public leafPointers: MemoryPointer[];
	public internalPointers: bigint[];
	private data: ArrayBuffer;

	constructor(
		keys: ReferencedValue[],
		leafPointers: MemoryPointer[],
		internalPointers: bigint[],
		data: ArrayBuffer
	) {
		this.keys = keys;
		this.leafPointers = leafPointers;
		this.internalPointers = internalPointers;
		this.data = data;
	}

	leaf(): boolean {
		return this.leafPointers.length > 0;
	}

	pointer(i: number): MemoryPointer {
		if (this.leaf()) {
			return this.leafPointers[
				(this.leafPointers.length + i) % this.leafPointers.length
			];
		}

		return {
			offset:
				this.internalPointers[
					this.internalPointers.length + (i % this.internalPointers.length)
				],
			length: 0, // disregard since this is a free value in golang version
		};
	}

	size(): bigint {
		let size = 4;

		for (let idx = 0; idx <= this.keys.length - 1; idx++) {
			const k = this.keys[idx];
			if (k.dataPointer.length > 0) {
				size += 4 + 12;
			} else {
				size += 4 * k.value.byteLength; // bytelength over length
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
		let dataView = new DataView(buffer.slice(0, 4));
		const size = dataView.getUint32(0);

		const leaf = size < 0;

		if (leaf) {
			this.leafPointers = Array<MemoryPointer>(-size);
			this.keys = Array<ReferencedValue>(-size);
		} else {
			this.internalPointers = Array<bigint>(size + 1);
			this.keys = Array<ReferencedValue>(size);
		}

		if (size === 0) {
			throw new Error("empty node");
		}

		let m = 4;

		for (let idx = 0; idx <= this.keys.length - 1; idx++) {
			dataView = new DataView(buffer.slice(m, m + 4));
			const l = dataView.getUint32(0);
			if (l === ~0 >>> 0) {
				dataView = new DataView(buffer.slice(m + 4, m + 12));
				this.keys[idx].dataPointer.offset = dataView.getBigUint64(0);
				dataView = new DataView(buffer.slice(m + 12, m + 16));
				this.keys[idx].dataPointer.length = dataView.getUint32(0);

				const dp = this.keys[idx].dataPointer;
				const dataSlice = this.data.slice(
					Number(dp.offset),
					Number(dp.offset + BigInt(dp.length))
				);
				this.keys[idx].value = new Uint8Array(dataSlice);

				m += 4 + 12;
			} else {
				this.keys[idx].value = new Uint8Array(buffer.slice(m + 4, m + 4 + l));
				m += 4 + l;
			}
		}

		for (let idx = 0; idx <= this.leafPointers.length - 1; idx++) {
			dataView = new DataView(buffer.slice(m, m + 8));
			this.leafPointers[idx].offset = dataView.getBigUint64(0);
			dataView = new DataView(buffer.slice(m + 8, m + 12));
			this.leafPointers[idx].length = dataView.getUint32(0);

			m += 12;
		}

		for (let idx = 0; idx <= this.internalPointers.length - 1; idx++) {
			dataView = new DataView(buffer.slice(m, m + 8));
			this.internalPointers[idx] = dataView.getBigUint64(0);

			m += 8;
		}
	}

	static async fromMemoryPointer(
		mp: MemoryPointer,
		resolver: RangeResolver,
		data: ArrayBuffer
	): Promise<{ node: BPTreeNode; bytesRead: number }> {
		const { data: bufferData } = await resolver({
			start: Number(mp.offset),
			end: Number(mp.offset) + 4096 - 1,
		});
		const node = new BPTreeNode([], [], [], data);
		await node.unmarshalBinary(bufferData);

		return { node, bytesRead: 4096 };
	}
}
