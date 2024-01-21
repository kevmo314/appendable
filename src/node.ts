import { RangeResolver } from "./resolver";

export type ReferencedValue = { dataPointer: MemoryPointer; value: Buffer };
export type MemoryPointer = { offset: number; length: number };

export class BPTreeNode {
	public pointers: MemoryPointer[];
	public keys: ReferencedValue[];

	constructor(pointers: MemoryPointer[], keys: ReferencedValue[]) {
		this.pointers = pointers;
		this.keys = keys;
	}

	leaf(): boolean {
		return this.pointers.length === this.keys.length;
	}

	static async fromMemoryPointer(
		mp: MemoryPointer,
		resolver: RangeResolver
	): Promise<{ node: BPTreeNode | null; bytesRead: number }> {
		try {
			let node = new BPTreeNode([], []);
			let { data: sizeData } = await resolver({
				start: mp.offset,
				end: mp.offset + mp.length,
			});

			let sizeBuffer = Buffer.from(sizeData);

			let size = sizeBuffer.readInt32BE(0);
			let leaf = size < 0;
			let absSize = Math.abs(size);

			node.pointers = new Array(absSize + (leaf ? 0 : 1))
				.fill(null)
				.map(() => ({ offset: 0, length: 0 }));
			node.keys = new Array(absSize).fill(null).map(() => ({
				dataPointer: { offset: 0, length: 0 },
				value: Buffer.alloc(0),
			}));

			let currentOffset = 4;

			for (let idx = 0; idx <= node.keys.length - 1; idx++) {
				let { data: keyData } = await resolver({
					start: currentOffset,
					end: currentOffset + 4,
				});

				let keyBuffer = Buffer.from(keyData);
				let l = keyBuffer.readUint32BE(0);

				currentOffset += 4;

				if (l === 0) {
					let { data: pointerData } = await resolver({
						start: currentOffset,
						end: currentOffset + 12,
					});
					let pointerBuffer = Buffer.from(pointerData);

					let dpOffset = pointerBuffer.readInt32BE(0);
					let dpLength = pointerBuffer.readUInt32BE(4);

					node.keys[idx].dataPointer = { offset: dpOffset, length: dpLength };
					currentOffset += 8;

					let { data: keyValue } = await resolver({
						start: dpOffset,
						end: dpOffset + dpLength - 1,
					});
					node.keys[idx].value = Buffer.from(keyValue);
					node.keys[idx].dataPointer.length = dpLength;
				} else {
					let { data: keyValue } = await resolver({
						start: currentOffset,
						end: currentOffset + l,
					});

					node.keys[idx].value = Buffer.from(keyValue);
					node.keys[idx].dataPointer.length = l;

					currentOffset += l;
				}
			}

			for (let idx = 0; idx <= node.pointers.length - 1; idx++) {
				let { data: offsetData } = await resolver({
					start: currentOffset,
					end: currentOffset + 4,
				});
				let offsetBuffer = Buffer.from(offsetData);

				let pointerOffset = offsetBuffer.readUint32BE(0);
				currentOffset += 4;

				let { data: lengthData } = await resolver({
					start: currentOffset,
					end: currentOffset + 4,
				});
				let lengthBuffer = Buffer.from(lengthData);

				let pointerLength = lengthBuffer.readUint32BE(0);
				currentOffset += 4;

				node.pointers[idx] = { offset: pointerOffset, length: pointerLength };
			}

			return { node, bytesRead: currentOffset };
		} catch (error) {
			// console.error(error);
			return { node: null, bytesRead: 0 };
		}
	}

	async bsearch(key: Uint8Array): Promise<number> {
		let lo = 0;
		let hi = this.keys.length - 1;

		while (lo <= hi) {
			const mid = Math.floor((lo + hi) / 2);
			const cmp = compareBytes(key, this.keys[mid].value);

			switch (cmp) {
				case 0:
					return mid;
				case -1:
					hi = mid - 1;
					break;
				case 1:
					lo = mid + 1;
					break;
			}
		}

		return ~lo;
	}
}

export function compareBytes(a: Uint8Array, b: Uint8Array): number {
	const len = Math.min(a.length, b.length);

	for (let idx = 0; idx < len; idx++) {
		if (a[idx] !== b[idx]) {
			return a[idx] < b[idx] ? -1 : 1;
		}
	}

	if (a.length < b.length) {
		return -1;
	}
	if (a.length > b.length) {
		return 1;
	}

	return 0;
}
