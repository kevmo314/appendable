class ReferencedValue {
	private dataPointer: MemoryPointer;
	public value: Buffer;

	constructor(dataPointer: MemoryPointer, value: Buffer) {
		this.dataPointer = dataPointer;
		this.value = value;
	}
}

export class MemoryPointer {
	offset: number;
	length: number;

	constructor(offset: number, length: number) {
		this.offset = offset;
		this.length = length;
	}
}

interface DataHandler {
	read(buffer: Uint8Array, offset: number): Promise<number>;
}

export class BPTreeNode {
	public dataHandler: DataHandler;
	public pointers: MemoryPointer[];
	public keys: ReferencedValue[];

	constructor(
		dataHandler: DataHandler,
		pointers: MemoryPointer[],
		keys: ReferencedValue[]
	) {
		this.dataHandler = dataHandler;
		this.pointers = pointers;
		this.keys = keys;
	}

	addPointer(pointer: MemoryPointer) {
		this.pointers.push(pointer);
	}

	addKey(key: ReferencedValue) {
		this.keys.push(key);
	}

	async readAtOffset(buffer: Uint8Array, offset: number): Promise<number> {
		return await this.dataHandler.read(buffer, offset);
	}

	async leaf(): Promise<boolean> {
		return this.pointers.length === this.keys.length;
	}

	async readFrom(buffer: Buffer): Promise<number> {
		let offset = 0;
		let size: number;

		let m = 4;

		try {
			// since we are reading a 32-bit integer, we move by 4 bytes
			size = buffer.readInt32BE(offset);
			offset += 4;

			const leaf = size < 0;
			const absSize = Math.abs(size);

			this.pointers = new Array(absSize + (leaf ? 0 : 1))
				.fill(null)
				.map(() => new MemoryPointer(0, 0));

			this.keys = new Array(absSize)
				.fill(null)
				.map(
					() => new ReferencedValue(new MemoryPointer(0, 0), Buffer.alloc(0))
				);

			for (let idx = 0; idx <= this.keys.length - 1; idx++) {
				const l = buffer.readUInt32BE(offset);

				offset += 4;
				m += 4;

				if (l == 0) {
					const dpOffset = buffer.readUInt32BE(offset);
					offset += 4;
					const dpLength = buffer.readUInt32BE(offset);
					offset += 4;

					const dataPointer = new MemoryPointer(dpOffset, dpLength);
					const keyValue = Buffer.alloc(dpLength);

					await this.dataHandler.read(keyValue, dpOffset);
					this.keys[idx] = new ReferencedValue(dataPointer, keyValue);
					m += 12;
				} else {
					const keyValue = buffer.slice(offset, offset + l);
					this.keys[idx] = new ReferencedValue(
						new MemoryPointer(0, 0),
						keyValue
					);

					offset += l;
					m += l;
				}
			}

			for (let idx = 0; idx <= this.pointers.length - 1; idx++) {
				const pointerOffset = buffer.readUint32BE(offset);
				offset += 4;

				const pointerLength = buffer.readUint32BE(offset);
				offset += 4;

				this.pointers[idx] = new MemoryPointer(pointerOffset, pointerLength);

				m += 8;
			}
		} catch (error) {
			return 0;
		}
		return m;
	}

	async bsearch(key: Uint8Array): Promise<[number, boolean]> {
		let lo = 0;
		let hi = this.keys.length - 1;

		while (lo <= hi) {
			const mid = (lo + hi) / 2;
			const cmp = compareBytes(key, this.keys[mid].value);

			switch (cmp) {
				case 0:
					return [mid, true];
				case -1:
					hi = mid - 1;
				case 1:
					lo = mid + 1;
			}
		}

		return [lo, false];
	}
}

// https://pkg.go.dev/internal/bytealg#Compare
export function compareBytes(a: Uint8Array, b: Uint8Array): number {
	const len = Math.min(a.length, b.length);

	for (let idx = 0; idx <= len - 1; idx++) {
		if (a[idx] !== b[idx]) {
			return -1;
		}

		if (a[idx] > b[idx]) {
			return 1;
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
