import { RangeResolver } from "../resolver";
import { MemoryPointer } from "./node";

export class LinkedMetaPage {
	private resolver: RangeResolver;
	private offset: number;

	constructor(resolver: RangeResolver, offset: number) {
		this.resolver = resolver;
		this.offset = offset;
	}

	async root(): Promise<MemoryPointer | null> {
		// we seek by 12 bytes since offset is 8 bytes, length is 4 bytes
		const { data } = await this.resolver({
			start: this.offset,
			end: this.offset + 12,
		});

		if (data.byteLength !== 12) {
			return null;
		}

		const view = new DataView(data);

		const pointerOffset = view.getBigUint64(0);
		const lengthOffset = view.getUint32(8);

		return {
			offset: pointerOffset,
			length: lengthOffset,
		};
	}

	async metadata(): Promise<ArrayBuffer> {
		const { data: lengthData } = await this.resolver({
			start: this.offset + 24,
			end: this.offset + 28,
		});

		const lengthView = new DataView(lengthData);

		// read the first four because that represnts length
		const metadataLength = lengthView.getUint32(0);

		// reard from 4 to 4 + length for metadata
		const { data: metadata } = await this.resolver({
			start: this.offset + 28,
			end: this.offset + 28 + metadataLength,
		});

		return metadata;
	}

	async next(): Promise<LinkedMetaPage | null> {
		const { data } = await this.resolver({
			start: this.offset + 12,
			end: this.offset + 12 + 8,
		});

		if (data.byteLength !== 8) {
			return null;
		}

		const view = new DataView(data);

		const nextOffset = view.getBigUint64(0);

		// since 2^64 - 1
		const maxUint64 = BigInt(2) ** BigInt(64) - BigInt(1);
		if (nextOffset === maxUint64) {
			// no next page
			return null;
		}

		return new LinkedMetaPage(this.resolver, Number(nextOffset));
	}
}
