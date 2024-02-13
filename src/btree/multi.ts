import { RangeResolver } from "../resolver";
import { MemoryPointer } from "./node";


const PAGE_SIZE_BYTES = 4096;

export class LinkedMetaPage {
	private resolver: RangeResolver;
	private offset: number;
	private pageData: ArrayBuffer | null;

	constructor(resolver: RangeResolver, offset: number) {
		this.resolver = resolver;
		this.offset = offset;
		this.pageData = null;
	}

	async root(): Promise<MemoryPointer | null> {
		const pageData = await this.getPage();

		// we seek by 12 bytes since offset is 8 bytes, length is 4 bytes
		const data = pageData.slice(this.offset, this.offset + 12);
		const view = new DataView(data);

		const pointerOffset = view.getBigUint64(0);
		const lengthOffset = view.getUint32(8);

		return {
			offset: pointerOffset,
			length: lengthOffset,
		};
	}

	async metadata(): Promise<ArrayBuffer> {
		const pageData = await this.getPage();	
		const lengthData = pageData.slice(this.offset + 24, this.offset + PAGE_SIZE_BYTES)	

		const lengthView = new DataView(lengthData);

		// read the first four because that represnts length
		const metadataLength = lengthView.getUint32(0);
		const metadata = pageData.slice(this.offset + 28, this.offset + metadataLength);

		return metadata;
	}

	private async getPage(): Promise<ArrayBuffer> {
		if (this.pageData) {
			return this.pageData
		}	

		const { data } = await this.resolver({
			start: this.offset,
			end: this.offset + PAGE_SIZE_BYTES - 1,
		});

		this.pageData = data;

		return data;
	}

	async next(): Promise<LinkedMetaPage | null> {
		const pageData = await this.getPage();
		const data = pageData.slice(this.offset + 12, this.offset + 12 + 8);

		const view = new DataView(data);

		const nextOffset = view.getBigUint64(0);

		const maxUint64 = BigInt(2) ** BigInt(64) - BigInt(1);
		if (nextOffset === maxUint64) {
			return null;
		}

		return new LinkedMetaPage(this.resolver, Number(nextOffset));
	}
}
