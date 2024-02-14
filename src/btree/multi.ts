import { RangeResolver } from "../resolver";
import { MemoryPointer } from "./node";
import {PageFile} from "./pagefile";

const PAGE_SIZE_BYTES = 4096;

export class LinkedMetaPage {
	private resolver: RangeResolver;
	private offset: bigint;
	private metaPageData: ArrayBuffer | null;

	constructor(resolver: RangeResolver, offset: bigint) {
		this.resolver = resolver;
		this.offset = offset;
		this.metaPageData = null;
	}

	async root(): Promise<MemoryPointer | null> {
		const pageData = await this.getMetaPage();

		// we seek by 12 bytes since offset is 8 bytes, length is 4 bytes
		const data = pageData.slice(Number(this.offset), Number(this.offset) + 11);
		const view = new DataView(data);

		const pointerOffset = view.getBigUint64(0);
		const lengthOffset = view.getUint32(8);

		return {
			offset: pointerOffset,
			length: lengthOffset,
		};
	}

	async metadata(): Promise<ArrayBuffer> {
		const pageData = await this.getMetaPage();
		const lengthData = pageData.slice(
			Number(this.offset) + 24,
			Number(this.offset) + PAGE_SIZE_BYTES - 1
		);

		const lengthView = new DataView(lengthData);

		// read the first four because that represnts length
		const metadataLength = lengthView.getUint32(0);
		const metadata = pageData.slice(
			Number(this.offset) + 28,
			Number(this.offset) + metadataLength - 1
		);

		return metadata;
	}

	private async getMetaPage(): Promise<ArrayBuffer> {
		if (this.metaPageData) {
			return this.metaPageData;
		}

		const { data } = await this.resolver({
			start: Number(this.offset),
			end: Number(this.offset) + PAGE_SIZE_BYTES - 1,
		});

		this.metaPageData= data;

		return data;
	}

	async next(): Promise<LinkedMetaPage | null> {
		const pageData = await this.getMetaPage();
		const data = pageData.slice(Number(this.offset) + 12, Number(this.offset) + 12 + 7);

		const view = new DataView(data);

		const nextOffset = view.getBigUint64(0);

		const maxUint64 = BigInt(2) ** BigInt(64) - BigInt(1);
		if (nextOffset === maxUint64) {
			return null;
		}

		return new LinkedMetaPage(this.resolver, nextOffset);
	}
}


export function ReadMultiBPTree(resolver: RangeResolver, pageFile: PageFile): LinkedMetaPage {
	const offset = pageFile.page(0);

	return new LinkedMetaPage(resolver, offset);
}
