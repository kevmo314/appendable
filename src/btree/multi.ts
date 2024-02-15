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

	/**
	 * `metadata()` gets the page data. It does the following:
	 * 		(1) creates a slice from 24 to the end of the page
	 * 		(2) it reads the first four bytes of that slice which gives us the length to seek to
	 * 		(3) slices from [24, (24 + dataLength)] which contain metadata
	 */
	async metadata(): Promise<ArrayBuffer> {
		console.log("metadata entered");
		const pageData = await this.getMetaPage();
		console.log("page data: ", pageData)
		const lengthData = pageData.slice(
			24,
			PAGE_SIZE_BYTES
		);

		const lengthView = new DataView(lengthData);

		// read the first four because that represents length
		const metadataLength = lengthView.getUint32(0);

		console.log("metadatalength: ", metadataLength);
		return pageData.slice(
			28,
			28 + metadataLength
		);
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
