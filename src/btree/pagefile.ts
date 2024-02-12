import { RangeResolver } from "../resolver";

const MAX_FREE_PAGE_INDICIES = 512;
const PAGE_SIZE_BYTES = 4096;

export class PageFile {
	private resolver: RangeResolver;
	private pageSize: number = PAGE_SIZE_BYTES;

	constructor(resolver: RangeResolver) {
		this.resolver = resolver;
	}

	async readPage(pageIndex: number): Promise<ArrayBuffer> {
		if (pageIndex < 0) {
			throw new Error("page index cannot be negative");
		}

		const start = pageIndex * this.pageSize;
		const end = start + this.pageSize - 1;
		const { data } = await this.resolver({ start, end });

		return data;
	}
}
