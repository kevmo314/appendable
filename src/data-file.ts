import { RangeResolver } from "./resolver";

export class DataFile {
	private originalResolver?: RangeResolver;

	private constructor(
		private resolver: (
			start: number,
			end: number,
			expectedLength?: number
		) => Promise<ArrayBuffer>
	) {}

	static forUrl(url: string) {
		return DataFile.forResolver(async ({ start, end }) => {
			const response = await fetch(url, {
				headers: { Range: `bytes=${start}-${end}` },
			});
			const totalLength = Number(
				response.headers.get("Content-Range")!.split("/")[1]
			);
			return {
				data: await response.arrayBuffer(),
				totalLength: totalLength,
			};
		});
	}

	static forResolver(resolver: RangeResolver) {
		const instance = new DataFile(async (start, end, expectedLength?) => {
			const result = await resolver({ start, end, expectedLength });
			return result.data;
		});
		instance.originalResolver = resolver; 
		return instance;
  }
  
  getResolver(): RangeResolver | undefined {
    return this.originalResolver;
}

	async get(startByteOffset: number, endByteOffset: number) {
		const data = await this.resolver(startByteOffset, endByteOffset);
		return new TextDecoder().decode(data);
	}
}
