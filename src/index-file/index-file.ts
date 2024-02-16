import {LinkedMetaPage, ReadMultiBPTree} from "../btree/multi";
import { LengthIntegrityError, RangeResolver } from "../resolver";
import { IndexMeta, unmarshalBinaryForIndexMeta } from "./index-meta";
import {PageFile} from "../btree/pagefile";



export class IndexFile {
	static async forUrl<T = any>(url: string) {
		return await IndexFile.forResolver<T>(
			async ({ start, end, expectedLength }) => {
				const response = await fetch(url, {
					headers: { Range: `bytes=${start}-${end}` },
				});
				const totalLength = Number(
					response.headers.get("Content-Range")!.split("/")[1]
				);
				if (expectedLength && totalLength !== expectedLength) {
					throw new LengthIntegrityError();
				}
				return {
					data: await response.arrayBuffer(),
					totalLength: totalLength,
				};
			}
		);
	}

	static async forResolver<T = any>(
		resolver: RangeResolver
	): Promise<VersionedIndexFile<T>> {
		return new IndexFileV1<T>(resolver);
	}
}

function decodeFloatingInt16(x: number) {
	const exponent = x >> 11;
	const mantissa = x & 0x7ff;
	return (1 << exponent) * mantissa + (1 << (exponent + 11)) - (1 << 11);
}

export type FileMeta = {
	version: number;
	format: number;
	readOffset: bigint;
};

export interface VersionedIndexFile<T> {
	tree(): Promise<LinkedMetaPage>;

	metadata(): Promise<FileMeta | null>;

	indexHeaders(): Promise<IndexMeta[]>;
}

class IndexFileV1<T> implements VersionedIndexFile<T> {
	private _tree?: LinkedMetaPage;

	constructor(private resolver: RangeResolver) {}

	async tree(): Promise<LinkedMetaPage> {
		if (this._tree) {
			return this._tree;
		}

		const pageFile = new PageFile(this.resolver);
		const tree = ReadMultiBPTree(this.resolver, pageFile);

		this._tree = tree;
		return tree;
	}

	async metadata(): Promise<FileMeta | null> {
		const tree = await this.tree();

		const buffer = await tree.metadata();

		// unmarshall binary for FileMeta
		if (buffer.byteLength < 9) {
			return null;
		}

		const dataView = new DataView(buffer);
		const version = dataView.getUint8(0);
		const format = dataView.getUint8(1);

		const readOffset = dataView.getBigUint64(2);

		return {
			version: version,
			format: format,
			readOffset: readOffset,
		};
	}

	async indexHeaders(): Promise<IndexMeta[]> {
		let headers: IndexMeta[] = [];

		let mp = await this.tree();

		while (mp) {
			const next = await mp.next();
			if (next === null) {
				return headers;
			}

			const nextBuffer = await next?.metadata();
			const indexMeta = await unmarshalBinaryForIndexMeta(this.resolver, nextBuffer);

			headers.push(indexMeta);

			mp = next;
		}

		return headers;
	}
}
