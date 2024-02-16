import { LinkedMetaPage, ReadMultiBPTree } from "../btree/multi";
import { LengthIntegrityError, RangeResolver } from "../resolver";
import { PageFile } from "../btree/pagefile";
import { FileFormat, IndexHeader, IndexMeta, collectIndexMetas, readIndexMeta } from "./meta";

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

	indexHeaders(): Promise<IndexHeader[]>;
}

export class IndexFileV1<T> implements VersionedIndexFile<T> {
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

	async metadata(): Promise<FileMeta> {
		const tree = await this.tree();

		const buffer = await tree.metadata();

		// unmarshall binary for FileMeta
		if (buffer.byteLength < 9) {
			throw new Error(`incorrect byte length! Want: 10, got ${buffer.byteLength}`);
		}

		const dataView = new DataView(buffer);
		const version = dataView.getUint8(0);
		const formatByte = dataView.getUint8(1);

		let format;
		switch (formatByte) {
			case FileFormat.CSV:
				format = FileFormat.CSV
				break
			case FileFormat.JSONL:
				format = FileFormat.JSONL
				break
			default:
				throw new Error(`invalid format: ${formatByte}`)
		}

		if (format !== FileFormat.CSV && format !== FileFormat.JSONL) {
			throw new Error(`unexpected file format. Got: ${format}`);
		}

		const readOffset = dataView.getBigUint64(2);

		return {
			version: version,
			format: format,
			readOffset: readOffset,
		};
	}

	async indexHeaders(): Promise<IndexHeader[]> {
		let headers: IndexMeta[] = [];

		let mp = await this.tree();

		while (mp) {
			const next = await mp.next();
			if (next === null) {
				return collectIndexMetas(headers);
			}

			const nextBuffer = await next?.metadata();
			const indexMeta = await readIndexMeta(nextBuffer);

			headers.push(indexMeta);

			mp = next;
		}

		return collectIndexMetas(headers);
	}
}
