import { LinkedMetaPage, ReadMultiBPTree } from "../btree/multi";
import { LengthIntegrityError, RangeResolver } from "../resolver";
import { PageFile } from "../btree/pagefile";
import {
	FileFormat,
	IndexHeader,
	IndexMeta,
	collectIndexMetas,
	readIndexMeta,
} from "./meta";
import { FieldType } from "../db/database";

export class IndexFile {
	static async forUrl<T = any>(url: string) {
		return await IndexFile.forResolver<T>(
			async ({ start, end, expectedLength }) => {
				console.log(start, end)
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
	getResolver(): RangeResolver;

	tree(): Promise<LinkedMetaPage>;

	metadata(): Promise<FileMeta>;

	indexHeaders(): Promise<IndexHeader[]>;

	seek(header: string, fieldType: FieldType): Promise<LinkedMetaPage[]>;
}

export class IndexFileV1<T> implements VersionedIndexFile<T> {
	private _tree?: LinkedMetaPage;

	constructor(private resolver: RangeResolver) {}

	getResolver(): RangeResolver {
		return this.resolver;
	}

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
		if (buffer.byteLength < 10) {
			throw new Error(
				`incorrect byte length! Want: 10, got ${buffer.byteLength}`
			);
		}

		const dataView = new DataView(buffer);
		const version = dataView.getUint8(0);
		const formatByte = dataView.getUint8(1);

		if (Object.values(FileFormat).indexOf(formatByte) === -1) {
			throw new Error(`unexpected file format. Got: ${formatByte}`);
		}

		const readOffset = dataView.getBigUint64(2);

		return {
			version: version,
			format: formatByte,
			readOffset: readOffset,
		};
	}

	async seek(header: string, fieldType: FieldType): Promise<LinkedMetaPage[]> {
		let mp = await this.tree();

		let headerMps = [];

		while (mp) {
			const next = await mp.next();
			if (next === null) {
				return headerMps;
			}

			const indexMeta = await readIndexMeta(await next.metadata());
			if (indexMeta.fieldName === header) {
				if (fieldType === FieldType.Float64) {
					// if key is a number or bigint, we cast it as a float64 type
					if (
						indexMeta.fieldType === FieldType.Float64 ||
						indexMeta.fieldType === FieldType.Int64 ||
						indexMeta.fieldType === FieldType.Uint64
					) {
						headerMps.push(next);
					}
				} else {
					if (indexMeta.fieldType === fieldType) {
						headerMps.push(next);
					}
				}
			}

			mp = next;
		}
		console.log(headerMps);

		return headerMps;
	}

	async indexHeaders(): Promise<IndexHeader[]> {
		let headers: IndexMeta[] = [];

		let mp = await this.tree();

		while (mp) {
			const next = await mp.next();
			if (next === null) {
				return collectIndexMetas(headers);
			}

			const nextBuffer = await next.metadata();
			const indexMeta = await readIndexMeta(nextBuffer);

			headers.push(indexMeta);

			mp = next;
		}

		return collectIndexMetas(headers);
	}
}
