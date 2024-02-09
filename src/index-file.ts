import { LinkedMetaPage } from "./btree/multi";
import { PageFile } from "./btree/pagefile";
import { LengthIntegrityError, RangeResolver } from "./resolver";

export type Header = {
	fieldName: string;
	fieldType: bigint;
	indexRecordCount: bigint;
};

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
}

class IndexFileV1<T> implements VersionedIndexFile<T> {
	private _tree?: LinkedMetaPage;

	constructor(private resolver: RangeResolver) {}

	async tree(): Promise<LinkedMetaPage> {
		if (this._tree) {
			return this._tree;
		}
		// read the linked meta page

		// we'll have to read the tree file: see multi.ts

	}

	async metadata(): Promise<FileMeta | null> {
		const tree = await this.tree();

		const buffer = await tree.metadata();

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
}

/*
export interface VersionedIndexFile<T> {
	indexFileHeader(): Promise<{
		indexLength: number;
		dataCount: number;
	}>;
	indexHeaders(): Promise<Header[]>;
	indexRecord(
		field: keyof T,
		offset: number
	): Promise<{
		dataNumber: number;
		fieldStartByteOffset: number;
		fieldLength: number;
	}>;
	dataRecord(
		offset: number
	): Promise<{ startByteOffset: number; endByteOffset: number }>;
}

class IndexFileV1<T> implements VersionedIndexFile<T> {
	private _indexFileHeader?: {
		indexLength: number;
		dataCount: number;
	};
	private _indexHeaders?: Header[];

	private static INDEX_RECORD_SIZE = 18;

	constructor(
		private resolver: (start: number, end: number) => Promise<ArrayBuffer>
	) {}

	async indexFileHeader() {
		if (this._indexFileHeader) {
			return this._indexFileHeader;
		}
		const header = new DataView(await this.resolver(1, 16));
		this._indexFileHeader = {
			indexLength: Number(header.getBigUint64(0)),
			dataCount: Number(header.getBigUint64(8)),
		};
		return this._indexFileHeader;
	}

	async indexHeaders() {
		if (this._indexHeaders) {
			return this._indexHeaders;
		}

		const indexFileHeader = await this.indexFileHeader();
		const buffer = await this.resolver(17, indexFileHeader.indexLength + 16);
		const data = new DataView(buffer);
		const headers: Header[] = [];
		let offset = 0;
		while (offset < indexFileHeader.indexLength) {
			const fieldNameLength = data.getUint32(offset);
			offset += 4;
			const fieldName = new TextDecoder("utf-8").decode(
				buffer.slice(offset, offset + fieldNameLength)
			);
			offset += fieldNameLength;
			const fieldType = data.getBigUint64(offset);
			offset += 8;
			const indexRecordCount = data.getBigUint64(offset);
			offset += 8;
			headers.push({
				fieldName,
				fieldType,
				indexRecordCount,
			});
		}
		if (offset !== indexFileHeader.indexLength) {
			throw new Error(
				`Inaccurate header read, offset = ${offset} but indexFileHeader.indexLength = ${indexFileHeader.indexLength}. This could indicate that the index file is corrupt.`
			);
		}
		this._indexHeaders = headers;
		return headers;
	}

	async indexRecord(field: keyof T, offset: number) {
		if (offset < 0) {
			throw new Error("offset out of range");
		}
		const headers = await this.indexHeaders();
		const headerIndex = headers.findIndex(
			(header) => header.fieldName === field
		);
		if (headerIndex === -1) {
			throw new Error("field not found");
		}
		const header = headers[headerIndex];
		if (offset >= Number(header.indexRecordCount)) {
			throw new Error("offset out of range");
		}

		const indexFileHeader = await this.indexFileHeader();
		const indexRecordsStart = 17 + indexFileHeader.indexLength;
		const headerOffset = headers.slice(0, headerIndex).reduce((acc, header) => {
			return (
				acc + Number(header.indexRecordCount) * IndexFileV1.INDEX_RECORD_SIZE
			);
		}, 0);
		const recordOffset =
			indexRecordsStart + headerOffset + offset * IndexFileV1.INDEX_RECORD_SIZE;
		const buffer = await this.resolver(
			recordOffset,
			recordOffset + IndexFileV1.INDEX_RECORD_SIZE
		);
		const data = new DataView(buffer);

		const dataNumber = data.getBigUint64(0);
		const fieldStartByteOffset = data.getBigUint64(8);
		const fieldLength = decodeFloatingInt16(data.getUint16(16));

		return {
			dataNumber: Number(dataNumber),
			fieldStartByteOffset: Number(fieldStartByteOffset),
			fieldLength: fieldLength,
		};
	}

	async dataRecord(offset: number) {
		if (offset < 0) {
			throw new Error("offset out of range");
		}
		const indexFileHeader = await this.indexFileHeader();
		if (offset >= indexFileHeader.dataCount) {
			throw new Error("offset out of range");
		}
		const headers = await this.indexHeaders();
		const indexRecordsLength = headers.reduce((acc, header) => {
			return (
				acc + Number(header.indexRecordCount) * IndexFileV1.INDEX_RECORD_SIZE
			);
		}, 0);
		const start = 17 + indexFileHeader.indexLength + indexRecordsLength;
		// fetch the byte offsets. if offset is 0, we can just fetch the first 8 bytes.
		if (offset === 0) {
			const buffer = await this.resolver(
				start + offset * 8,
				start + offset * 8 + 8
			);
			const data = new DataView(buffer);
			const endByteOffset = data.getBigUint64(0);
			return {
				startByteOffset: 0,
				endByteOffset: Number(endByteOffset),
			};
		}

		const buffer = await this.resolver(
			start + (offset - 1) * 8,
			start + offset * 8 + 8
		);
		const data = new DataView(buffer);
		const startByteOffset = data.getBigUint64(0);
		const endByteOffset = data.getBigUint64(8);
		return {
			startByteOffset: Number(startByteOffset),
			endByteOffset: Number(endByteOffset),
		};
	}
}
*/
