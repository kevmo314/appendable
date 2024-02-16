import { RangeResolver } from "../resolver";


export enum FileFormat {
	JSONL = 0,
	CSV = 1,
}

export type FileMeta = {
	version: number;
	format: FileFormat;
	readOffset: bigint;
}

export async function readFileMeta(buffer: ArrayBuffer): Promise<FileMeta> {
	if (buffer.byteLength !== 10) {
		throw new Error(`incorrect byte length! Want: 10, got ${buffer.byteLength}`);
	}

	const dataView = new DataView(buffer);

	const version = dataView.getUint8(0);
	const format = dataView.getUint8(1);

	if (format !== FileFormat.CSV && format !== FileFormat.JSONL) {
		throw new Error(`unexpected file format. Got: ${format}`)
	}

	const readOffset = dataView.getBigUint64(2);

	return {
		version,
		format,
		readOffset
	}
}



export type IndexMeta = {
	fieldName: string;
	fieldType: bigint;
};

export async function unmarshalBinaryForIndexMeta(
	resolver: RangeResolver,
	buffer: ArrayBuffer
): Promise<IndexMeta> {
	if (buffer.byteLength < 10) {
		throw new Error(`invalid metadata size ${buffer.byteLength}`);
	}

	const indexMeta = {
		fieldName: "",
		fieldType: BigInt(0),
	};

	const dataView = new DataView(buffer);

	indexMeta.fieldType = dataView.getBigUint64(0);

	const nameLength = dataView.getUint16(8);

	if (buffer.byteLength < 10 + nameLength) {
		throw new Error(`invalid metadata size: ${buffer.byteLength}`);
	}

	const { data: fieldNameData } = await resolver({
		start: 10,
		end: 10 + nameLength - 1,
	});

	indexMeta.fieldName = new TextDecoder("utf-8").decode(fieldNameData);

	return indexMeta;
}
