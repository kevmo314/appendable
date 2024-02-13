import { RangeResolver } from "../resolver";

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
