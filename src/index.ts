import { DataFile } from "./data-file";
import { Database, FieldType, containsType } from "./database";
import { IndexFile } from "./index-file";
import { RangeResolver } from "./resolver";

export enum FormatType {
	Csv = "csv",
	Jsonl = "jsonl",
}

export async function init(
	dataUrl: string | RangeResolver,
	indexUrl: string | RangeResolver,
	format: FormatType
) {
	return Database.forDataFileAndIndexFile(
		typeof dataUrl === "string"
			? DataFile.forUrl(dataUrl)
			: DataFile.forResolver(dataUrl),
		typeof indexUrl === "string"
			? await IndexFile.forUrl(indexUrl)
			: await IndexFile.forResolver(indexUrl),
		format
	);
}

interface GlobalMap {
	Appendable: {
		init: Function;
		FieldType: typeof FieldType;
		containsType: typeof containsType;
		FormatType: typeof FormatType;
	};
}

declare global {
	var Appendable: GlobalMap["Appendable"];
}

globalThis.Appendable = {
	init,
	FieldType,
	containsType,
	FormatType,
};
