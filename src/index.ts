import { DataFile } from "./data-file";
import { Database, FieldType, containsType } from "./database";
import { IndexFile } from "./index-file";
import { RangeResolver } from "./resolver";

export async function init(
	dataUrl: string | RangeResolver,
	indexUrl: string | RangeResolver
) {
	return Database.forDataFileAndIndexFile(
		typeof dataUrl === "string"
			? DataFile.forUrl(dataUrl)
			: DataFile.forResolver(dataUrl),
		typeof indexUrl === "string"
			? await IndexFile.forUrl(indexUrl)
			: await IndexFile.forResolver(indexUrl)
	);
}

interface GlobalMap {
	Appendable: {
		init: Function;
		FieldType: typeof FieldType;
		containsType: typeof containsType;
	};
}

declare global {
	var Appendable: GlobalMap["Appendable"];
}

globalThis.Appendable = {
	init,
	FieldType,
	containsType,
};
