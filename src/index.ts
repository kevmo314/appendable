import { DataFile } from "./data-file";
import { Database, FieldType, fieldTypeToString } from "./db/database";
import { IndexFile } from "./index-file/index-file";
import { RangeResolver } from "./resolver";

export type Config = {
  useMultipartByteRanges?: boolean;
};

export async function init(
  dataUrl: string | RangeResolver,
  indexUrl: string | RangeResolver,
  config?: Config,
) {
  if (!config) {
    config = { useMultipartByteRanges: true };
  }

  return Database.forDataFileAndIndexFile(
    typeof dataUrl === "string"
      ? DataFile.forUrl(dataUrl, config)
      : DataFile.forResolver(dataUrl),
    typeof indexUrl === "string"
      ? await IndexFile.forUrl(indexUrl, config)
      : await IndexFile.forResolver(indexUrl),
  );
}

interface GlobalMap {
  Appendable: {
    init: Function;
    FieldType: typeof FieldType;
    fieldTypeToString: Function;
  };
}

declare global {
  var Appendable: GlobalMap["Appendable"];
}

globalThis.Appendable = {
  init,
  FieldType,
  fieldTypeToString,
};
