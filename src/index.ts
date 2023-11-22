import { DataFile } from "./data-file";
import { Database } from "./database";
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

globalThis.Appendable = {
  init,
};
