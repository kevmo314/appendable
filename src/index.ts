import { DataFile } from "./data-file";
import { IndexFile } from "./index-file";

export function init(
  dataUrl: string | Resolver.RangeResolver,
  indexUrl: string | Resolver.RangeResolver
) {
  return Database.forDataFileAndIndexFile(
    typeof dataUrl === "string"
      ? DataFile.forUrl(dataUrl)
      : DataFile.forResolver(dataUrl),
    typeof dataUrl === "string"
      ? IndexFile.forUrl(dataUrl)
      : IndexFile.forResolver(dataUrl)
  );
}
