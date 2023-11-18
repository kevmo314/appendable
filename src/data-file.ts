import { RangeResolver } from "./resolver";

export class DataFile {
  private constructor(resolver: RangeResolver) {}

  static forUrl(url: string) {
    return DataFile.forResolver(async (start: number, end: number) => {
      const response = await fetch(url, {
        headers: { Range: `bytes=${start}-${end}` },
      });
      return await response.arrayBuffer();
    });
  }

  static forResolver(resolver: RangeResolver) {
    return new DataFile(resolver);
  }
}
