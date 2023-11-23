import { RangeResolver } from "./resolver";

export class DataFile {
  private constructor(private resolver: RangeResolver) {}

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

  async get(startByteOffset: number, endByteOffset: number) {
    const data = await this.resolver(startByteOffset, endByteOffset);
    return JSON.parse(new TextDecoder().decode(data));
  }
}
