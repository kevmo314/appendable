import { LengthIntegrityError, RangeResolver } from "./resolver";

export class DataFile {
  private constructor(
    private resolver: (start: number, end: number) => Promise<ArrayBuffer>
  ) {}

  static forUrl(url: string) {
    return DataFile.forResolver(async ({ start, end }) => {
      const response = await fetch(url, {
        headers: { Range: `bytes=${start}-${end}` },
      });
      const totalLength = Number(
        response.headers.get("Content-Range")!.split("/")[1]
      );
      return {
        data: await response.arrayBuffer(),
        totalLength: totalLength,
      };
    });
  }

  static forResolver(resolver: RangeResolver) {
    return new DataFile(async (start, end) => {
      return (
        await resolver({
          start,
          end,
        })
      ).data;
    });
  }

  async get(startByteOffset: number, endByteOffset: number) {
    const data = await this.resolver(startByteOffset, endByteOffset);
    return new TextDecoder().decode(data);
  }
}
