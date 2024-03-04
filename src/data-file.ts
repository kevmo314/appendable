import { Config } from ".";
import { requestRanges } from "./range-request";
import { RangeResolver } from "./resolver";

export class DataFile {
  private originalResolver?: RangeResolver;

  private constructor(private resolver: RangeResolver) {}

  static forUrl(url: string, config: Config) {
    return DataFile.forResolver(
      async (ranges) => await requestRanges(url, ranges, config),
    );
  }

  static forResolver(resolver: RangeResolver) {
    const instance = new DataFile(async (ranges) => {
      const result = await resolver(ranges);
      return result;
    });
    instance.originalResolver = resolver;
    return instance;
  }

  getResolver(): RangeResolver | undefined {
    return this.originalResolver;
  }

  async get(start: number, end: number) {
    const res = await this.resolver([{ start, end }]);
    return new TextDecoder().decode(res[0].data);
  }
}
