import { Config } from "../index";
import { requestRanges } from "../resolver/range-request";
import { RangeResolver } from "../resolver/resolver";

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
      return await resolver(ranges);
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
