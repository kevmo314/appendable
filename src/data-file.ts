import { parseMultipartBodyTextEncoder } from "./range-request";
import { LengthIntegrityError, RangeResolver } from "./resolver";

export class DataFile {
  private originalResolver?: RangeResolver;

  private constructor(private resolver: RangeResolver) {}

  static forUrl(url: string) {
    return DataFile.forResolver(async (ranges) => {
      const rangesHeader = ranges
        .map(({ start, end }) => `${start}-${end}`)
        .join(",");

      const response = await fetch(url, {
        headers: {
          Range: `bytes=${rangesHeader}`,
          Accept: "multipart/bytesranges",
        },
      });
      switch (response.status) {
        case 200:
          // fallback to resolving ranges individually
          const individualRangePromises = ranges.map(
            async ({ start, end, expectedLength }) => {
              const rangeHeader = `${start}-${end}`;
              const res = await fetch(url, {
                headers: { Range: `bytes=${rangeHeader}` },
              });

              const totalLength = Number(
                res.headers.get("Content-Range")!.split("/")[1],
              );
              if (expectedLength && totalLength !== expectedLength) {
                throw new LengthIntegrityError();
              }
              return {
                data: await res.arrayBuffer(),
                totalLength: totalLength,
              };
            },
          );
          return Promise.all(individualRangePromises)
            .then((res) => {
              return res;
            })
            .catch((error) => {
              throw new Error(
                `error occured when fetching for individual range promises: ${error}`,
              );
            });
        case 206:
          const contentType = response.headers.get("Content-Type");
          if (!contentType) {
            throw new Error("Missing Content-Type in response");
          }
          if (contentType.includes("multipart/byteranges")) {
            const boundary = contentType.split("boundary=")[1];
            const text = await response.text();

            const chunks = parseMultipartBodyTextEncoder(text, boundary);

            // the last element is null since the final boundary marker is followed by another delim.
            if (chunks[chunks.length - 1].body === undefined) {
              chunks.pop();
            }

            const enc = new TextEncoder();
            return chunks.map((c) => {
              const data = enc.encode(c.body).buffer;

              const totalLengthStr = c.headers["content-range"]?.split("/")[1];
              const totalLength = totalLengthStr
                ? parseInt(totalLengthStr, 10)
                : 0;

              return { data, totalLength };
            });
          } else if (response.headers.has("Content-Range")) {
            const abuf = await response.arrayBuffer();
            const totalLength = Number(
              response.headers.get("Content-Range")!.split("/")[1],
            );
            return [
              {
                data: abuf,
                totalLength: totalLength,
              },
            ];
          } else {
            throw new Error(`Unexpected response format: ${contentType}`);
          }
        default:
          throw new Error(
            `Expected 206 or 200 response, got ${response.status}`,
          );
      }
    });
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
