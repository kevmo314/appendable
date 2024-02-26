import { RangeResolver } from "./resolver";

interface Chunk {
  body: string;
  headers: { [key: string]: string };
}

export function parseMultipartBody(body: string, boundary: string): Chunk[] {
  return body
    .split(`--${boundary}`)
    .reduce((chunks: Chunk[], chunk: string) => {
      if (chunk && chunk !== "--") {
        const [head, body] = chunk.trim().split(/\r\n\r\n/g, 2);
        const headers = head
          .split(/\r\n/g)
          .reduce((headers: { [key: string]: string }, header: string) => {
            const [key, value] = header.split(/:\s+/);
            headers[key.toLowerCase()] = value;
            return headers;
          }, {});
        chunks.push({ body, headers });
      }
      return chunks;
    }, []);
}

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

      if (response.status !== 206) {
        throw new Error(`Expected 206 Partial Content, got ${response.status}`);
      }

      const contentType = response.headers.get("Content-Type");
      if (!contentType) {
        throw new Error("Missing Content-Type in response");
      }
      if (contentType.includes("multipart/byteranges")) {
        const boundary = contentType.split("boundary=")[1];
        const abuf = await response.arrayBuffer();
        const text = new TextDecoder().decode(abuf);

        const chunks = parseMultipartBody(text, boundary);

        // the last element is null since the final boundary marker is followed by another delim.
        if (chunks[chunks.length - 1].body === undefined) {
          chunks.pop();
        }
        console.log(chunks);
        return chunks.map((c) => {
          const enc = new TextEncoder();
          const data = enc.encode(c.body).buffer;

          const totalLengthStr = c.headers["content-range"]?.split("/")[1];
          const totalLength = totalLengthStr ? parseInt(totalLengthStr, 10) : 0;

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
