import { Config } from ".";
import parseMultipartBody from "./multipart";
import { LengthIntegrityError } from "./resolver";

async function resolveIndividualPromises(
  url: string,
  ranges: { start: number; end: number; expectedLength?: number }[],
) {
  console.log("resolving ranges individually");
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
  return await Promise.all(individualRangePromises);
}

export async function requestRanges(
  url: string,
  ranges: { start: number; end: number; expectedLength?: number }[],
  config: Config,
): Promise<{ data: ArrayBuffer; totalLength: number }[]> {
  const { useMultipartByteRanges } = config;
  if (
    useMultipartByteRanges === false ||
    useMultipartByteRanges === undefined
  ) {
    return await resolveIndividualPromises(url, ranges);
  }

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
      console.warn(
        `useMultipartByteRanges has not been set to false. The server can not handle multipart byte ranges.`,
      );
      return await resolveIndividualPromises(url, ranges);
    case 206:
      const contentType = response.headers.get("Content-Type");
      if (!contentType) {
        throw new Error("Missing Content-Type in response");
      }
      if (contentType.includes("multipart/byteranges")) {
        let chunks = [];

        if (!response.body) {
          throw new Error(`response body is null: ${response.body}`);
        }

        for await (const chunk of parseMultipartBody(
          contentType,
          response.body,
        )) {
          chunks.push(chunk);
        }

        // the last element is null since the final boundary marker is followed by another delim.
        if (chunks[chunks.length - 1].data === undefined) {
          chunks.pop();
        }

        return chunks.map(({ data, headers }) => {
          const totalLengthStr = headers["content-range"]?.split("/")[1];
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
    default:
      throw new Error(`Expected 206 or 200 response, got ${response.status}`);
  }
}
