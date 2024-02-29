import { Chunk } from "./range-request";

function getReader(stream: ReadableStream) {
  let residual: Uint8Array | null = null;
  let readDone = false;
  let reader:
    | ReadableStreamDefaultReader<Uint8Array>
    | ReadableStreamBYOBReader;
  try {
    reader = stream.getReader({ mode: "byob" });
  } catch (e) {
    reader = stream.getReader();
  }
  return async (
    buf: Uint8Array,
  ): Promise<ReadableStreamReadResult<Uint8Array>> => {
    if (reader instanceof ReadableStreamBYOBReader) {
      return await reader.read(buf);
    } else {
      while (true) {
        if (residual) {
          const n = Math.min(residual.length, buf.length);
          buf.set(residual.subarray(0, n));
          residual = residual.subarray(n);
          if (residual.length === 0) {
            residual = null;
          }
          return {
            done: readDone && residual === null,
            value: buf.subarray(0, n),
          };
        }
        const result = await reader.read();
        if (result.value) {
          residual = result.value;
        }
        readDone ||= result.done;
      }
    }
  };
}

function parseContentRangeHeader(
  header: string,
): [string, number, number, number] {
  // parse bytes a-b/c
  const tokens = header.split(" ");
  if (tokens.length !== 2) {
    throw new Error("Invalid Content-Range header");
  }
  const [range, total] = tokens[1].split("/");
  const [start, end] = range.split("-");
  return [tokens[0], Number(start), Number(end), Number(total)];
}

export default async function* parseMultipartBody(
  contentType: string,
  stream: ReadableStream,
) {
  const reader = getReader(stream);
  const tokens = contentType.split(";");
  if (tokens[0] !== "multipart/byteranges") {
    throw new Error("Not a multipart/byteranges body");
  }
  const boundaryToken = tokens
    .map((s) => s.trim())
    .find((s) => s.startsWith("boundary="))
    ?.split("=", 2)?.[1];
  if (!boundaryToken) {
    throw new Error("No boundary found");
  }
  const boundary = `--${boundaryToken}`;
  console.log("this is boundary", boundary);

  let headers: Record<string, string> = {};

  const buf = new Uint8Array(4096);
  let ptr = 0;
  let length = 0;

  const extend = async () => {
    if (length === buf.byteLength) {
      throw new Error("no buffer space left");
    }
    const { done, value } = await reader(
      ptr + length >= buf.length
        ? buf.subarray((ptr + length) % buf.length, ptr)
        : buf.subarray(ptr + length, buf.length),
    );
    if (done) {
      return done;
    }
    length += value.length;
    return false;
  };

  while (true) {
    // read boundary
    for (let i = 0; i < boundary.length; i++) {
      while (length === 0) {
        if (await extend()) {
          return;
        }
      }
      if (buf[ptr] !== boundary.charCodeAt(i)) {
        console.log("boundary.charCode", boundary.charCodeAt(i));
        throw new Error("Invalid boundary");
      }
      ptr = (ptr + 1) % buf.length;
      length--;
    }

    // read the boundary terminator
    for (const c of ["\r", "\n"]) {
      while (length === 0) {
        if (await extend()) {
          return;
        }
      }
      if (buf[ptr] === c.charCodeAt(0)) {
        ptr = (ptr + 1) % buf.length;
        length--;
      } else if (buf[ptr] === "-".charCodeAt(0)) {
        // eof
        return;
      } else {
        // invalid boundary
        throw new Error("Invalid boundary");
      }
    }

    // read headers
    let lastByte = 0;
    let header: number[] = [];
    while (true) {
      while (length === 0) {
        if (await extend()) {
          return;
        }
      }
      const byte = buf[ptr];
      ptr = (ptr + 1) % buf.length;
      length--;
      if (lastByte === "\r".charCodeAt(0) && byte === "\n".charCodeAt(0)) {
        // end of header
        if (header.length === 1 /* it's an \r */) {
          // end of headers
          break;
        } else {
          const decoded = new TextDecoder().decode(new Uint8Array(header));
          const tokens = decoded.split(":", 2);
          if (tokens.length !== 2) {
            throw new Error(`Invalid header: ${decoded}`);
          }
          const [key, value] = tokens;
          headers[key.trim()] = value.trim();
          header.length = 0;
        }
      } else {
        header.push(byte);
      }
      lastByte = byte;
    }

    // read body
    // read the Content-Range header
    if (!headers["Content-Range"]) {
      // TODO: read until the next boundary
      throw new Error("Missing Content-Range header");
    }
    const [unit, start, end] = parseContentRangeHeader(
      headers["Content-Range"],
    );
    if (unit !== "bytes") {
      throw new Error("Invalid Content-Range header");
    }
    const contentLength = end - start + 1;
    const data = new Uint8Array(contentLength);
    for (let i = 0; i < contentLength; i++) {
      while (length === 0) {
        if (await extend()) {
          return;
        }
      }
      data[i] = buf[ptr];
      ptr = (ptr + 1) % buf.length;
      length--;
    }
    yield { data, headers };
    headers = {};

    // read the trailing \r\n
    for (const c of ["\r", "\n"]) {
      while (length === 0) {
        if (await extend()) {
          return;
        }
      }
      if (buf[ptr] === c.charCodeAt(0)) {
        ptr = (ptr + 1) % buf.length;
        length--;
      } else {
        // invalid boundary
        throw new Error("Invalid boundary");
      }
    }
  }
}
