function isNewline(buf: Uint8Array, i: number) {
  return (
    i + 1 < buf.length &&
    buf[i] === "\r".charCodeAt(0) &&
    buf[i + 1] === "\n".charCodeAt(0)
  );
}

export default async function* parseMultipartBody(
  contentType: string,
  stream: ReadableStream
) {
  const reader = stream.getReader({ mode: "byob" });
  if (!contentType.startsWith("multipart/")) {
    throw new Error("Not a multipart body");
  }
  const boundaryToken = contentType
    .split(";")
    .map((s) => s.trim())
    .find((s) => s.startsWith("boundary="))
    ?.split("=", 2)?.[1];
  if (!boundaryToken) {
    throw new Error("No boundary found");
  }
  const boundary = `--${boundaryToken}`;

  let state: "boundary" | "headers" | "body" = "boundary";

  const headers: Record<string, string> = {};

  const buf = new Uint8Array(4096);
  let pos = 0;

  while (true) {
    const { done, value } = await reader.read(buf.subarray(pos));
    if (done) {
      break;
    }
    const n = pos + value.byteLength;
    switch (state) {
      case "boundary": {
        // in boundary state, the first n bytes correspond to the boundary read so far.
        // validate that the bytes read are the boundary
        for (let i = pos; i < Math.min(n, boundary.length); i++) {
          if (buf[i] !== boundary.charCodeAt(i)) {
            throw new Error("Invalid boundary");
          }
        }
        pos += value.byteLength;
        if (pos >= boundary.length + 2) {
          // finished reading the boundary, erase it from the buffer and start reading the headers.
          if (isNewline(buf, pos - 2)) {
            state = "headers";
            buf.copyWithin(0, pos, n);
            pos = 0;
            continue;
          } else {
            // end of file
            return;
          }
        }
      }
      case "headers": {
        // in headers state, the first n bytes correspond to the headers read so far. read until an \r\n
        for (let i = Math.max(pos - 1, 0); i + 1 < n; i++) {
          if (isNewline(buf, i)) {
            if (isNewline(buf, i + 2)) {
              // end of headers
              state = "body";
              buf.copyWithin(0, pos, i + 4);
              pos = 0;
              break;
            }
            // emit this header
            const header = new TextDecoder().decode(buf.subarray(pos, i));
            const [key, value] = header.split(": ", 2);
            headers[key] = value;
            pos = i + 2;
          }
        }
        pos += value.byteLength;
      }
      case "body": {
        // read the Content-Length header
        const contentLength = Number(headers["Content-Length"]);
        if (contentLength === undefined) {
          throw new Error("Missing Content-Length header");
        }
        if (n - pos >= contentLength) {
          // emit the body
          yield { data: buf.subarray(pos, pos + contentLength), headers };
          pos += contentLength;
          state = "boundary";
          buf.copyWithin(0, pos, n);
          pos = 0;
        }
      }
    }
  }
}
