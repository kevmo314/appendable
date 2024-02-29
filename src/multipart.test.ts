import parseMultipartBody from "./multipart";

async function collect<T>(gen: AsyncGenerator<T>) {
  const result: T[] = [];
  for await (const item of gen) {
    result.push(item);
  }
  return result;
}

describe("multipart", () => {
  it("should parse multipart with two chunks", async () => {
    const encoder = new TextEncoder();
    const data = encoder.encode(`--3d6b6a416f9b5\r
Content-Type: text/html\r
Content-Range: bytes 0-50/1270\r
\r
<!doctype html>
<html>
<head>
    <title>Example Do\r
--3d6b6a416f9b5\r
Content-Type: text/html\r
Content-Range: bytes 100-150/1270\r
\r
eta http-equiv="Content-type" content="text/html; c\r
--3d6b6a416f9b5--`);
    const { readable, writable } = new TransformStream();
    const writer = writable.getWriter();
    writer.write(data);
    writer.close();
    const multipart = await collect(
      parseMultipartBody(
        "multipart/byteranges; boundary=3d6b6a416f9b5",
        readable
      )
    );
    expect(multipart.length).toBe(2);
    const decoder = new TextDecoder();
    expect(decoder.decode(multipart[0].data)).toBe(
      "<!doctype html>\n<html>\n<head>\n    <title>Example Do"
    );
    expect(decoder.decode(multipart[1].data)).toBe(
      'eta http-equiv="Content-type" content="text/html; c'
    );
  });
});
