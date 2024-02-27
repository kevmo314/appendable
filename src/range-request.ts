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
