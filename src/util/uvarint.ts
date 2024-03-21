export type UvarintResponse = {
  value: number;
  bytesRead: number;
};

const MAX_VARINT_64 = 10;

export function encodeUvarint(n: number): ArrayBuffer {
  let i = 0;

  let ibuf = new Uint8Array(MAX_VARINT_64);

  while (n >= 0x80) {
    ibuf[i++] = (n & 0xff) | 0x80;
    n >>= 7;
  }

  ibuf[i] = n & 0xff;

  return ibuf.buffer.slice(0, i + 1);
}

export function decodeUvarint(buf: ArrayBuffer): UvarintResponse {
  let x: number = 0;
  let s: number = 0;

  const view = new Uint8Array(buf);

  for (let idx = 0; idx <= view.length - 1; idx++) {
    let b = view[idx];

    if (idx === MAX_VARINT_64) {
      return { value: 0, bytesRead: -(idx + 1) };
    }

    if (b < 0x80) {
      if (idx === MAX_VARINT_64 - 1 && b > 1) {
        return { value: 0, bytesRead: -(idx + 1) };
      }

      let value = x | (b << s);
      return { value, bytesRead: idx + 1 };
    }

    x |= (b & 0x7f) << s;
    s += 7;
  }

  return { value: 0, bytesRead: 0 };
}
