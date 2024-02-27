import path from "path";
import fs from "fs/promises";

export async function readBinaryFile(filename: string): Promise<Uint8Array> {
  const filePath = path.join(__dirname, `mock_binaries/${filename}`);
  const data = await fs.readFile(filePath);
  return new Uint8Array(data);
}

export function arrayBufferToString(arrayBuffer: ArrayBuffer): string {
  const decoder = new TextDecoder("utf-8");
  const uint8Array = new Uint8Array(arrayBuffer);

  return decoder.decode(uint8Array);
}
