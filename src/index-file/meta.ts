export enum FileFormat {
  JSONL = 0,
  CSV = 1,
}

export type FileMeta = {
  version: number;
  format: FileFormat;
  readOffset: bigint;
};

export async function readFileMeta(buffer: ArrayBuffer): Promise<FileMeta> {
  if (buffer.byteLength !== 10) {
    throw new Error(
      `incorrect byte length! Want: 10, got ${buffer.byteLength}`,
    );
  }

  const dataView = new DataView(buffer);

  const version = dataView.getUint8(0);
  const formatByte = dataView.getUint8(1);

  if (Object.values(FileFormat).indexOf(formatByte) === -1) {
    throw new Error(`unexpected file format. Got: ${formatByte}`);
  }

  const readOffset = dataView.getBigUint64(2, true);

  return {
    version,
    format: formatByte,
    readOffset,
  };
}

export type IndexMeta = {
  fieldName: string;
  fieldType: number;
  width: number;
};

export type IndexHeader = {
  fieldName: string;
  fieldTypes: number[];
};

export async function readIndexMeta(buffer: ArrayBuffer): Promise<IndexMeta> {
  if (buffer.byteLength < 6) {
    throw new Error(`invalid metadata size ${buffer.byteLength}`);
  }

  const dataView = new DataView(buffer);
  const fieldType = dataView.getUint16(0, true);
  const width = dataView.getUint16(2, true);
  const nameLength = dataView.getUint16(4, true);

  if (buffer.byteLength < 6 + nameLength) {
    throw new Error(`invalid metadata size ${buffer.byteLength}`);
  }

  const fieldNameBuffer = buffer.slice(6, 6 + nameLength);
  const fieldName = new TextDecoder("utf-8").decode(fieldNameBuffer);

  return {
    fieldName,
    fieldType,
    width,
  };
}

export function collectIndexMetas(indexMetas: IndexMeta[]): IndexHeader[] {
  const headersMap: Map<string, number[]> = new Map();

  for (const meta of indexMetas) {
    if (!headersMap.has(meta.fieldName)) {
      headersMap.set(meta.fieldName, [meta.fieldType]);
    } else {
      const updatedTypes = headersMap.get(meta.fieldName);
      updatedTypes?.push(meta.fieldType);
      headersMap.set(meta.fieldName, updatedTypes!!);
    }
  }

  const indexHeaders: IndexHeader[] = [];
  headersMap.forEach((fieldTypes, fieldName) => {
    indexHeaders.push({ fieldName, fieldTypes });
  });

  return indexHeaders;
}
