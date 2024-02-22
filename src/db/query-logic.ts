import { FieldType, OrderBy, Schema } from "./database";


type QueryWhere = {
  valueBuf: ArrayBuffer,
  fieldType: FieldType
}

export function processWhere<T>(value: T[keyof T]): QueryWhere | null {
  let valueBuf: ArrayBuffer;

  if (value === null) {
    return {
      fieldType: FieldType.Null,
      valueBuf: new ArrayBuffer(0)
    }
  } else {
    switch (typeof value) {
      case "bigint":
      case "number":
        valueBuf = new ArrayBuffer(8);
        new DataView(valueBuf).setFloat64(0, Number(value))
        return {
          fieldType: FieldType.Float64,
          valueBuf
        }
      case "boolean":
        return {
          fieldType: FieldType.Boolean,
          valueBuf: new Uint8Array([value ? 1 : 0]).buffer
        }

      case "string":
        return {
          fieldType: FieldType.String,
          valueBuf: new TextEncoder().encode(value as string).buffer
        }
    }
  }

  return null
}



export function handleSelect<T>(data: string, select?: (keyof T)[]) {
  let jData = JSON.parse(data)
  if (select && select.length > 0) {
    return select.reduce(
      (acc, field) => {
        if (field in jData) {
          acc[field] = jData[field];
        }
        return acc;
      },
      {} as Pick<T, keyof T>
    );
  }

  return jData
}
