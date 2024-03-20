import { FieldType } from "./database";

export type Schema = {
  [key: string]: {};
};

export type WhereNode<T extends Schema, K extends keyof T = keyof T> = {
  operation: "<" | "<=" | "==" | ">=" | ">";
  key: keyof T;
  value: T[K];
};

export type OrderBy<T extends Schema> = {
  key: keyof T;
  direction: "ASC" | "DESC";
};

export type SelectField<T extends Schema> = keyof T;

export type Query<T extends Schema> = {
  where?: WhereNode<T>[];
  orderBy?: OrderBy<T>[];
  select?: SelectField<T>[];
  limit?: number;
};

type QueryWhere = {
  valueBuf: ArrayBuffer;
  fieldType: FieldType;
};

export function processWhere<T>(value: T[keyof T]): QueryWhere | null {
  let valueBuf: ArrayBuffer;

  if (value === null) {
    return {
      fieldType: FieldType.Null,
      valueBuf: new ArrayBuffer(0),
    };
  } else {
    switch (typeof value) {
      case "bigint":
      case "number":
        valueBuf = new ArrayBuffer(8);
        new DataView(valueBuf).setFloat64(0, Number(value));
        return {
          fieldType: FieldType.Float64,
          valueBuf,
        };
      case "boolean":
        return {
          fieldType: FieldType.Boolean,
          valueBuf: new Uint8Array([value ? 1 : 0]).buffer,
        };

      case "string":
        return {
          fieldType: FieldType.String,
          valueBuf: new TextEncoder().encode(value as string).buffer,
        };
    }
  }

  return null;
}

export function handleSelect<T>(data: string, select?: (keyof T)[]) {
  let jData = JSON.parse(data);
  if (select && select.length > 0) {
    return select.reduce(
      (acc, field) => {
        if (field in jData) {
          acc[field] = jData[field];
        }
        return acc;
      },
      {} as Pick<T, keyof T>,
    );
  }

  return jData;
}
