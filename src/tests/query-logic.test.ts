import { FieldType } from "../db/database";
import { handleSelect, processWhere } from "../db/query-logic";


describe("query logic test", () => {

  it("should process the given key", () => {
    let floatBuf1 = new ArrayBuffer(8);
    new DataView(floatBuf1).setFloat64(0, 3.4, true)

    let floatBuf2 = new ArrayBuffer(8);
    new DataView(floatBuf2).setFloat64(0, Number(1n), true)

    const values: [string | number | bigint | boolean | null, FieldType, ArrayBuffer][] = [
      ["howdy", FieldType.String, new TextEncoder().encode("howdy").buffer],
      [3.4, FieldType.Float64, floatBuf1],
      [1n, FieldType.Float64, floatBuf2],
      [true, FieldType.Boolean, new Uint8Array([0]).buffer],
      [false, FieldType.Boolean, new Uint8Array([1]).buffer],
      [null, FieldType.Null, new ArrayBuffer(0)]
    ];

    for (const [value, expectedType, expectedVBuf] of values) {
      // @ts-ignore
      const res = processWhere(value);

      if (!res) {
        expect(res).not.toBeNull();
        return;
      }

      const { valueBuf, fieldType } = res;
      expect(expectedType).toEqual(fieldType)
      expect(valueBuf).toEqual(expectedVBuf)
    }
  })


  it("should select accordingly", () => {
    const select = ["george strait", "alan jackson"]

    const mockJson = {
      "george strait": "howdy",
      "alan jackson": true,
      "kelp": null,
      "wef": 30.4
    }

    const mockJsonStr = JSON.stringify(mockJson)
    const filtered = handleSelect(mockJsonStr, select);
    expect(filtered).toEqual({
      "george strait": "howdy",
      "alan jackson": true,
    })

    const pass = handleSelect(mockJsonStr);
    expect(pass).toEqual(mockJson)
  })

})
