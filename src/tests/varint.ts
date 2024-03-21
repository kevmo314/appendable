import { decodeUvarint, encodeUvarint } from "../util/uvarint";

describe("test varint codec", () => {
  it("should round trip correctly", () => {
    let values = [
      0,
      1,
      2,
      10,
      20,
      63,
      64,
      65,
      127,
      128,
      129,
      255,
      256,
      257,
      1 << (63 - 1),
    ];

    values.forEach((v) => {
      const b = encodeUvarint(v);
      const w = decodeUvarint(b);
      expect(v).toEqual(w);
    });
  });
});
