import { buildTrigram } from "../db/search";

describe("builds trigrams", () => {
  it("builds a basic trigram", () => {
    const phrase = "wakemeup";
    const expected = [
      {
        chunk: "wak",
        offset: 0,
      },
      {
        chunk: "ake",
        offset: 1,
      },
      {
        chunk: "kem",
        offset: 2,
      },
      {
        chunk: "eme",
        offset: 3,
      },
      {
        chunk: "meu",
        offset: 4,
      },
      {
        chunk: "eup",
        offset: 5,
      },
    ];

    const trigrams = buildTrigram(phrase);
    expect(trigrams).toEqual(expected);
  });

  it("builds a complex trigram", () => {
    const phrase = "I can't wake up";
    const expected = [
      {
        chunk: "can",
        offset: 2,
      },
      {
        chunk: "ant",
        offset: 3,
      },
      {
        chunk: "wak",
        offset: 8,
      },
      {
        chunk: "ake",
        offset: 9,
      },
    ];

    const trigrams = buildTrigram(phrase);
    expect(trigrams).toEqual(expected);
  });
});
