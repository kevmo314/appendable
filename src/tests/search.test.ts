import { buildTrigram, shuffle } from "../db/search";

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

describe("fuzz shuffle", () => {
  const generateRandomString = (length: number) => {
    const alpha =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 ";
    let result = "";
    for (let i = 0; i < length; i++) {
      result += alpha.charAt(Math.floor(Math.random() * alpha.length));
    }
    return result;
  };

  it("shuffles randomly", () => {
    for (let i = 0; i < 100; i++) {
      const phrase = generateRandomString(Math.floor(Math.random() * 50));
      const trigrams = buildTrigram(phrase);
      const shuffled = shuffle(trigrams);

      expect(shuffled.length).toBe(trigrams.length);
      expect(new Set(shuffled)).toEqual(new Set(trigrams));
    }
  });
});
