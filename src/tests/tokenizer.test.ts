import { NgramTable, NgramTokenizer } from "../search/tokenizer";

describe("builds trigrams", () => {
  let tok: NgramTokenizer;

  beforeEach(() => {
    tok = new NgramTokenizer(3, 3);
  });

  it("builds a basic trigram", () => {
    const phrase = "wakemeup";
    const expected = ["wak", "ake", "kem", "eme", "meu", "eup"];

    const trigrams = tok.tokens(phrase);
    expect(trigrams).toEqual(expected);
  });

  it("builds a complex trigram", () => {
    const phrase = "I can't wake up";
    const expected = ["can", "ant", "wak", "ake"];

    const trigrams = tok.tokens(phrase);
    expect(trigrams).toEqual(expected);
  });
});

describe("fuzz shuffle", () => {
  let tok: NgramTokenizer;

  beforeEach(() => {
    tok = new NgramTokenizer(3, 3);
  });
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
      const trigrams = tok.tokens(phrase);
      const shuffled = NgramTokenizer.shuffle(trigrams);

      expect(shuffled.length).toBe(trigrams.length);
      expect(new Set(shuffled)).toEqual(new Set(trigrams));
    }
  });
});
