import { NgramName, Tokenizer } from "./tokenizer";

describe("builds trigrams", () => {
  let tok: Tokenizer;

  beforeEach(() => {
    tok = new Tokenizer();
  });

  it("builds a basic trigram", () => {
    const phrase = "wakemeup";
    const expected = {
      name: NgramName.Trigram,
      tokens: ["wak", "ake", "kem", "eme", "meu", "eup"],
    };

    const trigrams = tok.buildTokens(phrase);
    expect(trigrams).toEqual(expected);
  });

  it("builds a complex trigram", () => {
    const phrase = "I can't wake up";
    const expected = ["can", "ant", "wak", "ake"];

    const trigrams = tok.buildTokens(phrase);
    expect(trigrams).toEqual(expected);
  });
});

describe("fuzz shuffle", () => {
  let tok: Tokenizer;

  beforeEach(() => {
    tok = new Tokenizer();
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
      const tokens = tok.buildTokens(phrase);
      const shuffled = tok.shuffle(tokens);

      expect(shuffled.length).toBe(tokens.length);
      expect(new Set(shuffled)).toEqual(new Set(tokens));
    }
  });
});
