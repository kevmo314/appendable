import { NgramTable, NgramTokenizer } from "../search/tokenizer";
import { FieldType } from "../db/database";

describe("builds 12grams", () => {
  let tok: NgramTokenizer;
  let textEncoder: TextEncoder;

  beforeAll(() => {
    textEncoder = new TextEncoder();
  });

  beforeEach(() => {
    tok = new NgramTokenizer(1, 2);
  });

  it("builds a basic 12gram", () => {
    const phrase = "wakemeup";
    const expected = [
      "w",
      "a",
      "k",
      "e",
      "m",
      "e",
      "u",
      "p",
      "wa",
      "ak",
      "ke",
      "em",
      "me",
      "eu",
      "up",
    ].map((s) => ({
      value: s,
      valueBuf: textEncoder.encode(s).buffer,
      type: s.length === 1 ? FieldType.Unigram : FieldType.Bigram,
    }));

    const trigrams = tok.tokens(phrase);
    expect(trigrams).toEqual(expected);
  });

  it("builds a complex 12 gram", () => {
    const phrase = "I can't wake up";
    const expected = [
      "i",
      "c",
      "a",
      "n",
      "t",
      "w",
      "a",
      "k",
      "e",
      "u",
      "p",
      "ca",
      "an",
      "nt",
      "wa",
      "ak",
      "ke",
      "up",
    ].map((s) => ({
      value: s,
      valueBuf: textEncoder.encode(s).buffer,
      type: s.length === 1 ? FieldType.Unigram : FieldType.Bigram,
    }));

    const trigrams = tok.tokens(phrase);
    expect(trigrams).toEqual(expected);
  });
});

describe("builds trigrams", () => {
  let tok: NgramTokenizer;
  let textEncoder: TextEncoder;

  beforeAll(() => {
    textEncoder = new TextEncoder();
  });

  beforeEach(() => {
    tok = new NgramTokenizer(3, 3);
  });

  it("builds a basic trigram", () => {
    const phrase = "wakemeup";
    const expected = ["wak", "ake", "kem", "eme", "meu", "eup"].map((s) => ({
      value: s,
      valueBuf: textEncoder.encode(s).buffer,
      type: FieldType.Trigram,
    }));

    const trigrams = tok.tokens(phrase);
    expect(trigrams).toEqual(expected);
  });

  it("builds a complex trigram", () => {
    const phrase = "I can't wake up";
    const expected = ["can", "ant", "wak", "ake"].map((s) => ({
      value: s,
      valueBuf: textEncoder.encode(s).buffer,
      type: FieldType.Trigram,
    }));

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
