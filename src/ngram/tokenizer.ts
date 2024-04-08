import { FieldType } from "../db/database";

export type NgramToken = {
  value: string;
  valueBuf: ArrayBuffer;
  type: FieldType;
};

export class NgramTokenizer {
  private readonly minGram: number;
  private readonly maxGram: number;

  private allGrams: Map<number, FieldType> = new Map<number, FieldType>([
    [1, FieldType.Unigram],
    [2, FieldType.Bigram],
    [3, FieldType.Trigram],
  ]);

  private static encoder: TextEncoder = new TextEncoder();

  constructor(minGram: number, maxGram: number) {
    this.maxGram = maxGram;
    this.minGram = minGram;
  }

  tokens(phrase: string): NgramToken[] {
    let ngrams: NgramToken[] = [];

    let wordOffsets: number[][] = [];
    let currentWordOffsets: number[] = [];

    Array.from(phrase).forEach((c, idx) => {
      if (/[a-zA-Z0-9]/.test(c)) {
        currentWordOffsets.push(idx);
      } else if (/\s/.test(c)) {
        if (currentWordOffsets.length >= this.minGram) {
          wordOffsets.push(currentWordOffsets);
        }
        currentWordOffsets = [];
      }
    });

    if (currentWordOffsets.length >= this.minGram) {
      wordOffsets.push(currentWordOffsets);
    }

    for (let N = this.minGram; N <= this.maxGram; N++) {
      const gType = this.allGrams.get(N);

      if (!gType) {
        throw new Error(`Unrecognized gram type for gram length: ${N}`);
      }

      wordOffsets.forEach((word) => {
        for (let idx = 0; idx <= word.length - N; idx++) {
          let str = "";

          for (let jdx = idx; jdx <= idx + N - 1; jdx++) {
            str += phrase[word[jdx]];
          }

          let value = str.toLowerCase();

          ngrams.push({
            value,
            valueBuf: NgramTokenizer.encoder.encode(value).buffer,
            type: gType,
          });
        }
      });
    }

    return ngrams;
  }

  static shuffle(tokens: NgramToken[]): NgramToken[] {
    // https://en.wikipedia.org/wiki/Fisher%E2%80%93Yates_shuffle
    let soup = [...tokens];

    for (let idx = tokens.length - 1; idx > 0; idx--) {
      const jdx = Math.floor(Math.random() * (idx + 1));
      [soup[idx], soup[jdx]] = [soup[jdx], soup[idx]];
    }

    return soup;
  }
}
