import { FieldType } from "../db/database";

export type NgramToken = {
  value: string;
  encodedValue: ArrayBuffer;
  type: FieldType;
};

export class NgramTokenizer {
  private encoder: TextEncoder;
  private readonly minGram: number;
  private readonly maxGram: number;
  private readonly allGrams: Map<number, FieldType> = new Map([
    [1, FieldType.Unigram],
    [2, FieldType.Bigram],
    [3, FieldType.Trigram],
  ]);

  constructor(minGram: number, maxGram: number) {
    this.maxGram = maxGram;
    this.minGram = minGram;
    this.encoder = new TextEncoder();
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
      wordOffsets.forEach((word) => {
        for (let idx = 0; idx <= word.length - N; idx++) {
          let str = "";

          for (let jdx = idx; jdx <= idx + N - 1; jdx++) {
            str += phrase[word[jdx]];
          }

          ngrams.push({
            type: this.allGrams.get(N)!,
            value: str.toLowerCase(),
            encodedValue: this.encoder.encode(str.toLowerCase()).buffer,
          });
        }
      });
    }

    return ngrams;
  }

  get fieldTypes(): Set<FieldType> {
    let fts: Set<FieldType> = new Set();
    for (let idx = this.minGram; idx <= this.maxGram; idx++) {
      const fieldType = this.allGrams.get(idx);
      if (fieldType !== undefined) {
        fts.add(fieldType);
      }
    }
    return fts;
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
