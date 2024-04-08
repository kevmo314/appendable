export class NgramTokenizer {
  private readonly minGram: number;
  private readonly maxGram: number;

  constructor(minGram: number, maxGram: number) {
    this.maxGram = maxGram;
    this.minGram = minGram;
  }

  tokens(phrase: string): string[] {
    let ngrams: string[] = [];

    let wordOffsets = [];
    let currentWordOffsets: number[] = [];

    Array.from(phrase).forEach((c, idx) => {
      if (/[a-zA-Z]/.test(c) || /[0-9]/.test(c)) {
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

          ngrams.push(str);
        }
      });
    }

    return ngrams;
  }

  static shuffle(tokens: string[]): string[] {
    // https://en.wikipedia.org/wiki/Fisher%E2%80%93Yates_shuffle
    let soup = [...tokens];

    for (let idx = tokens.length - 1; idx > 0; idx--) {
      const jdx = Math.floor(Math.random() * (idx + 1));
      [soup[idx], soup[jdx]] = [soup[jdx], soup[idx]];
    }

    return soup;
  }
}

type Entry = { key: string; count: number };
export class NgramTable {
  private visited: Map<string, number>;
  private topValue: Entry = { key: "", count: -1 };

  constructor() {
    this.visited = new Map();
  }

  insert(key: string) {
    const count = (this.visited.get(key) || 0) + 1;

    if (this.topValue.count < count) {
      this.topValue = { key, count };
    }

    this.visited.set(key, count);
  }

  get(): string | null {
    const { key, count } = this.topValue;

    if (count < 0) {
      return null;
    }

    this.visited.delete(key);

    this.topValue = { key: "", count: -1 };
    for (const [key, count] of this.visited.entries()) {
      if (count > this.topValue.count) {
        this.topValue = { key, count };
      }
    }

    return key;
  }

  clear() {
    this.visited = new Map();
    this.topValue = { key: "", count: -1 };
  }
}
