export enum NgramName {
  Default,
  Trigram,
}

interface NgramRange {
  min_gram: number;
  max_gram: number;
}

// we treat the first element as default

export class Tokenizer {
  private readonly ngramMap: Map<NgramName, NgramRange>;

  private allTokens: { name: NgramName; tokens: string[] }[] = [];
  private compositeScore: number = 0;
  private table: TokTable;

  constructor() {
    const ngramMap = new Map();
    ngramMap.set(NgramName.Default, { min_gram: 1, max_gram: 2 });
    ngramMap.set(NgramName.Trigram, { min_gram: 3, max_gram: 3 });

    this.ngramMap = ngramMap;
    this.table = new TokTable();
  }
  buildTokens(phrase: string): { name: NgramName; tokens: string[] }[] {
    const iterator = this.ngramMap.entries();

    let allTokens: { name: NgramName; tokens: string[] }[] = [];
    for (const [name, range] of iterator) {
      let tokens: string[] = [];
      let wordOffsets = [];
      let currentWordOffsets: number[] = [];

      Array.from(phrase).forEach((c, idx) => {
        if (/[a-zA-Z0-9]/.test(c)) {
          currentWordOffsets.push(idx);
        } else if (/\s/.test(c)) {
          if (currentWordOffsets.length >= range.min_gram) {
            wordOffsets.push(currentWordOffsets);
          }
          currentWordOffsets = [];
        }
      });

      if (currentWordOffsets.length >= range.min_gram) {
        wordOffsets.push(currentWordOffsets);
      }

      for (
        let curr_gram = range.min_gram;
        curr_gram <= range.max_gram;
        curr_gram++
      ) {
        wordOffsets.forEach((word) => {
          for (let idx = 0; idx <= word.length - curr_gram; idx++) {
            let val = "";

            for (let jdx = idx; jdx <= idx + curr_gram - 1; jdx++) {
              val += phrase[word[jdx]];
            }

            tokens.push(val);
          }
        });
      }

      allTokens.push({ name, tokens });
    }

    this.allTokens = allTokens;
    return this.allTokens;
  }

  shuffle<T>(tokens: T[]): T[] {
    let soup = [...tokens];

    for (let idx = tokens.length - 1; idx > 0; idx--) {
      const jdx = Math.floor(Math.random() * (idx + 1));
      [soup[idx], soup[jdx]] = [soup[jdx], soup[idx]];
    }

    return soup;
  }
}

type Entry = { key: string; count: number };
export class TokTable {
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
