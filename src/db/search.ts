export const N = 3;

type Trigram = string;

// https://en.wikipedia.org/wiki/Fisher%E2%80%93Yates_shuffle
export function shuffle(trigrams: Trigram[]): Trigram[] {
  let soup = [...trigrams];

  for (let idx = trigrams.length - 1; idx > 0; idx--) {
    const jdx = Math.floor(Math.random() * (idx + 1));
    [soup[idx], soup[jdx]] = [soup[jdx], soup[idx]];
  }

  return soup;
}

export function buildTrigram(phrase: string): Trigram[] {
  let trigrams: Trigram[] = [];

  let wordOffsets = [];
  let currentWordOffsets: number[] = [];

  Array.from(phrase).forEach((c, idx) => {
    if (/[a-zA-Z]/.test(c) || /[0-9]/.test(c)) {
      currentWordOffsets.push(idx);
    } else if (/\s/.test(c)) {
      if (currentWordOffsets.length >= N) {
        wordOffsets.push(currentWordOffsets);
      }
      currentWordOffsets = [];
    }
  });

  if (currentWordOffsets.length >= N) {
    wordOffsets.push(currentWordOffsets);
  }

  wordOffsets.forEach((word) => {
    for (let idx = 0; idx <= word.length - N; idx++) {
      let tri = "";

      for (let jdx = idx; jdx <= idx + N - 1; jdx++) {
        tri += phrase[word[jdx]];
      }

      trigrams.push(tri);
    }
  });

  return trigrams;
}

type Entry = { key: string; count: number };
export class TrigramTable {
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
