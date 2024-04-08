type Entry = { key: string; count: number; _score: number };
export class NgramTable {
  private visited: Map<string, number>;
  private topValue: Entry = { key: "", count: -1, _score: 0.0 };

  constructor() {
    this.visited = new Map();
  }

  insert(key: string) {
    const count = (this.visited.get(key) || 0) + 1;

    if (this.topValue.count < count) {
      this.topValue = { key, count, _score: 0.0 };
    }

    this.visited.set(key, count);
  }

  get(): string | null {
    const { key, count } = this.topValue;

    if (count < 0) {
      return null;
    }

    this.visited.delete(key);

    this.topValue = { key: "", count: -1, _score: 0.0 };
    for (const [key, count] of this.visited.entries()) {
      if (count > this.topValue.count) {
        this.topValue = { key, count, _score: 0.0 };
      }
    }

    return key;
  }

  clear() {
    this.visited = new Map();
    this.topValue = { key: "", count: -1, _score: 0.0 };
  }
}
