type Entry<T> = { key: T; count: number };
export class NgramTable<T> {
  private visited: Map<T, number>;
  private topValue: Entry<T> = { key: null as unknown as T, count: -1 };

  constructor() {
    this.visited = new Map<T, number>();
  }

  insert(key: T) {
    const count = (this.visited.get(key) || 0) + 1;

    if (this.topValue.count < count) {
      this.topValue = { key, count };
    }

    this.visited.set(key, count);
  }

  get top(): Entry<T> | null {
    const { key, count } = this.topValue;

    if (count < 0) {
      return null;
    }

    this.visited.delete(key);

    this.topValue = { key: null as unknown as T, count: -1 };
    for (const [key, count] of this.visited.entries()) {
      if (count > this.topValue.count) {
        this.topValue = { key, count };
      }
    }

    return {
      key,
      count,
    };
  }

  clear() {
    this.visited = new Map();
    this.topValue = { key: null as unknown as T, count: -1 };
  }

  get size(): number {
    return this.visited.size;
  }
}
