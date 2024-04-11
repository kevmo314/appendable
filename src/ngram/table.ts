type Entry<K> = { key: K; score: number };

export class PriorityTable<K> {
  private map: Map<K, number> = new Map<K, number>();

  insert(key: K, score: number) {
    const prevScore = this.map.get(key) ?? 0;
    this.map.set(key, prevScore + score);
  }

  *iter(): IterableIterator<Entry<K>> {
    const sorted = Array.from(this.map.entries()).sort((m, n) => n[1] - m[1]);

    for (const [key, score] of sorted) {
      yield { key, score };
    }
  }
  get size(): number {
    return this.map.size;
  }

  clear(): void {
    this.map = new Map<K, number>();
  }
}
