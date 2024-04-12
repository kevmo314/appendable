type Entry<K> = { key: K; score: number };

export class PriorityTable<K> {
  private map: Map<K, number> = new Map<K, number>();

  insert(key: K, score: number) {
    const prevScore = this.map.get(key) ?? 0;
    this.map.set(key, prevScore + score);
  }

  top(): Entry<K>[] {
    return Array.from(this.map, ([key, score]) => ({ key, score })).sort(
      (m, n) => n.score - m.score,
    );
  }
  get size(): number {
    return this.map.size;
  }

  clear(): void {
    this.map.clear();
  }
}
