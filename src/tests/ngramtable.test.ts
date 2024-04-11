import { PriorityTable } from "../ngram/table";

describe("tests ngram table", () => {
  it("correctly tracks the count", () => {
    const table = new PriorityTable<string>();
    table.insert("howdy", 3);
    table.insert("do", 3);
    table.insert("howdy", 2);

    const pq = table.iter();
    const value = pq.next().value;
    expect(value).toEqual({ key: "howdy", score: 5 });
  });

  it("should return null for top", () => {
    const table = new PriorityTable<string>();
    const pq = table.iter();
    expect(pq.next().done).toBeTruthy();
  });

  it("should correctly clear all entries", () => {
    const table = new PriorityTable<string>();
    table.insert("wef", 4);
    table.insert("wef", 3);
    table.insert("wef", 2);
    table.insert("ty", 1);
    expect(table.size).toEqual(2);
    table.clear();

    const pq = table.iter();
    expect(pq.next().done).toBeTruthy();
    expect(table.size).toEqual(0);
  });

  it("handles a large number of varied inserts", () => {
    const table = new PriorityTable<string>();
    const entries = new Map<string, number>();
    const itemCount = 1000;
    const possibleEntries = ["wef", "wef a", "beef", "tarikoplata", "omoplata"];

    for (let idx = 0; idx < itemCount; idx++) {
      const randomKey =
        possibleEntries[Math.floor(Math.random() * possibleEntries.length)];
      table.insert(randomKey, idx);
      entries.set(randomKey, (entries.get(randomKey) || 0) + idx);
    }

    const sorted = Array.from(entries).sort((m, n) => n[1] - m[1]);
    let queue = table.iter();

    let idx = 0;

    let next = queue.next();
    while (!next.done) {
      const { key } = next.value;

      expect(key).toEqual(sorted[idx][0]);
      idx++;

      next = queue.next();
    }
  });
});
