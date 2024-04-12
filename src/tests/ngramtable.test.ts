import { PriorityTable } from "../ngram/table";

describe("tests ngram table", () => {
  it("correctly tracks the count", () => {
    const table = new PriorityTable<string>();
    table.insert("howdy", 3);
    table.insert("do", 3);
    table.insert("howdy", 2);

    const pq = table.top();
    expect(pq.length).toEqual(2);
    expect(pq[0]).toEqual({ key: "howdy", score: 5 });
    expect(pq[1]).toEqual({ key: "do", score: 3 });
  });

  it("should return null for top", () => {
    const table = new PriorityTable<string>();
    const pq = table.top();
    expect(pq.length).toEqual(0);
  });

  it("should correctly clear all entries", () => {
    const table = new PriorityTable<string>();
    table.insert("wef", 4);
    table.insert("wef", 3);
    table.insert("wef", 2);
    table.insert("ty", 1);
    expect(table.size).toEqual(2);
    table.clear();

    const pq = table.top();
    expect(pq.length).toEqual(0);
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

    const sorted = Array.from(entries, ([key, score]) => ({
      key,
      score,
    })).sort((m, n) => n.score - m.score);
    let queue = table.top();

    expect(sorted).toEqual(queue);
  });
});
