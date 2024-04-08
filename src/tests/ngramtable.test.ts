import { NgramTable } from "../ngram/table";

describe("tests ngram table", () => {
  it("correctly tracks the count", () => {
    const table = new NgramTable<string>();
    table.insert("howdy");
    table.insert("do");
    table.insert("howdy");

    expect(table.top).toEqual({ key: "howdy", count: 2 });
  });

  it("should return null for top", () => {
    const table = new NgramTable<string>();
    expect(table.top).toBeNull();
  });

  it("should correctly clear all entries", () => {
    const table = new NgramTable<string>();
    table.insert("wef");
    table.insert("wef");
    table.insert("wef");
    table.insert("ty");
    expect(table.size).toEqual(2);
    table.clear();

    expect(table.top).toBeNull();
    expect(table.size).toEqual(0);
  });

  it("handles a large number of varied inserts", () => {
    const table = new NgramTable<string>();
    const entries = new Map<string, number>();
    const itemCount = 1000;
    const possibleEntries = ["wef", "wef a", "beef", "tarikoplata", "omoplata"];

    for (let idx = 0; idx < itemCount; idx++) {
      const randomKey =
        possibleEntries[Math.floor(Math.random() * possibleEntries.length)];
      table.insert(randomKey);
      entries.set(randomKey, (entries.get(randomKey) || 0) + 1);
    }

    while (table.size > 0) {
      let expectedTop = { key: "", count: 0 };
      for (const [key, count] of entries) {
        if (count > expectedTop.count) {
          expectedTop = { key, count };
        }
      }

      expect(table.top).toEqual(expectedTop);
      entries.delete(expectedTop.key);
    }
  });
});
