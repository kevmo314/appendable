import { Database, Query } from "../db/database";
import { QueryBuilder } from "../db/query-builder";
import { validateQuery } from "../db/query-validation";
import { IndexHeader } from "../index-file/meta";

describe("test validate queries", () => {
  interface MockSchema {
    [key: string]: {};
    VendorID: {};
    store_and_fwd_flag: {};
    fare_amount: {};
    payment_type: {};
  }

  const headers: IndexHeader[] = [
    {
      fieldName: "VendorID",
      fieldTypes: [2],
    },
    {
      fieldName: "store_and_fwd_flag",
      fieldTypes: [3],
    },
    {
      fieldName: "fare_amount",
      fieldTypes: [2],
    },
    {
      fieldName: "payment_type",
      fieldTypes: [3],
    },
  ];

  let database: Database<MockSchema>;

  it(`test query builder`, () => {
    let qb = new QueryBuilder(database);

    let qb1 = qb.where("VendorID", "<=", 2);

    expect(() => {
      validateQuery(qb1.toQuery(), headers);
    }).not.toThrow();
  });

  it(`test basic query chain`, () => {
    let q = new QueryBuilder(database).where("VendorID", "<=", 2);
    let query = q.toQuery();

    expect(query.where).not.toBeNull();
    expect(query.where).toEqual([
      { key: "VendorID", operation: "<=", value: 2 },
    ]);

    expect(() => {
      validateQuery(query, headers);
    }).not.toThrow();

    q = q.orderBy("VendorID", "ASC");
    query = q.toQuery();

    expect(query.where).not.toBeNull();
    expect(query.where).toEqual([
      { key: "VendorID", operation: "<=", value: 2 },
    ]);
    expect(query.orderBy).not.toBeNull();
    expect(query.orderBy).toEqual([{ key: "VendorID", direction: "ASC" }]);
    expect(() => {
      validateQuery(query, headers);
    }).not.toThrow();

    q = q.select(["VendorID", "store_and_fwd_flag", "fare_amount"]);
    query = q.toQuery();
    expect(query.where).not.toBeNull();
    expect(query.where).toEqual([
      { key: "VendorID", operation: "<=", value: 2 },
    ]);
    expect(query.orderBy).not.toBeNull();
    expect(query.orderBy).toEqual([{ key: "VendorID", direction: "ASC" }]);
    expect(query.select).not.toBeNull();
    expect(query.select).toEqual([
      "VendorID",
      "store_and_fwd_flag",
      "fare_amount",
    ]);
  });

  it(`test basic derived query chain`, () => {
    const q0 = new QueryBuilder(database).where("fare_amount", "==", 1);
    let query = q0.toQuery();

    expect(query.where).not.toBeNull();
    expect(query.where).toEqual([
      { key: "fare_amount", operation: "==", value: 1 },
    ]);

    let q1 = q0.orderBy("fare_amount", "DESC");
    query = q1.toQuery();

    expect(query.where).not.toBeNull();
    expect(query.where).toEqual([
      { key: "fare_amount", operation: "==", value: 1 },
    ]);
    expect(query.orderBy).not.toBeNull();
    expect(query.orderBy).toEqual([{ key: "fare_amount", direction: "DESC" }]);

    let q2 = q1.select(["fare_amount"]);
    query = q2.toQuery();
    expect(query.where).not.toBeNull();
    expect(query.where).toEqual([
      { key: "fare_amount", operation: "==", value: 1 },
    ]);
    expect(query.orderBy).not.toBeNull();
    expect(query.orderBy).toEqual([{ key: "fare_amount", direction: "DESC" }]);
    expect(query.select).not.toBeNull();
    expect(query.select).toEqual(["fare_amount"]);
  });

  it(`test multi derived query chain`, () => {
    const q0 = new QueryBuilder(database).where("fare_amount", "==", 2);
    let query = q0.toQuery();

    expect(query.where).not.toBeNull();
    expect(query.where).toEqual([
      { key: "fare_amount", operation: "==", value: 2 },
    ]);

    let q1 = q0.where("VendorID", "==", 2);
    query = q1.toQuery();

    expect(query.where).not.toBeNull();
    expect(query.where).toEqual([
      { key: "fare_amount", operation: "==", value: 2 },
      { key: "VendorID", operation: "==", value: 2 },
    ]);
  });

  it(`test green + red queries`, () => {
    const q0 = new QueryBuilder(database).where("payment_type", ">", 3);
    const failQuery = q0.orderBy("VendorID", "ASC");
    expect(failQuery.toQuery().orderBy).toEqual([
      { key: "VendorID", direction: "ASC" },
    ]);

    const passQuery = q0.orderBy("payment_type", "DESC");
    expect(passQuery.toQuery().orderBy).toEqual([
      { key: "payment_type", direction: "DESC" },
    ]);

    const failQuery2 = passQuery.select(["wef"]);
    const passQuery2 = passQuery.select([
      "VendorID",
      "payment_type",
      "fare_amount",
    ]);

    // red queries
    [failQuery, failQuery2].forEach((query) => {
      expect(() =>
        validateQuery(query.toQuery(), headers)
      ).toThrow();
    });

    // green queries
    [passQuery, passQuery2].forEach((query) => {
      expect(() => validateQuery(query.toQuery(), headers)).not.toThrow();
    });
  });
});
