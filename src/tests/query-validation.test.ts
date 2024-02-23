import { Query } from "../db/database";
import { validateQuery } from "../db/query-validation";
import { IndexHeader, IndexMeta } from "../index-file/meta";

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
      fieldTypes: [0],
    },
    {
      fieldName: "store_and_fwd_flag",
      fieldTypes: [6],
    },
    {
      fieldName: "fare_amount",
      fieldTypes: [3],
    },
    {
      fieldName: "payment_type",
      fieldTypes: [3, 0],
    },
  ];

  const validQueries: Query<MockSchema>[] = [
    {
      where: [
        {
          operation: "==",
          key: "VendorID",
          value: "",
        },
      ],
    },
    {
      where: [
        {
          operation: "<",
          key: "fare_amount",
          value: 10,
        },
      ],
      orderBy: [
        {
          key: "fare_amount",
          direction: "ASC",
        },
      ],
    },
    {
      where: [
        {
          operation: ">=",
          key: "payment_type",
          value: 300,
        },
      ],
      orderBy: [
        {
          key: "payment_type",
          direction: "DESC",
        },
      ],
      select: ["payment_type", "fare_amount"],
    },
    {
      where: [
        {
          operation: "==",
          key: "store_and_fwd_flag",
          value: false,
        },
      ],
      select: ["fare_amount", "payment_type"],
    },
  ];

  validQueries.forEach((query) => {
    it("test valid query", async () => {
      expect(async () => {
        await validateQuery(query, headers);
      }).not.toThrow();
    });
  });

  const notValidQueries: Query<MockSchema>[] = [
    {
      where: [
        {
          operation: "<=",
          key: "vendorid",
          value: 1,
        },
      ],
    },
    {
      where: [
        {
          operation: "==",
          key: "store_and_fwd_flag",
          value: 10,
        },
      ],
      orderBy: [
        {
          key: "store_an_flag",
          direction: "ASC",
        },
      ],
    },
    {
      where: [
        {
          operation: "<",
          key: "payment_type",
          value: false,
        },
      ],
      select: ["payment_type", "vendorid", "store_and_fwd_flag"],
    },
    {
      where: [
        {
          operation: "==",
          key: "payment_type",
          value: "",
        },
      ],
      select: ["paymet_type"],
    },
  ];

  notValidQueries.forEach((query, index) => {
    it(`test invalid query ${index}`, async () => {
      await expect(validateQuery(query, headers)).rejects.toThrow();
    });
  });
});
