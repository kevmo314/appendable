import { validateQuery } from "../db/query-validation";
import { IndexHeader } from "../file/meta";
import { Query, Search } from "../db/query-lang";
import { FieldType } from "../db/database";

describe("validate search queries", () => {
  interface MockSchema {
    [key: string]: {};
    Pollo: {};
    Bife: {};
    Cerdo: {};
  }

  const headers: IndexHeader[] = [
    {
      fieldName: "Pollo",
      fieldTypes: [FieldType.Unigram, FieldType.Bigram, FieldType.Trigram],
    },
    {
      fieldName: "Bife",
      fieldTypes: [FieldType.Unigram, FieldType.Bigram, FieldType.Trigram],
    },
    {
      fieldName: "Cerdo",
      fieldTypes: [FieldType.Unigram, FieldType.Bigram, FieldType.Trigram],
    },
  ];

  it("performs a simple search query", () => {
    for (let minGram = 0; minGram <= 3; minGram++) {
      for (let maxGram = minGram; maxGram <= 3; maxGram++) {
        const search = {
          key: "Pollo",
          like: "wefhowdy",
          minGram,
          maxGram,
        };
        const q: Query<MockSchema> = { search };

        expect(() => {
          validateQuery(q, headers);
        }).not.toThrow();
      }
    }
  });

  it("query a defaults to a 12gram", () => {
    const search = {
      key: "Cerdo",
      like: "wefhowdy",
    };

    const q: Query<MockSchema> = { search };

    expect(() => {
      validateQuery(q, headers);
    }).not.toThrow();

    expect(q.search).not.toBeUndefined();
    expect(q.search!.config).not.toBeUndefined();
    expect(q.search!.config!.minGram).toEqual(1);
    expect(q.search!.config!.maxGram).toEqual(2);
  });

  it("fails to validate query via unknown header", () => {
    const search = {
      key: "Atun",
      like: "bacalao",
    };

    const q: Query<MockSchema> = { search };

    expect(() => {
      validateQuery(q, headers);
    }).toThrow();
  });

  it("fails to validate query via invalid range", () => {
    const search = {
      key: "Pollo",
      like: "bacalao",
      config: {
        minGram: 2,
        maxGram: 1,
      },
    };

    const q: Query<MockSchema> = { search };

    expect(() => {
      validateQuery(q, headers);
    }).toThrow();
  });
});

describe("validate filter queries", () => {
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

  it("test valid query", () => {
    validQueries.forEach((query) => {
      expect(() => {
        validateQuery(query, headers);
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
    it(`test invalid query ${index}`, () => {
      expect(() => validateQuery(query, headers)).toThrow();
    });
  });
});
