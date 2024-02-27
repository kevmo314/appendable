import { LinkedMetaPage } from "../btree/multi";
import { FieldType } from "../db/database";
import { IndexMeta } from "../index-file/meta";
import { SkipList } from "../index-file/skiplist";
import { RangeResolver } from "../resolver";

describe("test skiplist", () => {
  it("initializes a skiplist", () => {
    const sl = new SkipList();
    const head = sl.getSentinel();

    expect(head.fieldName).toEqual("");
    expect(head.fieldType).toEqual(0);
    expect(head.mp).toEqual(null);
  });

  let mockRangeResolver: RangeResolver;
  beforeEach(() => {
    mockRangeResolver = async ([{ start, end }]) => {
      return [
        {
          data: new ArrayBuffer(0),
          totalLength: 0,
        },
      ];
    };
  });

  it("inserts and queries", async () => {
    const s = new SkipList();

    const payload: { indexMeta: IndexMeta; metaPage: LinkedMetaPage }[] = [
      {
        indexMeta: {
          fieldName: "a",
          fieldType: FieldType.Null,
        },
        metaPage: new LinkedMetaPage(mockRangeResolver, 0n),
      },
      {
        indexMeta: {
          fieldName: "a",
          fieldType: FieldType.Boolean,
        },
        metaPage: new LinkedMetaPage(mockRangeResolver, 1n),
      },
      {
        indexMeta: {
          fieldName: "ab",
          fieldType: FieldType.String,
        },
        metaPage: new LinkedMetaPage(mockRangeResolver, 2n),
      },
      {
        indexMeta: {
          fieldName: "b",
          fieldType: FieldType.Float64,
        },
        metaPage: new LinkedMetaPage(mockRangeResolver, 3n),
      },

      {
        indexMeta: {
          fieldName: "b",
          fieldType: FieldType.Uint64,
        },
        metaPage: new LinkedMetaPage(mockRangeResolver, 4n),
      },

      {
        indexMeta: {
          fieldName: "b",
          fieldType: FieldType.Int64,
        },
        metaPage: new LinkedMetaPage(mockRangeResolver, 5n),
      },

      {
        indexMeta: {
          fieldName: "x",
          fieldType: FieldType.Float64,
        },
        metaPage: new LinkedMetaPage(mockRangeResolver, 6n),
      },

      {
        indexMeta: {
          fieldName: "trip_distance",
          fieldType: FieldType.Float64,
        },
        metaPage: new LinkedMetaPage(mockRangeResolver, 7n),
      },

      {
        indexMeta: {
          fieldName: "wef",
          fieldType: FieldType.Boolean,
        },
        metaPage: new LinkedMetaPage(mockRangeResolver, 8n),
      },
      {
        indexMeta: {
          fieldName: "wef2",
          fieldType: FieldType.Boolean,
        },
        metaPage: new LinkedMetaPage(mockRangeResolver, 9n),
      },
    ];

    for (const { indexMeta, metaPage } of payload) {
      await s.insert(indexMeta, metaPage);
    }

    for (const { indexMeta, metaPage } of payload) {
      const mp = await s.search(indexMeta);
      expect(mp).toEqual(metaPage);
    }
  });
});
