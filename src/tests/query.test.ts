import { Query } from "../db/database";
import { validateQuery } from "../db/query-validation";
import { Header } from "../index-file";

describe("test validate queries", () => {
	interface MockSchema {
		[key: string]: {};
		VendorID: {};
		store_and_fwd_flag: {};
		fare_amount: {};
		payment_type: {};
	}

	const headers: Header[] = [
		{
			fieldName: "VendorID",
			fieldType: BigInt(2),
			indexRecordCount: BigInt(683211),
		},
		{
			fieldName: "store_and_fwd_flag",
			fieldType: BigInt(33),
			indexRecordCount: BigInt(423),
		},
		{
			fieldName: "fare_amount",
			fieldType: BigInt(2),
			indexRecordCount: BigInt(68211),
		},
		{
			fieldName: "payment_type",
			fieldType: BigInt(34),
			indexRecordCount: BigInt(63887),
		},
	];

	const validQueries: Query<MockSchema>[] = [
		{
			where: [
				{
					operation: "==",
					key: "VendorID",
					value: 1,
				},
			],
		},
		{
			where: [
				{
					operation: "<",
					key: "fare_amount",
					value: 100,
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
					value: "",
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
					key: "store_and_flag",
					direction: "ASC",
				},
			],
		},
		{
			where: [
				{
					operation: "<",
					key: "payment_type",
					value: 100,
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
                }
            ],
            select: ["payment_type"]
        }
	];

	notValidQueries.forEach((query, index) => {
		it(`test invalid query ${index}`, async () => {
			await expect(validateQuery(query, headers)).rejects.toThrow();
		});
	});
});
