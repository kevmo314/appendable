import { Database, Query } from "../database";
import { DataFile } from "../data-file";
import { IndexFile, VersionedIndexFile } from "../index-file";

jest.mock("../data-file");
jest.mock("../index-file");

describe("test query relation", () => {
	let mockDataFile: jest.Mocked<DataFile>;
	let mockIndexFile: jest.Mocked<VersionedIndexFile<any>>;
	let database: Database<any>;
	beforeEach(() => {
		(DataFile.forUrl as jest.Mock).mockReturnValue({
			get: jest.fn().mockResolvedValue("mocked response"),
		});
		mockDataFile = DataFile.forUrl(
			"http://example.com/data"
		) as jest.Mocked<DataFile>;

		mockIndexFile = {
			indexFileHeader: jest.fn(),
			indexHeaders: jest.fn(),
			indexRecord: jest.fn(),
			dataRecord: jest.fn(),
		} as jest.Mocked<VersionedIndexFile<any>>;

		// instantiate a Database object with given mocked data file and index file
		database = Database.forDataFileAndIndexFile(mockDataFile, mockIndexFile);
	});

	/*
    This test case tests the query function in `database.ts`.
    */
	it("should handle a simple query", async () => {
		mockIndexFile.indexHeaders.mockResolvedValue([
			{
				fieldName: "weight",
				fieldType: BigInt(4),
				indexRecordCount: BigInt(1),
			},
			{
				fieldName: "age",
				fieldType: BigInt(4),
				indexRecordCount: BigInt(1),
			},
		]);

		mockIndexFile.indexRecord.mockResolvedValue({
			dataNumber: 1,
			fieldStartByteOffset: 0,
			fieldLength: 10,
		});

		mockIndexFile.dataRecord.mockResolvedValue({
			startByteOffset: 0,
			endByteOffset: 10,
		});

		// Adjust the mocked DataFile.get to return a string that represents a valid JSON object
		mockDataFile.get.mockImplementation(
			async (startByteOffset, endByteOffset) => {
				const mockData = { weight: 25, age: 30 }; // Mock data
				const field = "weight"; // Field being queried
				return JSON.stringify(mockData[field]);
			}
		);

		type PersonSchema = {
			weight: number;
			age: number;
		};

		const query: Query<PersonSchema> = {
			where: [
				{
					operation: "<",
					key: "weight",
					value: 30,
				},
			],
			orderBy: [
				{
					key: "weight",
					direction: "ASC",
				},
			],
		};

		const results = [];
		for await (const item of database.query(query)) {
			results.push(item);
		}

		expect(results).toEqual([25]);
	});
});

describe("test type bitmask decoding", () => {
	let mockDataFile: jest.Mocked<DataFile>;
	let mockIndexFile: jest.Mocked<VersionedIndexFile<any>>;
	let database: Database<any>;
	beforeEach(() => {
		(DataFile.forUrl as jest.Mock).mockReturnValue({
			get: jest.fn().mockResolvedValue("mocked response"),
		});
		mockDataFile = DataFile.forUrl(
			"http://example.com/data"
		) as jest.Mocked<DataFile>;

		mockIndexFile = {
			indexFileHeader: jest.fn(),
			indexHeaders: jest.fn(),
			indexRecord: jest.fn(),
			dataRecord: jest.fn(),
		} as jest.Mocked<VersionedIndexFile<any>>;

		// instantiate a Database object with given mocked data file and index file
		database = Database.forDataFileAndIndexFile(mockDataFile, mockIndexFile);
	});

	it("it should return the correct field rank from fieldtype", async () => {
		const testCases = [
			{ fieldType: BigInt(34), expected: new Set<number>([1, 3]) },
			{ fieldType: BigInt(1), expected: new Set<number>([4]) },
			{ fieldType: BigInt(2), expected: new Set<number>([3]) },
			{ fieldType: BigInt(16), expected: new Set<number>([2]) },
			{ fieldType: BigInt(32), expected: new Set<number>([1]) },
			{ fieldType: BigInt(33), expected: new Set<number>([1, 4]) },
		];

		testCases.forEach(({ fieldType, expected }) => {
			const result = database.decodeType(fieldType);
			expect(result).toEqual(expected);
		});
	});
});
