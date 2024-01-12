import { Database, Query } from "../database";
import { DataFile } from "../data-file";
import { IndexFile, VersionedIndexFile } from "../index-file";

jest.mock("../data-file");
jest.mock("../index-file");

describe("Database", () => {
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

		database = Database.forDataFileAndIndexFile(mockDataFile, mockIndexFile);
	});

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
