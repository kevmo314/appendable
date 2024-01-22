import { DataFile } from "./data-file";
import { IndexFile, VersionedIndexFile } from "./index-file";

type Schema = {
	[key: string]: {};
};

type WhereNode<T extends Schema, K extends keyof T = keyof T> = {
	operation: "<" | "<=" | "==" | ">=" | ">";
	key: keyof T;
	value: T[K];
};

type OrderBy<T extends Schema> = {
	key: keyof T;
	direction: "ASC" | "DESC";
};

export type Query<T extends Schema> = {
	where?: WhereNode<T>[];
	orderBy?: OrderBy<T>[];
};

// This record maps from fieldRank -> Bitmask
export const typeRankToBits: Record<number, bigint> = {
	4: BigInt(1), // FieldTypeString = 1 << 0
	3: BigInt(2), // FieldTypeNumber = 1 << 1
	//'Object': BigInt(4),    // FieldTypeObject = 1 << 2
	//'Array': BigInt(8),     // FieldTypeArray = 1 << 3
	2: BigInt(16), // FieldTypeBoolean = 1 << 4
	1: BigInt(32), // FieldTypeNull = 1 << 5
};

function parseIgnoringSuffix(x: string) {
	// TODO: implement a proper parser.
	try {
		return JSON.parse(x);
	} catch (error) {
		const e = error as Error;
		let m = e.message.match(/position\s+(\d+)/);
		if (m) {
			x = x.slice(0, Number(m[1]));
		}
	}
	return JSON.parse(x);
}

function fieldRank(token: any) {
	if (token === null) {
		return 1;
	}
	if (typeof token === "boolean") {
		return 2;
	}
	if (typeof token === "number" || typeof token === "bigint") {
		return 3;
	}
	if (typeof token === "string") {
		return 4;
	}
	throw new Error("unknown type");
}

function cmp(a: any, b: any) {
	const atr = fieldRank(a);
	const btr = fieldRank(b);
	if (atr !== btr) {
		return atr - btr;
	}
	switch (atr) {
		case 1:
			return 0;
		case 2:
			return a ? 1 : -1;
		case 3:
			return a - b;
		case 4:
			return a.localeCompare(b);
		default:
			throw new Error("unknown type");
	}
}

export class Database<T extends Schema> {
	private constructor(
		private dataFile: DataFile,
		private indexFile: VersionedIndexFile<T>
	) {}

	static forDataFileAndIndexFile<T extends Schema>(
		dataFile: DataFile,
		indexFile: VersionedIndexFile<T>
	) {
		return new Database(dataFile, indexFile);
	}

	async fields() {
		return await this.indexFile.indexHeaders();
	}

	async *query(query: Query<T>) {
		// verify that the query does not require a composite index
		if (new Set((query.where ?? []).map((where) => where.key)).size > 1) {
			throw new Error("composite indexes not supported... yet");
		}
		// convert each of the where nodes into a range of field values.
		const headers = await this.indexFile.indexHeaders();
		const fieldRanges = await Promise.all(
			(query.where ?? []).map(async ({ key, value, operation }) => {
				const header = headers.find((header) => header.fieldName === key);
				if (!header) {
					throw new Error("field not found");
				}
				let firstIndex = 0,
					lastIndex = Number(header.indexRecordCount);
				if (operation === ">" || operation === ">=" || operation === "==") {
					let start = 0;
					let end = Number(header.indexRecordCount);
					while (start + 1 < end) {
						const mid = Math.floor((start + end) / 2);
						const indexRecord = await this.indexFile.indexRecord(key, mid);
						const data = await this.dataFile.get(
							indexRecord.fieldStartByteOffset,
							indexRecord.fieldStartByteOffset + indexRecord.fieldLength
						);
						const dataFieldValue = parseIgnoringSuffix(data);
						console.log(mid, dataFieldValue);
						if (cmp(value, dataFieldValue) < 0) {
							end = mid;
						} else if (cmp(value, dataFieldValue) > 0) {
							start = mid + 1;
						} else if (operation === ">") {
							start = mid + 1;
						} else {
							end = mid;
						}
					}
					firstIndex = end;
				}
				if (operation === "<" || operation === "<=" || operation === "==") {
					let start = 0;
					let end = Number(header.indexRecordCount);
					while (start + 1 < end) {
						const mid = Math.floor((start + end) / 2);
						const indexRecord = await this.indexFile.indexRecord(key, mid);
						const dataFieldValue = parseIgnoringSuffix(
							await this.dataFile.get(
								indexRecord.fieldStartByteOffset,
								indexRecord.fieldStartByteOffset + indexRecord.fieldLength
							)
						);
						if (cmp(value, dataFieldValue) < 0) {
							end = mid;
						} else if (cmp(value, dataFieldValue) > 0) {
							start = mid + 1;
						} else if (operation === "<") {
							end = mid;
						} else {
							start = mid + 1;
						}
					}
					lastIndex = end;
				}
				return [key, [firstIndex, lastIndex]] as [keyof T, [number, number]];
			})
		);
		// group the field ranges by the field name and merge them into single ranges.
		const fieldRangeMap = new Map<keyof T, [number, number]>();
		for (const [key, value] of fieldRanges) {
			const existing = fieldRangeMap.get(key);
			if (existing) {
				fieldRangeMap.set(key, [
					Math.max(existing[0], value[0]),
					Math.min(existing[1], value[1]),
				]);
			} else {
				fieldRangeMap.set(key, value);
			}
		}
		// sort the field ranges by size.
		const fieldRangesSorted = [...fieldRangeMap.entries()].sort(
			(a, b) => a[1][1] - a[1][0] - (b[1][1] - b[1][0])
		);
		// move the order by fields to the front of the field ranges.
		const orderByFields = (query.orderBy ?? []).map((orderBy) => orderBy.key);
		for (const orderByField of orderByFields) {
			const index = fieldRangesSorted.findIndex(
				(fieldRange) => fieldRange[0] === orderByField
			);
			if (index >= 0) {
				fieldRangesSorted.unshift(...fieldRangesSorted.splice(index, 1));
			}
		}
		// evaluate the field ranges in order.
		for (const [key, [start, end]] of fieldRangesSorted) {
			// check if the iteration order should be reversed.
			const orderBy = query.orderBy?.find((orderBy) => orderBy.key === key);
			const reverse = orderBy?.direction === "DESC";
			const length = end - start;
			for (let offset = 0; offset < length; offset++) {
				const index = reverse ? end - offset - 1 : start + offset;
				const indexRecord = await this.indexFile.indexRecord(key, index);
				const dataRecord = await this.indexFile.dataRecord(
					indexRecord.dataNumber
				);
				const dataFieldValue = parseIgnoringSuffix(
					await this.dataFile.get(
						dataRecord.startByteOffset,
						dataRecord.endByteOffset
					)
				);
				yield dataFieldValue;
			}
		}
	}

	// given a bitmask, we decode and return the types
	decodeType(bitmask: bigint): Set<number> {
		let decodedRanks = new Set<number>();

		for (const [fieldRank, bitValue] of Object.entries(typeRankToBits)) {
			if ((bitmask & bitValue) !== BigInt(0)) {
				decodedRanks.add(Number(fieldRank));
			}
		}

		return decodedRanks;
	}
}
