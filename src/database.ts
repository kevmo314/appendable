import { FormatType } from ".";
import { DataFile } from "./data-file";
import { VersionedIndexFile } from "./index-file";

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

type SelectField<T extends Schema> = keyof T;

export type Query<T extends Schema> = {
	where?: WhereNode<T>[];
	orderBy?: OrderBy<T>[];
	selectFields?: SelectField<T>[];
};

export enum FieldType {
	String = 1 << 0,
	Number = 1 << 1,
	Boolean = 1 << 4,
	Null = 1 << 5,
}

// given a fieldType and the desired type, this function performs a bitwise operation to test membership
export function containsType(fieldType: bigint, desiredType: FieldType) {
	return (fieldType & BigInt(desiredType)) !== BigInt(0);
}

function parseIgnoringSuffix(x: string, format: FormatType) {
	switch (format) {
		case FormatType.Jsonl:
			try {
				console.log("parsing no error", JSON.parse(x));
				return JSON.parse(x);
			} catch (error) {
				console.log("registered as an error");
				const e = error as Error;
				let m = e.message.match(/position\s+(\d+)/);
				if (m) {
					console.log(x.slice(0, Number(m[1])));
					x = x.slice(0, Number(m[1]));
				}
			}
			console.log(JSON.parse(x));
			return JSON.parse(x);

		case FormatType.Csv:
			try {
				console.log("parsing no error", parseCsvLine(x));
				return parseCsvLine(x);
			} catch (error) {
				console.log("registered as an error");
				let lastCompleteLine = findLastCompleteCsvLine(x);
				console.log(lastCompleteLine);
				return parseCsvLine(lastCompleteLine);
			}
	}
}

export function parseCsvLine(line: string) {
	console.log("parsing csv: ");
	let fields: string[] = line.split(",");

	fields.forEach((field) => {
		if (field.length > 0) {
			console.log("parsing: ", field);
			return JSON.parse(field);
		}
	});
}

function findLastCompleteCsvLine(data: string) {
	let lastNewlineIndex = data.lastIndexOf("\n");
	return lastNewlineIndex >= 0 ? data.slice(0, lastNewlineIndex) : data;
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
		private indexFile: VersionedIndexFile<T>,
		private formatType: FormatType
	) {}

	static forDataFileAndIndexFile<T extends Schema>(
		dataFile: DataFile,
		indexFile: VersionedIndexFile<T>,
		format: FormatType
	) {
		return new Database(dataFile, indexFile, format);
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
						console.log("data looks like: ", data);
						const dataFieldValue = parseIgnoringSuffix(data, this.formatType);
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
							),
							this.formatType
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

		console.log("Field ranges: ", fieldRanges);
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

				console.log(`Data record: `, dataRecord);
				const parsedFieldValue = parseIgnoringSuffix(
					await this.dataFile.get(
						dataRecord.startByteOffset,
						dataRecord.endByteOffset
					),
					this.formatType
				);

				let dataFieldValue = parsedFieldValue;
				if (query.selectFields && query.selectFields.length > 0) {
					if (
						typeof parsedFieldValue === "object" &&
						parsedFieldValue !== null
					) {
						dataFieldValue = query.selectFields.reduce((acc, field) => {
							if (field in parsedFieldValue) {
								acc[field] = parsedFieldValue[field];
							}
							return acc;
						}, {} as Partial<T>);
					}
				}

				yield dataFieldValue;
			}
		}
	}
}
