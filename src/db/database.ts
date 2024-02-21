import { BPTree, ReferencedValue } from "../btree/bptree";
import { MemoryPointer } from "../btree/node";
import { DataFile } from "../data-file";
import { VersionedIndexFile } from "../index-file/index-file";
import { FileFormat, readIndexMeta } from "../index-file/meta";
import { QueryBuilder } from "./query-builder";

export type Schema = {
	[key: string]: {};
};

export type WhereNode<T extends Schema, K extends keyof T = keyof T> = {
	operation: "<" | "<=" | "==" | ">=" | ">";
	key: keyof T;
	value: T[K];
};

export type OrderBy<T extends Schema> = {
	key: keyof T;
	direction: "ASC" | "DESC";
};

export type SelectField<T extends Schema> = keyof T;

export type Query<T extends Schema> = {
	where?: WhereNode<T>[];
	orderBy?: OrderBy<T>[];
	select?: SelectField<T>[];
	limit?: number;
};

export enum FieldType {
	String = 0,
	Int64 = 1,
	Uint64 = 2,
	Float64 = 3,
	Object = 4,
	Array = 5,
	Boolean = 6,
	Null = 7,
}

function parseIgnoringSuffix(
	x: string,
	format: FileFormat,
	headerFields: string[]
) {
	switch (format) {
		case FileFormat.JSONL:
			try {
				return JSON.parse(x);
			} catch (error) {
				const e = error as Error;
				let m = e.message.match(/position\s+(\d+)/);
				if (m) {
					console.log(x.slice(0, Number(m[1])));
					x = x.slice(0, Number(m[1]));
				}
			}
			console.log(JSON.parse(x));
			return JSON.parse(x);

		case FileFormat.CSV:
			const fields = x.split(",");

			if (fields.length === 2) {
				x = fields[0];
				return JSON.parse(x);
			} else {
				const newlinePos = x.indexOf("\n");
				const result = newlinePos !== -1 ? x.substring(0, newlinePos) : x;
				const csvFields = result.split(",");

				// assert lengths are equal
				if (csvFields.length === headerFields.length) {
					return buildJsonFromCsv(csvFields, headerFields);
				} else {
					return result;
				}
			}
	}
}

function buildJsonFromCsv(csvFields: string[], headerFields: string[]) {
	return headerFields.reduce<{ [key: string]: string }>(
		(acc, header, index) => {
			acc[header] = csvFields[index];
			return acc;
		},
		{}
	);
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
		if (new Set((query.where ?? []).map((where) => where.key)).size > 1) {
			throw new Error("composite indexes not supported... yet");
		}

		const headers = await this.indexFile.indexHeaders();

		for (const { key, value, operation } of query.where ?? []) {
			const header = headers.find((header) => header.fieldName === key);
			if (!header) {
				throw new Error("field not found");
			}

			let valueBuf: ArrayBuffer;
			let fieldType: number;

			if (key === null) {
				fieldType = FieldType.Null;
				valueBuf = new ArrayBuffer(0);
			} else {
				switch (typeof value) {
					case "bigint":
					case "number":
						fieldType = FieldType.Float64;
						valueBuf = new ArrayBuffer(8);
						new DataView(valueBuf).setFloat64(0, Number(value), true);
						break;

					case "boolean":
						fieldType = FieldType.Boolean;
						valueBuf = new ArrayBuffer(1);
						new DataView(valueBuf).setUint8(0, value ? 1 : 0);
						break;

					case "string":
						fieldType = FieldType.String;
						valueBuf = new TextEncoder().encode(value as string).buffer;
						break;
					default:
						throw new Error("unmatched key type");
				}
			}
			const mps = await this.indexFile.seek(key as string, fieldType);
			const mp = mps[0];

			const dfResolver = this.dataFile.getResolver();

			if (dfResolver === undefined) {
				throw new Error("data file is undefined");
			}

			const valueRef = new ReferencedValue({ offset: 0n, length: 0 }, valueBuf);

			const { format } = await this.indexFile.metadata();
			const { fieldType: mpFieldType } = await readIndexMeta(
				await mp.metadata()
			);

			const bptree = new BPTree(
				this.indexFile.getResolver(),
				mp,
				dfResolver,
				format,
				mpFieldType
			);

			const iter = bptree.iter(valueRef);

			if (operation === ">") {
				while (await iter.next()) {
					const currentKey = iter.getKey();

					let res = ReferencedValue.compareBytes(valueBuf, currentKey.value);
					if (res === 1) {
						const [_, mp] = await bptree.find(currentKey);

						const data = await this.dataFile.get(
							Number(mp.offset),
							Number(mp.offset) + mp.length - 1
						);

						yield JSON.parse(data);
					}
				}
			} else if (operation === ">=") {
				while (await iter.next()) {
					const currentKey = iter.getKey();

					let res = ReferencedValue.compareBytes(valueBuf, currentKey.value);
					if (res === 1 || res === 0) {
						const [_, mp] = await bptree.find(currentKey);

						const data = await this.dataFile.get(
							Number(mp.offset),
							Number(mp.offset) + mp.length - 1
						);

						yield JSON.parse(data);
					}
				}
			} else if (operation === "==") {
				while (await iter.next()) {
					const currentKey = iter.getKey();

					let res = ReferencedValue.compareBytes(valueBuf, currentKey.value);
					if (res === 0) {
						const [_, mp] = await bptree.find(currentKey);

						const data = await this.dataFile.get(
							Number(mp.offset),
							Number(mp.offset) + mp.length - 1
						);

						yield JSON.parse(data);
					}
				}
			} else if (operation === "<=") {
				// this is the only case where we need to go both ways.
				// first we'll go prev() to catch everything <
				// then we'll go next() to catch everything ==
				while (await iter.prev()) {
					const currentKey = iter.getKey();

					let res = ReferencedValue.compareBytes(valueBuf, currentKey.value);
					if (res === -1) {
						const [_, mp] = await bptree.find(currentKey);

						const data = await this.dataFile.get(
							Number(mp.offset),
							Number(mp.offset) + mp.length - 1
						);

						yield JSON.parse(data);
					}
				}

				// reset iterator
				const iter2 = bptree.iter(valueRef);

				while (await iter2.next()) {
					const currentKey = iter2.getKey();

					let res = ReferencedValue.compareBytes(valueBuf, currentKey.value);
					if (res === 0) {
						const [_, mp] = await bptree.find(currentKey);

						const data = await this.dataFile.get(
							Number(mp.offset),
							Number(mp.offset) + mp.length - 1
						);

						yield JSON.parse(data);
					}
				}
			} else {
				while (await iter.prev()) {
					const currentKey = iter.getKey();

					let res = ReferencedValue.compareBytes(valueBuf, currentKey.value);
					if (res === -1) {
						const [_, mp] = await bptree.find(currentKey);

						const data = await this.dataFile.get(
							Number(mp.offset),
							Number(mp.offset) + mp.length - 1
						);

						yield JSON.parse(data);
					}
				}
			}
		}
	}

	where(
		key: keyof T,
		operation: WhereNode<T>["operation"],
		value: T[keyof T]
	): QueryBuilder<T> {
		return new QueryBuilder(this).where(key, operation, value);
	}
}
