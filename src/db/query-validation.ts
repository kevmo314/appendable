import { Header } from "../index-file";
import { FieldType, Query, Schema } from "./database";

/**
 * containsType checks if the given compositeType includes the specified singleType.
 * It uses a bitwise AND operation to determine if the singleType's bit is set in the compositeType.
 *
 * @param {bigint} compositeType - A bigint representing a composite of multiple types.
 * @param {FieldType} singleType - The specific type to check for within the compositeType.
 * @returns {boolean} - Returns true if singleType is included in compositeType, false otherwise.
 */
function containsType(compositeType: bigint, singleType: FieldType): boolean {
	return (compositeType & BigInt(singleType)) !== BigInt(0);
}

/**
 * validateQuery checks the structure and syntax of the query against the provided headers.
 * It ensures that the fields specified in the query are valid and exist in the headers.
 * If any part of the query is invalid (e.g., a field doesn't exist), it throws an error.
 *
 * @param {Query<T>} query - The query object to validate.
 * @param {Header[]} headers - The headers against which to validate the query fields.
 * @throws {Error} Throws an error if query is invalid.
 */
export async function validateQuery<T extends Schema>(
	query: Query<T>,
	headers: Header[]
): Promise<void> {
	const headerNames = new Set(headers.map((header) => header.fieldName));

	const headerTypeMap: Record<string, bigint> = {};
	headers.forEach(({ fieldName, fieldType }) => {
		headerTypeMap[fieldName] = fieldType;
	});

	// check existence of 'where' clause
	if (!query.where || !Array.isArray(query.where) || query.where.length === 0) {
		throw new Error("Missing 'where' clause.");
	}

	// validate 'where' clause
	for (const whereNode of query.where) {
		if (!["<", "<=", "==", ">=", ">"].includes(whereNode.operation)) {
			throw new Error("Invalid operation in 'where' clause.");
		}

		if (typeof whereNode.key !== "string") {
			throw new Error("'key' in 'where' clause must be a string.");
		}

		if (!headerNames.has(whereNode.key)) {
			throw new Error(
				`key: ${whereNode.key} in 'where' clause does not exist in dataset.`
			);
		}

		if (typeof whereNode.value === "undefined") {
			throw new Error("'value' in 'where' clause is missing.");
		}

		const headerType = headerTypeMap[whereNode.key];

		const valueType = getType(whereNode.value);
		const expectedType = typeToFieldTypeMap[valueType];

		if (!expectedType) {
			throw new Error(`Unsupported type for key: ${whereNode.key}`);
		}

		if (!containsType(headerType, expectedType)) {
			throw new Error(
				`'key: ${whereNode.key} does not have type: ${headerType}.`
			);
		}
	}

	// check existence of 'orderBy' clause
	if (query.orderBy) {
		if (!Array.isArray(query.orderBy) || query.orderBy.length === 0) {
			throw new Error("Invalid 'orderBy' clause.");
		}

		// Note: currently we only support one orderBy and it must be the where clause. When we add composite indexes and complex querying, refactor.
		const orderBy = query.orderBy[0];

		if (!["ASC", "DESC"].includes(orderBy.direction)) {
			throw new Error("Invalid direction in `orderBy`.");
		}

		if (orderBy.key !== query.where[0].key) {
			throw new Error("'key' in `orderBy` must match `key` in `where` clause");
		}
	}

	// check existence of 'select' clause
	if (query.select) {
		if (!Array.isArray(query.select) || query.select.length === 0) {
			throw new Error("Invalid 'selectFields' clause");
		}

		for (const field of query.select) {
			if (!headerNames.has(field as string)) {
				throw new Error(
					`'key': ${field as string} in 'selectFields' clause does not exist in dataset.`
				);
			}
		}
	}
}

type ValueType = "null" | "boolean" | "number" | "bigint" | "string";

const typeToFieldTypeMap: { [key in ValueType]: FieldType } = {
	null: FieldType.Null,
	boolean: FieldType.Boolean,
	number: FieldType.Number,
	bigint: FieldType.Number,
	string: FieldType.String,
};

function getType(value: any): ValueType {
	if (value === null) return "null";
	return typeof value as ValueType;
}
