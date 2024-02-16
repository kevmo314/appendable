import { IndexMeta } from "../index-file/meta";
import {
	FieldType,
	OrderBy,
	Query,
	Schema,
	SelectField,
	WhereNode,
} from "./database";

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
 * validateWhere validates the 'where' clause of the query.
 *
 * @param {WhereNode<T>[] | undefined} where - The 'where' clause to validate.
 * @param {IndexMeta[]} headers - List of headers to check field existence and type compatibility.
 * @throws {Error} - Throws an error if the 'where' clause is missing, invalid, or refers to non-existent fields.
 */
function validateWhere<T extends Schema>(
	where: WhereNode<T>[] | undefined,
	headers: IndexMeta[]
): void {
	if (!where || !Array.isArray(where) || where.length === 0) {
		throw new Error("Missing 'where' clause.");
	}


	for (const whereNode of where) {
		if (!["<", "<=", "==", ">=", ">"].includes(whereNode.operation)) {
			throw new Error("Invalid operation in 'where' clause.");
		}

		if (typeof whereNode.key !== "string") {
			throw new Error("'key' in 'where' clause must be a string.");
		}

		const header = headers.find((h) => h.fieldName === whereNode.key);

		if (!header) {
			throw new Error(
				`key: ${whereNode.key} in 'where' clause does not exist in dataset.`
			);
		}

		if (typeof whereNode.value === "undefined") {
			throw new Error("'value' in 'where' clause is missing.");
		}

		const headerType = header.fieldType;

		if (whereNode.value === null) {
			if (!containsType(headerType, FieldType.Null)) {
				throw new Error(`'key: ${whereNode.key} does not have type: null.`);
			}
		} else {
			function fieldTypeError(
				key: string,
				actual: FieldType,
				expected: bigint
			): string {
				return `key: ${key} does not have type: ${actual}. Expected: ${expected}`;
			}

			switch (typeof whereNode.value) {
				case "bigint":
				case "number":
					if (!containsType(headerType, FieldType.Number)) {
						throw new Error(
							fieldTypeError(whereNode.key, FieldType.Number, headerType)
						);
					}
					break;
				case "boolean":
					if (!containsType(headerType, FieldType.Boolean)) {
						throw new Error(
							fieldTypeError(whereNode.key, FieldType.Boolean, headerType)
						);
					}
					break;
				case "string":
					if (!containsType(headerType, FieldType.String)) {
						throw new Error(
							fieldTypeError(whereNode.key, FieldType.String, headerType)
						);
					}
					break;
				default:
					throw new Error(`Unsupported type for key: ${whereNode.key}`);
			}
		}
	}
}

/**
 * validateOrderBy validates the 'orderBy' clause of the query.
 * Currently supports strictly one 'orderBy' condition that must match the 'where' clause key.
 *
 * @param {OrderBy<T>[] | undefined} orderBy - The 'orderBy' clause to validate.
 * @param {string} whereKey - The key used in the 'where' clause for compatibility.
 * @throws {Error} Throws an error if the 'orderBy' clause is invalid or doesn't match the 'where' clause key.
 */
function validateOrderBy<T extends Schema>(
	orderBy: OrderBy<T>[] | undefined,
	whereKey: string
): void {
	if (orderBy) {
		if (!Array.isArray(orderBy) || orderBy.length === 0) {
			throw new Error("Invalid 'orderBy' clause.");
		}

		// Note: currently we only support one orderBy and it must be the where clause. When we add composite indexes and complex querying, refactor.
		const orderByObj = orderBy[0];

		if (!["ASC", "DESC"].includes(orderByObj.direction)) {
			throw new Error("Invalid direction in `orderBy`.");
		}

		if (orderByObj.key !== whereKey) {
			throw new Error("'key' in `orderBy` must match `key` in `where` clause");
		}
	}
}

/**
 * validateSelect validates the 'select' fields of a query.
 *
 * @param {SelectField<T>[] | undefined} select - The 'select' clause to validate.
 * @param {IndexMeta[]} headers - List of headers to check for field existence.
 * @throws {Error} Throws an error if any field in the 'select' clause doesn't exist in headers.
 */
function validateSelect<T extends Schema>(
	select: SelectField<T>[] | undefined,
	headers: IndexMeta[]
): void {
	if (select) {
		if (!Array.isArray(select) || select.length === 0) {
			throw new Error("Invalid 'select' clause");
		}

		for (const field of select) {
			const header = headers.find((h) => h.fieldName === field);

			if (!header) {
				throw new Error(
					`'key': ${field as string} in 'select' clause does not exist in dataset.`
				);
			}
		}
	}
}

/**
 * validateQuery checks the structure and syntax of the query against the provided headers.
 * It ensures that the fields specified in the query are valid and exist in the headers.
 * If any part of the query is invalid (e.g., a field doesn't exist), it throws an error.
 *
 * @param {Query<T>} query - The query object to validate.
 * @param {IndexMeta[]} headers - The headers against which to validate the query fields.
 * @throws {Error} Throws an error if query is invalid.
 */
export async function validateQuery<T extends Schema>(
	query: Query<T>,
	headers: IndexMeta[]
): Promise<void> {
	validateWhere(query.where, headers);
	validateOrderBy(query.orderBy, query.where![0].key as string);
	validateSelect(query.select, headers);
}
