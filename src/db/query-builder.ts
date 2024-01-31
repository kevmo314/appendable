import { Database, OrderBy, Query, Schema, WhereNode } from "./database";

/**
 * A class for building and executing database queries in a flexible API style.
 * Allows chaining methods for 'where', 'orderBy', 'select', and 'limit' clauses.
 */
export class QueryBuilder<T extends Schema> {
	private queryObject: Query<T> = {
		where: [],
		orderBy: undefined,
		select: undefined,
		limit: undefined,
	};

	/**
	 * Initializes a new instance of the QueryBuilder class.
	 * @param {Database<T>} database - An Appendable database instance to execute queries against.
	 */
	constructor(private database: Database<T>) {}

	/**
	 * Executes the constructed query
	 */
	get() {
		return this.database.query(this.queryObject);
	}

	/**
	 * Adds a 'where' clause to the query.
	 *
	 * @param {keyof T} key - The index header's field name to apply the 'where' condition.
	 * @param {WhereNode<T>["operation"]} operation - The comparison relation (e.g., >=, <=, ==, >=, >).
	 * @param {T[keyof T]} value - The value to compare against.
	 * @returns {QueryBuilder<T>} The QueryBuilder instance.
	 */
	where(
		key: keyof T,
		operation: WhereNode<T>["operation"],
		value: T[keyof T]
	): QueryBuilder<T> {
		this.queryObject.where?.push({ key, operation, value });
		return this;
	}

	/**
	 * Adds an 'orderBy' clause to the query.
	 *
	 * @param {keyof T} key - The index header's field name to order by.
	 * @param {OrderBy<T>["direction"]} direction - The sorting direction (e.g., ASC, DESC).
	 * @returns {QueryBuilder<T>} The QueryBuilder instance.
	 */
	orderBy(key: keyof T, direction: OrderBy<T>["direction"]): QueryBuilder<T> {
		this.queryObject.orderBy
			? this.queryObject.orderBy.push({ key, direction })
			: (this.queryObject.orderBy = [{ key, direction }]);

		return this;
	}

	/**
	 * Specifies the fields to be selected in the query.
	 *
	 * @param {(keyof T)[]} keys - A list of index header field names.
	 * @returns {QueryBuilder<T>} The QueryBuilder instance.
	 */
	select(keys: (keyof T)[]): QueryBuilder<T> {
		this.queryObject.select = keys;
		return this;
	}

	/**
	 * Limits the number of records returned by the query.
	 *
	 * @param {number} limit - The maximum number of records to return.
	 * @returns {QueryBuilder<T>} The QueryBuilder instance.
	 */
	limit(limit: number): QueryBuilder<T> {
		this.queryObject.limit = limit;
		return this;
	}
}
