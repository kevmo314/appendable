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
	 * Retrieves an immutable copy of the current query.
	 *
	 * @returns {Query<T>} The Query instance.
	 */
	toQuery(): Query<T> {
		return {
			where: this.queryObject.where ? [...this.queryObject.where] : [],
			orderBy: this.queryObject.orderBy
				? [...this.queryObject.orderBy]
				: undefined,
			select: this.queryObject.select
				? [...this.queryObject.select]
				: undefined,
			limit: this.queryObject.limit,
		};
	}

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
		const newQuery = new QueryBuilder<T>(this.database);
		newQuery.queryObject = {
			...this.queryObject,
			where: [...(this.queryObject.where || []), { key, operation, value }],
		};
		return newQuery;
	}
	/**
	 * Adds an 'orderBy' clause to the query.
	 *
	 * @param {keyof T} key - The index header's field name to order by.
	 * @param {OrderBy<T>["direction"]} direction - The sorting direction (e.g., ASC, DESC).
	 * @returns {QueryBuilder<T>} The QueryBuilder instance.
	 */
	orderBy(key: keyof T, direction: OrderBy<T>["direction"]): QueryBuilder<T> {
		const newQuery = new QueryBuilder<T>(this.database);
		newQuery.queryObject = {
			...this.queryObject,
			orderBy: [...(this.queryObject.orderBy || []), { key, direction }],
		};
		return newQuery;
	}

	/**
	 * Specifies the fields to be selected in the query.
	 *
	 * @param {(keyof T)[]} keys - A list of index header field names.
	 * @returns {QueryBuilder<T>} The QueryBuilder instance.
	 */
	select(keys: (keyof T)[]): QueryBuilder<T> {
		const newQuery = new QueryBuilder<T>(this.database);
		newQuery.queryObject = {
			...this.queryObject,
			select: keys,
		};
		return newQuery;
	}

	/**
	 * Limits the number of records returned by the query.
	 *
	 * @param {number} limit - The maximum number of records to return.
	 * @returns {QueryBuilder<T>} The QueryBuilder instance.
	 */
	limit(limit: number): QueryBuilder<T> {
		const newQuery = new QueryBuilder<T>(this.database);
		newQuery.queryObject = {
			...this.queryObject,
			limit: limit,
		};
		return newQuery;
	}
}
