import { Database } from "./database";
import { OrderBy, Query, Schema, WhereNode } from "./query-lang";
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

  constructor(private database: Database<T>) {}

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

  where(
    key: keyof T,
    operation: WhereNode<T>["operation"],
    value: T[keyof T],
  ): QueryBuilder<T> {
    const newQuery = new QueryBuilder<T>(this.database);
    newQuery.queryObject = {
      ...this.queryObject,
      where: [...(this.queryObject.where || []), { key, operation, value }],
    };
    return newQuery;
  }

  orderBy(key: keyof T, direction: OrderBy<T>["direction"]): QueryBuilder<T> {
    const newQuery = new QueryBuilder<T>(this.database);
    newQuery.queryObject = {
      ...this.queryObject,
      orderBy: [...(this.queryObject.orderBy || []), { key, direction }],
    };
    return newQuery;
  }

  select(keys: (keyof T)[]): QueryBuilder<T> {
    const newQuery = new QueryBuilder<T>(this.database);
    newQuery.queryObject = {
      ...this.queryObject,
      select: keys,
    };
    return newQuery;
  }

  limit(limit: number): QueryBuilder<T> {
    const newQuery = new QueryBuilder<T>(this.database);
    newQuery.queryObject = {
      ...this.queryObject,
      limit: limit,
    };
    return newQuery;
  }
}
