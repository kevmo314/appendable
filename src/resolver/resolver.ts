/**
 * RangeResolver is a function that takes a range of bytes and returns a promise
 * that resolves to an ArrayBuffer containing the bytes in that range. Note that
 * the range is inclusive.
 *
 * Additionally, the RangeResolver must return a checksum which is computed from
 * the source data. This checksum is used to verify that the data has not been
 * changed between requests. The checksum can be any type, for example it is
 * valid to use the last modified timestamp of the source data or the total
 * length of the data. This checksum is passed to the RangeResolver on future
 * requests as the `checksum` argument. If it does not match the checksum when
 * reading the data, the RangeResolver should throw a LengthIntegrityError.
 *
 * @see LengthIntegrityError
 */
export type RangeResolver = (
  args: {
    start: number;
    end: number;
    expectedLength?: number;
  }[],
) => Promise<
  {
    data: ArrayBuffer;
    totalLength: number;
  }[]
>;

/**
 * LengthIntegrityError is thrown by a RangeResolver when the length argument is
 * inconsistent with the data returned. This is used to detect when the data has
 * changed between requests.
 *
 * When a LengthIntegrityError is thrown, typically the cache is evicted and the
 * query will be tried again with the exception of the data file where the error
 * is ignored due to the assumed immutability of the data file.
 *
 * @see RangeResolver
 */
export class LengthIntegrityError extends Error {
  constructor() {
    super("length integrity error");
  }
}
