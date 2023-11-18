export type RangeResolver = (
  start: number | bigint,
  end: number | bigint
) => Promise<ArrayBuffer>;
