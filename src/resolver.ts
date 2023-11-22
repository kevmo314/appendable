export type RangeResolver = (
  start: number,
  end: number
) => Promise<ArrayBuffer>;
