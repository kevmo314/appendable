import { RangeResolver } from "./resolver";

export function cache(resolver: RangeResolver): RangeResolver {
	const cache: [
		[number, number],
		Promise<{ data: ArrayBuffer; totalLength: number }>,
	][] = [];

	return async ({
		start,
		end,
	}): Promise<{ data: ArrayBuffer; totalLength: number }> => {
		// check if start-end is contained in any of the cached ranges
		const cached = cache.find(([[s, e]]) => s <= start && end <= e);
		if (cached) {
			return cached[1].then((cachedData) => {
				const data = cachedData.data.slice(
					start - cached[0][0],
					end - cached[0][0]
				);
				return {
					data,
					totalLength: cachedData.totalLength,
				};
			});
		}

		// TODO: check if start-end overlaps with any of the cached ranges

		const promise = resolver({ start, end });
		cache.push([[start, end], promise]);
		return promise;
	};
}
