import { compareBytes } from "../node";

describe("test query relation", () => {
	beforeEach(() => {});

	const strToUint8Array = (str: string | null) => {
		if (str === null) return null;
		return new Uint8Array(str.split("").map((c) => c.charCodeAt(0)));
	};

	const testCases = [
		{ a: "", b: "", i: 0 },
		{ a: "a", b: "", i: 1 },
		{ a: "", b: "a", i: -1 },
		{ a: "abc", b: "abc", i: 0 },
		{ a: "abd", b: "abc", i: 1 },
		{ a: "abc", b: "abd", i: -1 },
		{ a: "ab", b: "abc", i: -1 },
		{ a: "abc", b: "ab", i: 1 },
		{ a: "x", b: "ab", i: 1 },
		{ a: "ab", b: "x", i: -1 },
		{ a: "x", b: "a", i: 1 },
		{ a: "b", b: "x", i: -1 },
		{ a: "abcdefgh", b: "abcdefgh", i: 0 },
		{ a: "abcdefghi", b: "abcdefghi", i: 0 },
		{ a: "abcdefghi", b: "abcdefghj", i: -1 },
		{ a: "abcdefghj", b: "abcdefghi", i: 1 },
		{ a: null, b: null, i: 0 },
		{ a: "", b: null, i: 0 },
		{ a: null, b: "", i: 0 },
		{ a: "a", b: null, i: 1 },
		{ a: null, b: "a", i: -1 },
	];

	// This test uses the Go test cases for `bytes.Compare` for `compareBytes()`
	// https://cs.opensource.google/go/go/+/refs/tags/go1.21.6:src/bytes/compare_test.go
	testCases.forEach(({ a, b, i }) => {
		it("test `compareBytes`", async () => {
			const result = compareBytes(strToUint8Array(a), strToUint8Array(b));
			console.log(
				`Testing a: ${a}, b: ${b}, expected: ${i}, received: ${result}`
			);

			expect(result).toBe(i);
		});
	});
});
