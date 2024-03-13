export type UvarintResponse = {
	value: number;
	bytesRead: number;
};

const MAXVARINT64LEN = 10;

export function decodeUvarint(buf: ArrayBuffer): UvarintResponse {
	let x: number = 0;
	let s: number = 0;

	const view = new Uint8Array(buf);

	for (let idx = 0; idx <= view.length - 1; idx++) {
		let b = view[idx];

		if (idx === MAXVARINT64LEN) {
			return { value: 0, bytesRead: -(idx + 1) };
		}

		if (b < 0x80) {
			if (idx === MAXVARINT64LEN - 1 && b > 1) {
				return { value: 0, bytesRead: -(idx + 1) };
			}

			let value = (b & 0x7f) << s
			return { value, bytesRead: idx + 1 };
		}

		x |= (b & 0x7f) << s;
		s += 7;
	}

	return { value: 0, bytesRead: 0 };
}
