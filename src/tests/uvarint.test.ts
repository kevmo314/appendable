import {decodeUvarint} from "../uvarint";


describe("it decodes uvarint correctly", () => {
    test.each([
        { input: [0x01], expected: { value: 1, bytesRead: 1}},
        { input: [0xAC, 0x02], expected: { value: 300, bytesRead: 2}},
        { input: [0xFF, 0xFF, 0xFF, 0xFF, 0x07], expected: { value: Math.pow(2, 32) - 1, bytesRead: 5 }}, // Max 32-bit unsigned int
        { input: [0x80, 0x80, 0x80, 0x80, 0x10], expected: { value: 0, bytesRead: -5 }}, // Buffer too big, signals overflow
    ])("correctly decodes values", ({ input, expected}) => {
        const buf = new Uint8Array(input).buffer;
        const result = decodeUvarint(buf);

        expect(result).toEqual(expected)
    })

})