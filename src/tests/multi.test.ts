import {LengthIntegrityError, RangeResolver} from "../resolver";
import {PageFile} from "../btree/pagefile";
import {ReadMultiBPTree} from "../btree/multi";
import {arrayBufferToString, readBinaryFile} from "./test-util";




describe("test multi", () => {

    let mockRangeResolver: RangeResolver;

    beforeEach(async () => {

        mockRangeResolver = async({ start, end, expectedLength }) => {
            const bufferSize = 4096 * 2; // 2 blocks of 4kb each
            const buffer = new ArrayBuffer(bufferSize);
            const view = new Uint8Array(buffer);

            const metadata = new Uint8Array(await readBinaryFile("filled_metadata.bin"));
            const metadataLength = metadata.byteLength;

            const dataView = new DataView(buffer);
            dataView.setUint32(4096 + 24, metadataLength);

            view.set(metadata, 4096 + 24 + 4);
            const slice = view.slice(start, end + 1);

            if(expectedLength !== undefined && slice.byteLength !== expectedLength) {
                throw new LengthIntegrityError;
            }

            return {
                data: slice.buffer,
                totalLength: view.byteLength,
            }
        }
    })


    it("storing metadata works", async() => {



        const pageFile = new PageFile(mockRangeResolver);
        const tree = ReadMultiBPTree(mockRangeResolver, pageFile);
        const metadata = await tree.metadata();

        expect("hello").toEqual(arrayBufferToString(metadata))
    });
});
