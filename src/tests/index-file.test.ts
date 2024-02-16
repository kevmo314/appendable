import { ReadMultiBPTree } from "../btree/multi";
import { PageFile } from "../btree/pagefile";
import { readFileMeta } from "../index-file/meta";
import { RangeResolver } from "../resolver";
import { readBinaryFile } from "./test-util";

describe("test index-file parsing", () => {

    let mockRangeResolver: RangeResolver;

    beforeEach(() => {
        mockRangeResolver = async ({ start, end }) => {
            const indexFile = await readBinaryFile("green_tripdata_2023-01.csv.index");
            const slicedPart = indexFile.slice(start, end + 1);

            const arrayBuffer = slicedPart.buffer.slice(slicedPart.byteOffset, slicedPart.byteOffset + slicedPart.byteLength);
           
            

            console.log("indexFile", start, end, arrayBuffer.byteLength);

            return {
                data: arrayBuffer,
                totalLength: arrayBuffer.byteLength, 
            }
        }
    });



    it("should read the file meta", async () => {
        const pageFile = new PageFile(mockRangeResolver);
        
        const tree = ReadMultiBPTree(mockRangeResolver, pageFile);

        const metadata = await tree.metadata();

        const fileMeta = await readFileMeta(metadata);

        console.log(fileMeta);

        expect(fileMeta.format).toEqual(1);
        expect(fileMeta.version).toEqual(1);

        console.log(fileMeta.readOffset)

    });


});