import { RangeResolver } from "../resolver";
import { MemoryPointer } from "./node";
import { PageFile } from "./pagefile";

const PAGE_SIZE_BYTES = 4096;

export class LinkedMetaPage {
  private resolver: RangeResolver;
  private offset: bigint;
  private metaPageData: ArrayBuffer | null;

  constructor(resolver: RangeResolver, offset: bigint) {
    this.resolver = resolver;
    this.offset = offset;
    this.metaPageData = null;
  }

  async root(): Promise<MemoryPointer | null> {
    const pageData = await this.getMetaPage();

    // we seek by 12 bytes since offset is 8 bytes, length is 4 bytes
    const data = pageData.slice(0, 12);
    const view = new DataView(data);

    const pointerOffset = view.getBigUint64(0);
    const lengthOffset = view.getUint32(8);

    return {
      offset: pointerOffset,
      length: lengthOffset,
    };
  }

  /**
   * `metadata()` gets the page data. It does the following:
   * 		(1) creates a slice from 24 to the end of the page
   * 		(2) it reads the first four bytes of that slice which gives us the length to seek to
   * 		(3) slices from [24, (24 + dataLength)] which contain metadata
   */
  async metadata(): Promise<ArrayBuffer> {
    const pageData = await this.getMetaPage();
    const lengthData = pageData.slice(24, PAGE_SIZE_BYTES);

    const lengthView = new DataView(lengthData);

    // read the first four because that represents length
    const metadataLength = lengthView.getUint32(0);

    return pageData.slice(28, 28 + metadataLength);
  }

  /**
   * `getMetaPage()` seeks the index-file with the absolute bounds for a given page file.
   * It caches the data in a pagefile. Note: all other methods that call this should be slicing with relative bounds.
   */
  private async getMetaPage(): Promise<ArrayBuffer> {
    if (this.metaPageData) {
      return this.metaPageData;
    }

    const { data } = await this.resolver({
      start: Number(this.offset),
      end: Number(this.offset) + PAGE_SIZE_BYTES - 1,
    });

    this.metaPageData = data;

    return data;
  }

  /**
   * `next()` - returns a new LinkedMetaPage
   */
  async next(): Promise<LinkedMetaPage | null> {
    const pageData = await this.getMetaPage();
    const data = pageData.slice(12, 12 + 8);

    const view = new DataView(data);
    const nextOffset = view.getBigUint64(0);
    const maxUint64 = BigInt(2) ** BigInt(64) - BigInt(1);
    console.log("next offset: ", nextOffset);
    if (nextOffset === maxUint64) {
      return null;
    }

    return new LinkedMetaPage(this.resolver, nextOffset);
  }

  getOffset(): bigint {
    return this.offset;
  }
}

export function ReadMultiBPTree(
  resolver: RangeResolver,
  pageFile: PageFile,
): LinkedMetaPage {
  const offset = pageFile.page(0);

  return new LinkedMetaPage(resolver, offset);
}
