import { RangeResolver } from "../resolver";
import { MemoryPointer } from "./node";
import { PageFile } from "./pagefile";

const PAGE_SIZE_BYTES = 4096;
export const maxUint64 = 2n ** 64n - 1n;

export class LinkedMetaPage {
  private resolver: RangeResolver;
  private offset: bigint;
  private metaPageData: ArrayBuffer | null;
  private metaPagePromise: Promise<ArrayBuffer> | null = null;

  constructor(resolver: RangeResolver, offset: bigint) {
    this.resolver = resolver;
    this.offset = offset;
    this.metaPageData = null;
  }

  async root(): Promise<MemoryPointer> {
    const pageData = await this.getMetaPage();

    // we seek by 12 bytes since offset is 8 bytes, length is 4 bytes
    const data = pageData.slice(0, 12);
    const view = new DataView(data);

    const pointerOffset = view.getBigUint64(0, true);
    const lengthOffset = view.getUint32(8, true);

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

    const lengthView = new DataView(pageData, 24);

    // read the first four because that represents length
    const metadataLength = lengthView.getUint32(0, true);

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

    if (!this.metaPagePromise) {
      this.metaPagePromise = this.resolver({
        start: Number(this.offset),
        end: Number(this.offset) + PAGE_SIZE_BYTES - 1,
      })
        .then(({ data }) => {
          this.metaPageData = data;
          this.metaPagePromise = null;
          return data;
        })
        .catch((error) => {
          this.metaPagePromise = null;
          throw error;
        });
    }

    return this.metaPagePromise;
  }

  /**
   * `next()` - returns a new LinkedMetaPage
   */
  async next(): Promise<LinkedMetaPage | null> {
    const pageData = await this.getMetaPage();

    const view = new DataView(pageData, 12, 8);
    const nextOffset = view.getBigUint64(0, true);

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
  pageFile: PageFile
): LinkedMetaPage {
  const offset = pageFile.page(0);
  return new LinkedMetaPage(resolver, offset);
}
