import { RangeResolver } from "../resolver";


export class IndexMeta {
    private resolver: RangeResolver;  
    
    public fieldName: string | null;
    public fieldType: number | null;

    constructor(resolver: RangeResolver) {
        this.resolver = resolver;
        this.fieldName = null;
        this.fieldType = null;
    }

    async unmarshalBinary(buffer: ArrayBuffer): Promise<void> {
        if (buffer.byteLength < 10) {
            throw new Error(`invalid metadata size ${buffer.byteLength}`);
        }

        const dataView = new DataView(buffer);

        const high = dataView.getUint32(0); 
        const low = dataView.getUint32(4); 
        this.fieldType = (high * Math.pow(2, 32)) + low; 

        const nameLength = dataView.getUint16(8);

        if(buffer.byteLength < 10 + nameLength) {
            throw new Error(`invalid metadata size: ${buffer.byteLength}`);
        }

        const { data: fieldNameData } = await this.resolver({
            start: 10,
            end: 10 + nameLength - 1,
        });

        this.fieldName = new TextDecoder().decode(fieldNameData);
    }

}