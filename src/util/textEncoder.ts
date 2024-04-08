export class QuickTextEncoder {
  private textEncoder: TextEncoder = new TextEncoder();

  buffer(phrase: string): ArrayBuffer {
    return this.textEncoder.encode(phrase).buffer;
  }
}
