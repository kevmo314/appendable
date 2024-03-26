export const N = 3;

type Trigram = {
  chunk: string;
  offset: number;
};

export function buildTrigram(phrase: string): Trigram[] {
  let trigrams: Trigram[] = [];

  let wordOffsets = [];
  let currentWordOffsets: number[] = [];

  Array.from(phrase).forEach((c, idx) => {
    if (/[a-zA-Z]/.test(c) || /[0-9]/.test(c)) {
      currentWordOffsets.push(idx);
    } else if (/\s/.test(c)) {
      if (currentWordOffsets.length >= N) {
        wordOffsets.push(currentWordOffsets);
      }
      currentWordOffsets = [];
    }
  });

  if (currentWordOffsets.length >= N) {
    wordOffsets.push(currentWordOffsets);
  }

  wordOffsets.forEach((word) => {
    for (let idx = 0; idx <= word.length - N; idx++) {
      let tri = "";

      for (let jdx = idx; jdx <= idx + N - 1; jdx++) {
        tri += phrase[word[jdx]];
      }

      trigrams.push({
        chunk: tri,
        offset: word[idx],
      });
    }
  });

  return trigrams;
}
