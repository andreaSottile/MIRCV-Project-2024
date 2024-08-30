import time
from collections import Counter

from nltk.corpus import stopwords
from src.modules.documentProcessing import fetch_data_row_from_collection
from src.modules.preprocessing import clean_text, remove_stopwords, stemming, tokenizer

stop_words = set(stopwords.words('english'))
punctuation_signs = []
print(stop_words)


def test_read(docid):
    tic = time.perf_counter()
    d, t = fetch_data_row_from_collection(docid)

    toc = time.perf_counter()
    print("document " + str(d))
    raw_text = t[0]
    print("raw text")
    print(raw_text)
    clean = clean_text(raw_text)
    print("clean text")
    print(clean)
    tokens = tokenizer(clean)
    clean_tokens = remove_stopwords(tokens)
    print("no stopwords text")
    print(clean_tokens)
    stemmed = stemming(clean_tokens)
    print("stemmed")
    print(stemmed)
    tac = time.perf_counter()
    print(f"read completed in {toc - tic:0.4f} seconds")
    print(f"preprocessing completed in {tac - toc:0.4f} seconds")

    count = Counter(stemmed)
    print("PRINTING COUNT LIST")
    for line in count.items():
        print(line)


# docid
test_read(52)
test_read(126)
test_read(532456)
test_read(6833456)
test_read(165562)
test_read(4564)
# preprocessing time is under 0.001s
