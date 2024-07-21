import time
from nltk.corpus import stopwords
from src.modules.documentProcessing import open_dataset, fetch_data_row_from_collection
from src.modules.preprocessing import clean_text, remove_stopwords, stemming

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
    stemmed = remove_stopwords(clean)
    print("no stopwords text")
    print(stemmed)
    stemmed = stemming(stemmed)
    print("stemmed")
    print(stemmed)
    tac = time.perf_counter()
    print(f"read completed in {toc - tic:0.4f} seconds")
    print(f"preprocessing completed in {tac - toc:0.4f} seconds")


# docid
test_read(52)
test_read(126)
test_read(532456)
test_read(6833456)
test_read(165562)
test_read(4564)
# preprocessing time is under 0.001s