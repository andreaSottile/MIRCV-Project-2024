import time

from nltk.corpus import stopwords

from src.modules.invertedIndex import index_setup

stop_words = set(stopwords.words('english'))
punctuation_signs = []
print(stop_words)

i = 0


def test_index(file_count, index_name):
    tic = time.perf_counter()
    test_index_element = index_setup(index_name, True, True, False, 3, 0, 0, 0)
    if test_index_element.is_ready():
        test_index_element.scan_dataset(file_count, False)
    else:
        print("index not ready")
    toc = time.perf_counter()
    return tic, toc


t1s, t1e = test_index(50, "t5")
print("T5 created in " + str(t1e - t1s))
