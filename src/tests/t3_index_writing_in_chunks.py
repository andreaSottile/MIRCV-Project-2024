import time

from nltk.corpus import stopwords

from src.modules.InvertedIndex import index_setup

stop_words = set(stopwords.words('english'))
punctuation_signs = []
print(stop_words)

i = 0


def test_index(file_count, index_name):
    tic = time.perf_counter()
    test_index_element = index_setup(index_name, True, True, "no", 3, 0, 0)
    if test_index_element.is_ready():
        test_index_element.scan_dataset(file_count, False)
    else:
        print("index not ready")
    toc = time.perf_counter()
    return tic, toc


t1s, t1e = test_index(50, "t50")
print("T5 created in " + str(t1e - t1s))
# 11s
# index file size: 15kB
# lexicon filesize: 9kB
# about 3 chunks (size: 250)

t2s, t2e = test_index(1500, "t1500")
print("T5 created in " + str(t2e - t2s))
# 770s
# index file size: 451kB
# lexicon filesize: 110kB
# about 150 chunks (size: 250)
