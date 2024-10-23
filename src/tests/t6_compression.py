from src.config import *
import time
from src.modules.InvertedIndex import index_setup, load_from_disk
from src.modules.QueryHandler import QueryHandler


def test_index(index_name, flags, compression):
    test_index_element = load_from_disk(index_name)
    if test_index_element is None:
        test_index_element = index_setup(index_name, stemming_flag=flags[4], stop_words_flag=flags[5],
                                         compression_flag=compression,
                                         k=flags[3], join_algorithm=flags[1], scoring_f=flags[2])
    if test_index_element.is_ready():
        if not test_index_element.content_check(int(flags[0] / 2)):
            test_index_element.scan_dataset(flags[0], delete_after_compression=False)
    return QueryHandler(test_index_element)


# gamma compression contains unary compression, so it's like testing both at the same time
config = [300, query_processing_algorithm_config[0], scoring_function_config[0], 15, False, True]

query_handler = test_index("indt6_compressed", config,  "gamma")
query_handler_uncompressed = test_index("indt6_uncompressed", config,  "no")

test_words = ["between", "also", "third", "interact", "medic", "respons"]

tic = time.perf_counter()
uncompressed_posting_lists, _ = query_handler_uncompressed.fetch_posting_lists(test_words, search_into_file_algorithms[0])
decompressed_posting_lists, _ = query_handler.fetch_posting_lists(test_words, search_into_file_algorithms[0])

toc = time.perf_counter()
i = 0
for pl in range(len(test_words)):
    print(test_words[i])
    print("uncompressed: ")
    print(uncompressed_posting_lists[i])
    print("decompressed: ")
    print(decompressed_posting_lists[i])
    print("\n")
    i += 1

print("execution time: " + str(toc - tic))
