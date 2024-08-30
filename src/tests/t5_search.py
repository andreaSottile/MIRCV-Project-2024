import time

from src.config import *

from src.modules.InvertedIndex import load_from_disk, index_setup
from src.modules.queryHandler import QueryHandler

test_index_element = load_from_disk("indt4")


def test_init_index(name, flags):
    test_index_element = load_from_disk(name)
    if test_index_element is None:
        test_index_element = index_setup(name, stemming_flag=flags[4], stop_words_flag=flags[5],
                                         compression_flag="no",
                                         k=flags[3], join_algorithm=flags[1], scoring_f=flags[2], eval_f=0)
    if test_index_element.is_ready():
        if not test_index_element.content_check(int(flags[0] / 2)):
            test_index_element.scan_dataset(flags[0], delete_after_compression=False)
    else:
        print("index not ready")
    return QueryHandler(test_index_element)


def test_search(words_list, file_search_algorithm):
    global query_handler
    print(" --  Searching with " + file_search_algorithm + "  -- ")

    print(words_list)
    tic = time.perf_counter()

    # TEST QUERY

    res = query_handler.query(words_list, search_file_algorithms=file_search_algorithm)

    toc = time.perf_counter()
    print(" -- Query completed in " + str(toc - tic) + "ms-- ")
    print(res)


config = [300, query_processing_algorithm_config[0], scoring_function_config[0], 4, True, True]
query_handler = test_init_index("t5", config)

print("test query: between companies")
print("testing ternary search")
test_search("between companies", search_into_file_algorithms[0])
print("test query: third figur")
test_search("final third figur", search_into_file_algorithms[0])

print("testing skipping search")
print("test query: between companies")
test_search("between companies", search_into_file_algorithms[1])
print("test query: third figur")
test_search("final third figur", search_into_file_algorithms[1])
