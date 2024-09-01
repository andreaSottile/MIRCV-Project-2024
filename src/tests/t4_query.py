import time

from src.config import scoring_function_config, query_processing_algorithm_config, search_into_file_algorithms
from src.modules.InvertedIndex import index_setup, load_from_disk
from src.modules.queryHandler import QueryHandler

i = 0


# options available for k
# k_returned_results_config = [10, 20]
# options available for query processing
# query_processing_algorithm_config = ["conjunctive", "disjunctive"]
# options available for scoring function
# scoring_function_config = ["BM11", "BM25", "TFIDF"]

def print_query_result(results):
    for document, relevance in results:
        print("relevance: " + str(relevance) + " ; document: " + str(document))


def test_index(query_string, search_function, index_name, flags):
    tic = time.perf_counter()
    test_index_element = load_from_disk(index_name)
    if test_index_element is None:
        test_index_element = index_setup(index_name, stemming_flag=flags[4], stop_words_flag=flags[5],
                                         compression_flag="no",
                                         k=flags[3], join_algorithm=flags[1], scoring_f=flags[2])
    if test_index_element.is_ready():
        if not test_index_element.content_check(int(flags[0] / 2)):
            test_index_element.scan_dataset(flags[0], delete_after_compression=False)
    else:
        print("index not ready")

    # TEST QUERY
    handler = QueryHandler(test_index_element)
    res = handler.query(query_string, search_file_algorithms=search_function)

    print_query_result(res)

    toc = time.perf_counter()
    return tic, toc


# file_count, method, scoring, k, skip_stemming, allow stop words
# disjunctive test
config = [300, query_processing_algorithm_config[1], scoring_function_config[0], 4, True, True]
# disjunctive test, top6
config = [300, query_processing_algorithm_config[0], scoring_function_config[0], 6, True, True]

query_search_function = search_into_file_algorithms[0]

query_string_test = "communities between"
for query_processing_algorithm in query_processing_algorithm_config:
    for sf in scoring_function_config:
        print("test with query " + query_processing_algorithm + " and algorithm: " + sf)
        config = [300, query_processing_algorithm, sf, 6, True, True]
        test_index(query_string_test, query_search_function, "indt_" + query_processing_algorithm + "_" + sf, config)
