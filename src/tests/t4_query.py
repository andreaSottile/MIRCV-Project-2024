import time

from src.config import scoring_function_config, query_processing_algorithm_config
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


def test_index(query_string, index_name, flags):
    tic = time.perf_counter()
    test_index_element = load_from_disk(index_name)
    if test_index_element is None:
        test_index_element = index_setup(index_name, stemming_flag=flags[4], stop_words_flag=flags[5],
                                         compression_flag=False,
                                         k=flags[3], algorithm=flags[1], scoring_f=flags[2], eval_f=0)
    if test_index_element.is_ready():
        if not test_index_element.content_check(int(flags[0] / 2)):
            test_index_element.scan_dataset(flags[0], delete_after_compression=False)
    else:
        print("index not ready")

    # TEST QUERY
    handler = QueryHandler(test_index_element)
    res = handler.query(query_string)

    print_query_result(res)

    toc = time.perf_counter()
    return tic, toc


# file_count, method, scoring, k, skip_stemming, allow stop words
# disjunctive test
config = [50, query_processing_algorithm_config[1], scoring_function_config[0], 4, True, True]
# disjunctive test, top6
config = [50, query_processing_algorithm_config[0], scoring_function_config[0], 6, True, True]

query_string_test = "communities between"
for query_processing_algorithm in query_processing_algorithm_config:
    for scoring_function in scoring_function_config:
        print("test with query " + query_processing_algorithm + " and algorithm: " + scoring_function)
        config = [50, query_processing_algorithm, scoring_function, 6, True, True]
        test_index(query_string_test, "indt_" + query_processing_algorithm + "_" + scoring_function, config)

test_index(query_string_test, "indt4", config)
