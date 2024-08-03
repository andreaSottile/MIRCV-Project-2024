import time

from src.modules.InvertedIndex import index_setup, load_from_disk

i = 0


# options available for k
# k_returned_results_config = [10, 20]
# options available for query processing
# query_processing_algorithm_config = ["conjunctive", "disjunctive"]
# options available for scoring function
# scoring_function_config = ["BM11", "BM25", "TFIDF"]

def test_index(query_string, file_count, index_name, method, scoring, k):
    tic = time.perf_counter()
    test_index_element = load_from_disk(index_name)
    if test_index_element is None:
        test_index_element = index_setup(index_name, stemming_flag=True, stop_words_flag=True, compression_flag=False,
                                         k=k, algorithm=method, scoring_f=scoring, eval_f=0)
    if test_index_element.is_ready():
        if not test_index_element.content_check(int(file_count / 2)):
            test_index_element.scan_dataset(file_count, delete_after_compression=False)
    else:
        print("index not ready")

    # TEST QUERY

    toc = time.perf_counter()
    return tic, toc
