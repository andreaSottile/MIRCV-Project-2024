import copy
import os
import time

from evaluate import load

from src.config import query_processing_algorithm_config, scoring_function_config, k_returned_results_config, \
    evaluation_trec_queries_2020_path, search_into_file_algorithms, index_config_path, \
    file_format, output_query_trec_evaluation_file
from src.modules.InvertedIndex import load_from_disk
from src.modules.QueryHandler import QueryHandler
from src.modules.evaluation import read_query_file, create_run_dict, evaluate_on_trec, make_name
from src.modules.utils import print_log, export_dict_to_file, search_in_file

print("Preparing indexes")
query_handlers_catalogue = []
trec_eval = load("trec_eval")

# indexes available on hard disk (made with merge_indexes)
# warning: merge indexes ignores local paths. moving files to different computers requires editing the files

indexes_to_evaluate = ["indexes_full_do_stemming_keep_stopwords", "indexes_full_do_stemming_no_stopwords",
                       "indexes_full_no_stemming_keep_stopwords", "indexes_full_no_stemming_no_stopwords"]
compression_sets = ["_uncompressed", "_gamma"]  # skipped: unary (unfeasible disk size)

config_set = []

index_limit = 3  # test purposes. put -1 to have no limits
tic = time.perf_counter()
for compression in compression_sets:
    for index_name in indexes_to_evaluate:
        # find index on disk (must be present)
        local_name = index_name + compression
        if index_limit != 0:
            if os.path.exists(index_config_path + local_name + file_format):

                base_index = load_from_disk(local_name)
                query_algorithm="conjunctive"
                scoring_function="TFIDF"
                topk=20
                #for query_algorithm in query_processing_algorithm_config:
                #for scoring_function in scoring_function_config:
                #for topk in k_returned_results_config:
                #TAB START
                # avoid sharing the same index
                index = copy.deepcopy(base_index)
                # assign the required parameter configurations
                index.topk = topk  # options required by the specs
                index.scoring = scoring_function  # tfidf, bm11, etc...
                index.algorithm = query_algorithm  # conjunctive/disjunctive
                # prepare a query handler
                query_handler = QueryHandler(index)
                query_handlers_catalogue.append(query_handler)
                if index_limit > 0:
                    index_limit -= 1
                    print_log(f"loaded {len(query_handlers_catalogue)} indexes", 1)
                else:
                    break  # test purposes: reduce the number of indexes to evaluate
                #TAB END
            else:
                print_log("missing index to evaluate: " + str(local_name), 0)
toc = time.perf_counter()
print_log(f"total number of configuration sets to evaluate: {len(query_handlers_catalogue)} ", 1)
print_log(f"loading index phase took {toc - tic} s", 2)
print_log("loading trec file", 2)

query_file = open(evaluation_trec_queries_2020_path, "r")

trec_score_dicts_list = []
query_count = 0
next_qid, next_query = read_query_file(query_file)
print_log("starting evaluation", 1)
queries_to_skip = [0]
while True:
    timer = 0
    index_count = 0
    print_log("next query: " + next_query, 2)
    if query_count in queries_to_skip:
        print_log(" skipped ", 3)
        # skipping some queries is useful for debug
    else:
        for handler in query_handlers_catalogue:
            print_log(f"running query on index {index_count}", 2)
            index_count += 1
            #if index_count < 7:
            #    continue
            for algorithm in search_into_file_algorithms:
                if search_in_file(output_query_trec_evaluation_file, handler.index.name + " " + handler.index.scoring + " " + str(handler.index.topk) + " " + handler.index.algorithm + " " + algorithm, next_qid):
                    continue
                tic = time.perf_counter()
                result = handler.query(next_query, algorithm)
                # result have this structure [(docid, score),....(docid, score)]
                # example: [('116', 5.891602731662223), ('38', 0), ('221', 0), ('297', 0)]
                run_dict = create_run_dict(next_qid, handler.index.name, result)

                # dictionary to plot the results
                trec_score_dict = {}
                if len(run_dict['query']) > 0:
                    trec_score_dict = evaluate_on_trec(run_dict, trec_eval)
                    trec_score_dict["empty"] = False
                else:
                    trec_score_dict["empty"] = True
                # important: append something even if the result is empty
                trec_score_dict["name"] = make_name(handler, algorithm)
                trec_score_dict["qid"] = next_qid

                toc = time.perf_counter()
                trec_score_dict["exec_time_s"] = toc - tic
                trec_score_dicts_list.append(trec_score_dict)
                timer += toc - tic

    print_log(f"evaluation for query{query_count} took {timer} ", 2)
    query_count += 1
    next_qid, next_query = read_query_file(query_file)
    if next_qid == -1:
        break  # termination condition: query file is over

score_count = 0

export_dict_to_file(trec_score_dicts_list)

print_log("evaluation complete, plotting data", 1)
# plot_metrics_line_charts(trec_score_dicts_list)
