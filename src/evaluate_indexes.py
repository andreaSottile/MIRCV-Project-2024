import time

from evaluate import load

from src.config import query_processing_algorithm_config, scoring_function_config, k_returned_results_config, \
    compression_choices_config, evaluation_trec_queries_2020_path, search_into_file_algorithms
from src.modules.evaluation import prepare_index, read_query_file, create_run_dict, evaluate_on_trec, \
    plot_metrics_line_charts
from src.modules.utils import print_log

size_limit = -1

print("Prepare indexes")
query_handlers_catalogue = []
name_template = "eval_index_" + str(size_limit) + "_"
trec_eval = load("trec_eval")

# config : name, query_alg, scoring_f, k, stem_skip, allow_stopword, compression
config_set = []
for query_algorithm in query_processing_algorithm_config:
    for scoring_function in scoring_function_config:
        for topk in k_returned_results_config:
            for compression in compression_choices_config:
                config_set.append(
                    [name_template, query_algorithm, scoring_function, topk, True, True, compression])
                config_set.append(
                    [name_template, query_algorithm, scoring_function, topk, False, True,
                     compression])
                config_set.append(
                    [name_template, query_algorithm, scoring_function, topk, True, False,
                     compression])
                config_set.append(
                    [name_template, query_algorithm, scoring_function, topk, False, False,
                     compression])

for config in config_set[0:1]:
    query_handlers_catalogue.append(prepare_index(config, size_limit))

print("TREC EVALUATION")
query_file = open(evaluation_trec_queries_2020_path, "r")

trec_score_dicts_list = []
query_count = 0
next_qid, next_query = read_query_file(query_file)
while True:
    print_log("next query: " + next_query, 3)
    for handler in query_handlers_catalogue:
        for algorithm in search_into_file_algorithms:
            tic = time.perf_counter()
            result = handler.query(next_query, algorithm)
            # result have this structure [(docid, score),....(docid, score)]
            # example: [('116', 5.891602731662223), ('38', 0), ('221', 0), ('297', 0)]
            run_dict = create_run_dict(next_qid, handler.index.name, result)
            trec_score_dict = {}
            if len(run_dict['query']) > 0:
                trec_score_dict = evaluate_on_trec(run_dict, trec_eval)
            trec_score_dict["name"] = handler.index.name + " " + algorithm + " " + handler.index.scoring + " " +\
                                      handler.index.algorithm + " k=" + str(handler.index.topk)
            trec_score_dict["qid"] = next_qid
            toc = time.perf_counter()
            trec_score_dict["exec_time_s"] = toc - tic
            trec_score_dicts_list.append(trec_score_dict)

    query_count += 1
    next_qid, next_query = read_query_file(query_file)
    if query_count == 40:
        break
    if next_qid == -1:
        break

if size_limit == -1:
    plot_metrics_line_charts(trec_score_dicts_list)
