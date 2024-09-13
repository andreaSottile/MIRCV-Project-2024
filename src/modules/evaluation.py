'''
EVALUATION PHASE
0 - we have different indexes, each one having a different set of parameters
1 - take a query from the trec-queries.tsv
2 - execute the query (on each index we have)
3 - for each document retrieved, search the relevance in the trec-qrel.txt
                    (there is a relevance for each query_id-doc_id pair)
4 - take the list of relevances and calculate the measure score
5 - compare (graphically?) the scores between different indexes
6 - repeat for each query at step 1

'''

import pandas as pd
from evaluate import load
import matplotlib.pyplot as plt
import time
from src.config import *

from src.modules.InvertedIndex import load_from_disk, index_setup
from src.modules.queryHandler import QueryHandler
from src.modules.utils import print_log

'''
example usage of qrel
qrel_test = {
    "query": [0],
    "q0": ["q0"],
    "docid": ["doc_1"],
    "rel": [2]
}
run_test = {
    "query": [0, 0],
    "q0": ["q0", "q0"],
    "docid": ["doc_2", "doc_1"],
    "rank": [0, 1],
    "score": [1.5, 1.2],
    "system": ["test", "test"]
}

trec_eval = load("trec_eval")
results_test = trec_eval.compute(references=[qrel_test], predictions=[run_test])
print("ciao")
'''


def evaluate_on_trec(run_dict):
    '''
    This function take as an argument a dict related to a query with this structure:
        query (int): Query ID.
        q0 (str): Literal "q0".
        docid (str): Document ID.
        rank (int): Rank of document.
        score (float): Score of document.
        system (str): Tag for current run.

        Example:
            run = {
                    "query": [0, 0],
                    "q0": ["q0", "q0"],
                    "docid": ["doc_2", "doc_1"],
                    "rank": [0, 1],
                    "score": [1.5, 1.2],
                    "system": ["test", "test"]
                 }
    :param run_dict: dict on a single retrieval run.
    :return:
    '''
    # Structure of QREL txt file qid, “Q0”, docid, rating
    qrel = pd.read_csv(evaluation_trec_qrel_2020_path, sep=' ',
                       names=["query", "q0", "docid", "rel"])
    qrel["q0"] = "q0"
    qrel["docid"] = qrel["docid"].astype(str)
    qrel = qrel.to_dict(orient="list")
    results = {}
    if run_dict['query'][0] in qrel['query']:
        trec_eval = load("trec_eval")
        results = trec_eval.compute(references=[qrel], predictions=[run_dict])
    return results


def create_run_dict(qid, name, docID_score):
    """
    This function creates the run_dict with this structure:
        predictions (dict): a single retrieval run.
        query (int): Query ID.
        q0 (str): Literal "q0".
        docid (str): Document ID.
        rank (int): Rank of document.
        score (float): Score of document.
        system (str): Tag for current run.

    Example of a simple dict:
        run = {
        "query": [0, 0],
        "q0": ["q0", "q0"],
        "docid": ["doc_2", "doc_1"],
        "rank": [0, 1],
        "score": [1.5, 1.2],
        "system": ["test", "test"]
        }
    :param qid:
    :param name:
    :param docID_score:
    :return:
    """
    run_dict = {}
    run_dict["query"] = []
    run_dict["q0"] = []
    run_dict["docid"] = []
    run_dict["rank"] = []
    run_dict["score"] = []
    run_dict["system"] = []
    i = 0
    for doc_id, score in docID_score:
        run_dict["query"].append(int(qid))
        run_dict["q0"].append("q0")
        run_dict["docid"].append(doc_id)
        run_dict["rank"].append(i)
        run_dict["score"].append(score)
        run_dict["system"].append(name)
        i += 1
    return run_dict


def plot_metrics_line_charts(data_list):
    '''
    METRICS offered by huggingfaces
    runid (str): Run name.
    num_ret (int): Number of retrieved documents.
    num_rel (int): Number of relevant documents.
    num_rel_ret (int): Number of retrieved relevant documents.
    num_q (int): Number of queries.
    map (float): Mean average precision.
    gm_map (float): geometric mean average precision.
    bpref (float): binary preference score.
    Rprec (float): precision@R, where R is number of relevant documents.
    recip_rank (float): reciprocal rank
    P@k (float): precision@k (k in [5, 10, 15, 20, 30, 100, 200, 500, 1000]).
    NDCG@k (float): nDCG@k (k in [5, 10, 15, 20, 30, 100, 200, 500, 1000]).
    '''
    metrics = ['map', 'P@30', 'Rprec', 'NDCG@5']

    for metric in metrics:
        # Create a figure for each metric
        plt.figure(figsize=(10, 6))

        # Group data by 'name' and plot each group's data
        grouped_data = {}
        for item in data_list:
            name = item['name']
            if name not in grouped_data:
                grouped_data[name] = {'qid': [], 'values': []}
            grouped_data[name]['qid'].append(item['qid'])
            grouped_data[name]['values'].append(item[metric])

        # TODO: mettere subplot
        # Plotting the lines for each name
        for name, values in grouped_data.items():
            plt.plot(values['qid'], values['values'], marker='o', label=name)

        # Add titles and labels
        plt.title(f'{metric} Line Chart')
        plt.xlabel('QID')
        plt.ylabel(f'{metric} Value')

        # Rotate x-axis labels for better readability
        plt.xticks(rotation=45, ha='right')

        # Show the legend
        plt.legend(title='Name')

        # Adjust layout to prevent overlap
        plt.tight_layout()

        # Display the chart
        plt.show()


def read_query_file(file_pointer):
    line = file_pointer.readline()
    if line == "":
        return -1, ""
    content = line.split("\t")
    query_id = content[0]
    query_string = " ".join(content[1:])
    return query_id, query_string


def prepare_index(args):
    global size_limit

    # name building: makes clear which indexing flag is used, to avoid repeating indexing with the same flags
    name = "_" + str(args[0]) + "_"
    if args[6] == "no":
        name += "uncompressed_"
    else:
        name += args[6] + "_"
    if args[5]:
        name += "stopwords_"
    if args[4]:
        name += "no_stemming"
    else:
        name += "w_stemming"

    tic = time.perf_counter()
    test_index_element = load_from_disk(name)

    if test_index_element is None:
        test_index_element = index_setup(name, stemming_flag=args[4], stop_words_flag=args[5],
                                         compression_flag=args[6],
                                         k=args[3], join_algorithm=args[1], scoring_f=args[2])
    else:
        test_index_element.algorithm = args[1]
        test_index_element.scoring = args[2]
        test_index_element.topk = args[3]
    if test_index_element.is_ready():
        if not test_index_element.content_check(int(size_limit / 2)):
            test_index_element.scan_dataset(size_limit, delete_after_compression=True)

    toc = time.perf_counter()
    print_log("===========================================================", 0)
    print_log("Time for preparing the index: ", 0)
    print_log(str(toc - tic), 0)
    print_log("===========================================================", 0)
    return QueryHandler(test_index_element)


# size_limit = -1
size_limit = 318

print("Prepare indexes")
query_handlers_catalogue = []
name_template = "eval_index_" + str(size_limit) + "_"

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

for config in config_set[:1]:
    query_handlers_catalogue.append(prepare_index(config))

print("TREC EVALUATION")
query_file = open(evaluation_trec_queries_2020_path, "r")

trec_score_dicts_list = []
query_count = 0
next_qid, next_query = read_query_file(query_file)
while True:
    print_log("next query: " + next_query, 4)
    for handler in query_handlers_catalogue:
        for algorithm in search_into_file_algorithms:
            result = handler.query(next_query, algorithm)
            # result have this structure [(docid, score),....(docid, score)]
            # example: [('116', 5.891602731662223), ('38', 0), ('221', 0), ('297', 0)]
            run_dict = create_run_dict(next_qid, handler.index.name, result)
            trec_score_dict = {}
            if len(run_dict['query']) > 0:
                trec_score_dict = evaluate_on_trec(run_dict)
            trec_score_dict["name"] = handler.index.name
            trec_score_dict["qid"] = next_qid
            trec_score_dicts_list.append(trec_score_dict)
    query_count += 1
    next_qid, next_query = read_query_file(query_file)
    if next_qid == -1:
        break

if size_limit == -1:
    plot_metrics_line_charts(trec_score_dicts_list)
