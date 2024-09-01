# TODO : leggersi pytrec e vedere come funziona

# TODO: FUNZIONAMENTO:
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

from src.config import *
from src.modules.InvertedIndex import load_from_disk, index_setup
from src.modules.queryHandler import QueryHandler


def read_query_file(file_pointer):
    line = file_pointer.readline()
    if line == "":
        return -1, ""
    content = line.split(" ")
    query_id = content[0]
    query_string = " ".join(content[1:])
    return query_id, query_string


def load_data(file_path):
    # Load the text file into a DataFrame with whitespace as delimiter
    df = pd.read_csv(file_path, delim_whitespace=True, header=None, names=['qid', 'q0', 'docid', 'relevance'])
    return df


def search_relevance(df, qid, docid):
    # Use .loc to filter rows that match the given qid and docid
    res = df.loc[(df['qid'] == qid) & (df['docid'] == docid), 'relevance']

    # If a match is found, return the points as an integer
    if not res.empty:
        return int(res.values[0])

    # If no match is found, return None
    return 0


def prepare_index(args):
    global size_limit
    test_index_element = load_from_disk(args[0])
    if test_index_element is None:
        test_index_element = index_setup(args[0], stemming_flag=args[4], stop_words_flag=args[5],
                                         compression_flag=args[6],
                                         k=args[3], join_algorithm=args[1], scoring_f=args[2])
    if test_index_element.is_ready():
        if not test_index_element.content_check(int(size_limit / 2)):
            test_index_element.scan_dataset(size_limit, delete_after_compression=True)
    return QueryHandler(test_index_element)


def relevance_score(relevance_table_df, query_id, k_result_list):
    for docid, _ in k_result_list:
        rel_score = search_relevance(relevance_table_df, query_id, docid)
        # TODO : calcolo del TREC score
    return 0


size_limit = 150

print("Prepare indexes")
query_handlers_catalogue = []
index_num = 1
name_template = "eval_index_" + str(size_limit) + "_"

# config : name, query_alg, scoring_f, k, stem_skip, allow_stopword, compression
confing_set = []
for query_algorithm in query_processing_algorithm_config:
    for scoring_function in scoring_function_config:
        for topk in k_returned_results_config:
            for compression in compression_choices_config:
                confing_set.append(
                    [name_template + str(index_num), query_algorithm, scoring_function, topk, True, True, compression])
                confing_set.append(
                    [name_template + str(index_num + 1), query_algorithm, scoring_function, topk, False, True,
                     compression])
                confing_set.append(
                    [name_template + str(index_num + 2), query_algorithm, scoring_function, topk, True, False,
                     compression])
                confing_set.append(
                    [name_template + str(index_num + 3), query_algorithm, scoring_function, topk, False, False,
                     compression])
                index_num += 4

# VERY DANGEROUS EXECUTION:
# for cfg in confing_set:
#    query_handlers_catalogue.append(prepare_index(cfg))

query_handlers_catalogue.append(prepare_index(confing_set[0:1]))

print("TREC 2019 EVALUATION")
relevance_dataframe = load_data(evaluation_trec_qrel_2019_path)
query_file = open(evaluation_trec_queries_2019_path, "r")

query_count = 0
next_qid, next_query = read_query_file(query_file)
while True:
    print("next query: " + next_query)
    for handler in query_handlers_catalogue:
        for algorithm in search_into_file_algorithms:
            result = handler.query(next_query, algorithm)
            trec_score = relevance_score(relevance_dataframe, next_qid, result)

    # TODO : vedere il confronto tra tutti

    query_count += 1
    next_qid, next_query = read_query_file(query_file)
