'''
Query Execution.
Your program must execute ranked queries. Your program must
return the top 10 or 20 results according to the TFIDF scoring function. You must
implement conjunctive and disjunctive query processing algorithms.

Scoring Function. You should implement the BM25 or other scoring functions such as
those based on language models

Query Processing Efficiency. You should implement skipping in your inverted lists,
you should implement the nextGEQ() operation in your posting interface, and
you should implement a dynamic pruning algorithm.
'''
import math

from src.config import posting_separator, element_separator, docid_separator, print_log, BM25_b, BM_k_one, \
    scoring_function_config
from src.modules.preprocessing import preprocess_text


class QueryHandler:

    def __init__(self, index_file):
        self.index = index_file
        # file stats.txt: doc_id,doc_no,doc_length
        self.doc_lengths = 0  # read from stats.txt
        self.doc_ids = 0  # read from stats.txt
        self.num_docs = 0  # len (stats.txt)
        self.doc_len_average = 0
        # TODO : calcolare il num_docs

    def prepare_query(self, query_raw):
        return preprocess_query_string(query_raw, stem_flag=self.index.skip_stemming,
                                       stop_flag=self.index.allow_stop_words)

    def compute_score(self, query_terms):
        # @param query_terms : list of query terms
        # @return : a dictionary of {doc_id,score}

        if self.index.scoring in scoring_function_config:
            return compute_scoring_function(query_terms, self.num_docs, self.doc_len_average, self.index.scoring)
        else:
            print_log("CRITICAL ERROR: scoring function not set correctly", 0)
            return 0

    def extract_top_k(self, results):
        # @param results: a dictionary of {doc_id,score}
        ranked_list = {k: v for k, v in sorted(results.items(), key=lambda x: x[1])}
        return ranked_list[0:self.index.topk]


def fetch_posting_list(key):
    pass
    return ""


def fetch_doc_size(key):
    pass
    return 0


def preprocess_query_string(query_raw, stem_flag, stop_flag):
    # check the preprocessing necessary in index
    words = preprocess_text(query_raw, skip_stemming=stem_flag, allow_stop_words=stop_flag)
    return words


def read_posting_list(posting_string):
    # convert a line from the index file to a dictionary with the useful info
    line = posting_string.split(posting_separator)
    posting_dict = {}
    for element in line[1].split(element_separator):
        pair = element.split(docid_separator)
        # docid = pair[0]
        # freq = pair[1] # required casting to integer
        posting_dict[pair[0]] = int(pair[1])
    # return : token_id,dictionary of {docid: term_freq}
    return line[0], posting_dict


def fetch_relevant_documents(query_terms, algorithm):
    # TODO
    candidates = []
    for term in query_terms:
        # read posting list
        raw_list = fetch_posting_list(term)
        key, posting_dict = read_posting_list(raw_list)
        if key != term:
            print_log("Term not matching? this should never happen", 1)
            continue


def compute_scoring_function(query_terms, doc_count, doclen_avg=1, scoring_f=""):
    # @ param query_terms : list of words in the query string
    # @ param doc_count : number of documents in the index
    # @ param doclen_avg : average of len of all the docs in the collection
    # @ param scoring_f : string for scoring function (see configs for options)
    # return a dictionary of relevant documents {docid,score}
    scores = {}
    for term in query_terms:  # TODO : toglliere questo ciclo e mettere solo la fetch_relevant_document
        # read posting list
        raw_list = fetch_posting_list(term)
        key, posting_dict = read_posting_list(raw_list)
        if key != term:
            print_log("Term not matching? this should never happen", 1)
            continue

        idf = math.log(doc_count / len(key))  # log ( N of docs in the collection / N of relevant docs )
        # extract doc_ids from posting list
        for docid, count in posting_dict:
            w_t_d = 0
            if scoring_f == "TFIDF":
                w_t_d = weight_tfidf(idf, term_freq=count)
            elif scoring_f == "BM11":
                w_t_d = weight_bm11(key, idf, term_freq=count, avg=doclen_avg)
            elif scoring_f == "BM15":
                w_t_d = weight_bm15(idf, term_freq=count)
            elif scoring_f == "BM25":
                w_t_d = weight_bm25(key, idf, term_freq=count, avg=doclen_avg)
            if key in scores.keys:
                # add the weight to the document's score
                scores[docid] += w_t_d
            else:
                # new relevant document, initialize its score
                scores[docid] = w_t_d
    return scores


def weight_tfidf(idf, term_freq):
    # calculate tfidf score for a query
    # @ param idf : IDF is calculated in outer scope, since it's the same for each term
    # @ param term_freq : frequency read from posting list
    if term_freq == 0:
        return 0
    '''
    per tfidf devo calcolare i pesi nella matrice matrice docid * queryterm, con tutti i term della query, con tutti i docid
     delle posting list di queste 
    per ogni parola, per ogni documento, calcolo tfidf(parola,documento)
    se la parola ha count == 0 nel documento, il peso vale zero
    altrimenti si calcola come:
    1 + log ( term freq elemento letto dalla posting list) * log ( numero_documenti / numero_documenti_con_queryterm)
    '''
    tf = 1 + math.log(term_freq)
    return tf * idf


def weight_bm25(docid, idf, term_freq, avg):
    # calculate bm25 score for a query
    # @ param docid : docid required to retreive the size of the document
    # @ param idf : IDF is calculated in outer scope, since it's the same for each term
    # @ param term_freq : frequency read from posting list
    # @ param avg : average of the length of all the documents in the collection
    if term_freq == 0:
        return 0
    doc_len = fetch_doc_size(docid)
    return idf * term_freq / (term_freq + BM_k_one * (1 - BM25_b + BM25_b * (doc_len / avg)))


def weight_bm15(idf, term_freq):
    # calculated like BM25 with BM25_b = 0
    return idf * term_freq / (term_freq * BM_k_one)


def weight_bm11(docid, idf, term_freq, avg):
    # calculated like BM25 with BM25_b = 1
    if term_freq == 0:
        return 0
    doc_len = fetch_doc_size(docid)
    return idf * term_freq / (term_freq + BM_k_one * (doc_len / avg))


'''
def query(self, query_str, top_k=10):
    query_terms = query_str.lower().split()
    scores = defaultdict(float)
    for term in query_terms:
        if term in self.index:
            for doc_id, _ in self.index[term]:
                scores[doc_id] += self.compute_tfidf(term, doc_id)
    ranked_docs = heapq.nlargest(top_k, scores.items(), key=lambda item: item[1])
    return [(self.doc_ids[doc_id], score) for doc_id, score in ranked_docs]
'''
