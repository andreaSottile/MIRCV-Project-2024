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
import os

from src.config import *
from src.modules.compression import decode_posting_list
from src.modules.preprocessing import preprocess_text
from src.modules.utils import get_last_line, ternary_search, get_row_id, print_log, set_search_interval, next_GEQ_line


def get_top_k(k, results):
    # rearrange a list of QueryResult and cut it to k elements
    print_log("Results list:", 4)
    print_log(results, 4)
    print_log("ordering the results list", 4)
    result_list = []
    for key, score in results.items():
        result_list.append((key, score))
    # order in descending order with the reverse flag
    ordered = sorted(result_list, key=lambda x: x[1], reverse=True)
    if len(ordered) > k:
        print_log("extracted top k elements of relevant documents", 4)
        return ordered[:k]
    else:
        print_log("relevant documents are less than k", 2)
        return ordered


class QueryHandler:

    def __init__(self, index_file):
        self.index = index_file
        # file stats.txt: doc_id,doc_no,doc_length
        self.doc_lengths = 0  # read from stats.txt
        self.doc_ids = 0  # read from stats.txt
        self.num_docs = index_file.num_doc
        self.doc_len_average = self.compute_docs_average(index_file.num_doc)  # len (stats.txt)

    def prepare_query(self, query_raw):
        return preprocess_query_string(query_raw, stem_flag=self.index.skip_stemming,
                                       stop_flag=self.index.allow_stop_words)

    def compute_docs_average(self, docs_count):  # len (stats.txt)
        total_length_doc = 0
        with open(self.index.collection_statistics_path, 'rb') as f:
            while True:
                content = f.readline()
                if len(content) == 0:
                    break
                content = content.decode("utf-8").strip().split(collection_separator)
                total_length_doc += int(content[2])
        avg_length = total_length_doc / docs_count
        return avg_length

    def query(self, query_string, search_algorithm):
        print_log("received query", 2)
        query_terms = self.prepare_query(query_string)
        print_log("reading posting lists for each query term", 2)
        raw_posting_lists = self.fetch_posting_lists(query_terms, search_algorithm)
        print_log("converting post list to dictionaries", 2)
        posting_lists = make_posting_candidates(raw_posting_lists)
        print_log("calculating relevance with algorithm: " + self.index.algorithm, 2)
        relevant_documents = self.detect_relevant_documents(posting_lists)
        print_log("calculating scores for relevant documents", 2)
        scores = self.compute_scoring_function(posting_lists, relevant_documents)
        return get_top_k(self.index.topk, scores)

    def extract_top_k(self, results):
        # @param results: a dictionary of {doc_id,score}
        ranked_list = {k: v for k, v in sorted(results.items(), key=lambda x: x[1])}
        return ranked_list[0:self.index.topk]

    def fetch_posting_lists(self, query_terms, search_algorithm):
        '''
        This function needs to:
        read lexicon file
        search query terms
        for each query term:
            read its posting list
            append the posting list to a list of results (strings)
        :param query_terms:
        :return:
        '''
        res=[]
        if self.index.is_ready():
            print_log("Calling Search on unknown index", 0)
            print_log(self.index.index_file_path, 0)
            return []
        print_log("opening file " + str(self.index.index_file_path), 4)
        if self.index.compression!= "no":
            index_file = open(self.index.index_file_path, "rb")
        else:
            index_file = open(self.index.index_file_path, "r+")
        with open(self.index.lexicon_path, 'r+') as f:
            for token in query_terms:
                res_offset_intervals = search_in_lexicon(f, token, search_algorithm)
                res_posting_string = search_in_index(res_offset_intervals, index_file, self.index.compression)
                res.append(token + res_posting_string)
        return res

    def fetch_doc_size(self, key):
        #    expected row from the collection statistics file:
        #    docid + collection_separator + docno + collection_separator + size + chunk_line_separator
        res = search_in_file(self.index.collection_statistics_path, self.index.num_doc, [key], collection_separator)
        # res is a string like "docid,docno,doc_size"
        size = res[0].split(collection_separator)[2]
        return int(size)

    def detect_relevant_documents(self, posting_lists):
        # take all the token_keys from the first element
        if len(posting_lists) < 1:
            print_log("no posting list found: ", 2)
            print_log(posting_lists, 2)
            return []
        # initialize the candidates with the keys of the first posting list
        candidates = set(posting_lists[next(iter(posting_lists))].keys())
        print_log("first set of candidates", 3)
        print_log(candidates, 3)
        for term in posting_lists:  # TODO: skip the first
            if self.index.algorithm == "conjunctive":
                # intersection
                candidates &= set(posting_lists[term].keys())
                print_log("conjunctive candidates", 5)
                print_log(candidates, 5)
            elif self.index.algorithm == "disjunctive":
                # union
                print_log("disjunctive candidates", 5)
                print_log(candidates, 5)
                for new_token in posting_lists[term].keys():
                    candidates.add(new_token)
            else:
                print_log("CRITICAL ERROR: query algorithm not set", 0)
                return []
        # returns a list of relevant docids
        print_log("relevant documents list", 2)
        print_log(candidates, 2)
        return sorted(list(candidates), key=lambda x: int(x))

    def compute_scoring_function(self, posting_lists, relevant_documents):
        # @ param query_terms : list of words in the query string
        # @ param doc_count : number of documents in the index
        # @ param doclen_avg : average of len of all the docs in the collection
        # @ param scoring_f : string for scoring function (see configs for options)
        # return the scored list of the relevant documents {docid,score}
        scores = {}
        if not relevant_documents:
            print_log("Cannot compute scores, no relevant document detected", 1)
            return {}
        print(posting_lists)
        for token_key, posting_dict in posting_lists.items():
            # log ( N of docs in the collection / N of relevant docs )
            idf = math.log(self.num_docs / len(posting_dict.keys()))
            print_log("calculated IDF : " + str(idf), 3)

            # read posting list, extract doc_ids from posting list
            for docid, count in posting_dict.items():
                if docid not in relevant_documents:
                    # check if document is flagged as relevant (at least one occurrence of token_key)
                    scores[docid] = 0
                    print_log("discarded not relevant document : " + str(docid), 5)
                    continue

                # weigth of the token for the document relevance
                w_t_d = 0
                if self.index.scoring == "TFIDF":
                    w_t_d = weight_tfidf(idf, term_freq=count)
                elif self.index.scoring == "BM11":
                    doc_len = self.fetch_doc_size(docid)
                    w_t_d = weight_bm11(idf, term_freq=count, avg=self.doc_len_average, doc_len=doc_len)
                elif self.index.scoring == "BM15":
                    w_t_d = weight_bm15(idf, term_freq=count)
                elif self.index.scoring == "BM25":
                    doc_len = self.fetch_doc_size(docid)
                    w_t_d = weight_bm25(idf, term_freq=count, avg=self.doc_len_average, doc_len=doc_len)
                if docid in scores.keys():
                    # add the weight to the document's score
                    scores[docid] += w_t_d
                else:
                    # new relevant document, initialize its score
                    scores[docid] = w_t_d
        return scores


def make_posting_candidates(raw_posting_lists):
    # make a dictionary of posting lists {token_key : posting_list_dictionary}
    # where each posting_list_dictionary is a { docid: term_freq}
    res = {}
    # each pl is a string representing a posting list
    for pl in raw_posting_lists:
        token_key, posting_list = read_posting_list(pl)
        res[token_key] = posting_list
    return res

def search_in_index(self, res_offset, index_file, compression):
    # index_file.seek(7802)
    # compressed_bytes = index_file.read()
    # decoded_doc_ids, decoded_freq = decode_posting_list(compressed_bytes, config[6], encoding_type="unary")
    # print(decoded_doc_ids)
    for offset_start, offset_stop in res_offset:
        nbytes = offset_stop - offset_start
        index_file.seek(offset_start)
        compressed_bytes = index_file.read(nbytes)
        posting_list_string_decoded = decode_posting_list(compressed_bytes, compression)
    return posting_list_string_decoded

def search_in_lexicon(lexicon, query_terms, method):
    # search some query_terms inside a file
    # returns a list of posting lists

    results = []
    last_read_position = 0
    for key in query_terms:
        if method == "ternary":
            last_line_pos, last_line = get_last_line(lexicon)
            line_pos, line = ternary_search(f, start_position=last_read_position, target_key=key,
                                            delimiter=key_delimiter, end_position=last_line_pos,
                                            last_key=get_row_id(last_line, key_delimiter), last_row=last_line)
        elif method == "skipping":
            step = search_chunk_size_config
            line_pos, high, line = set_search_interval(lexicon, last_read_position, key, key_delimiter,
                                                       step_size=step)
            # skipping big chunks when reading lots of files, small chunks when search interval is smaller
            while step >= 1:
                step = step // 10
                line_pos, high, line = set_search_interval(lexicon, line_pos, key, key_delimiter,
                                                           step_size=step)
                if key == get_row_id(line, key_delimiter):
                    break
            # precision scan
            while key != get_row_id(line, key_delimiter):
                line_pos, line = next_GEQ_line(lexicon, lexicon.tell())
                if lexicon.tell() > high:
                    # termination condition: nothing found
                    line_pos = -1
                    break

        if line_pos != -1:
            # i have found the correct word
            results.append(line.strip())
            # since i enforced the query_terms list to be sorted, there is no need to look the same lines again
            last_read_position = line_pos
    return results

    def read_line_at(file_pointer, row_id):
        print_log("opening file at line " + str(row_id), 5)
        file_pointer.seek(row_id)
        file_pointer.readline()  # Skip partial line
        return file_pointer.readline().decode()

    def binary_search_file(file_pointer, target_key): #  TODO
        # at the first execution, left and right are the first and the last line
        left, right = 0, file_size
        print_log("binary searching " + str(target_key), 5)
        while str(left) < str(right):
            mid = (left + right) // 2
            value = read_line_at(file_pointer, mid)
            if not value:
                # this should never happen, since we know there are no blank lines
                print_log("value not found at line "+str(mid))
                right = mid
                continue
            current_key = value.split(key_delimiter)[0]
            if current_key < target_key:
                left = mid
            else:
                right = mid
        return left

    with open(file_path, 'rb') as f:
        for key in query_terms:
            pos = binary_search_file(f, key)
            print_log("search result : " + str(pos), 5)
            f.seek(pos)
            while True:
                line = f.readline().decode()
                if not line or line.startswith(key + key_delimiter):
                    break
            if line.startswith(key + key_delimiter):
                results.append(line.strip())

    return results


def preprocess_query_string(query_raw, stem_flag, stop_flag):
    # check the preprocessing necessary in index
    words = preprocess_text(query_raw, skip_stemming=stem_flag, allow_stop_words=stop_flag)
    return words


def read_posting_list(posting_string):
    # convert a line from the index file to a dictionary with the useful info
    line = posting_string.split(posting_separator)
    # key = line[0] ; content = line[1]
    posting_dict = {}
    for element in line[1].split(element_separator):
        pair = element.split(docid_separator)
        # docid = pair[0]
        # freq = pair[1] # required casting to integer
        posting_dict[pair[0]] = int(pair[1])
    # return : token_id,dictionary of {docid: term_freq}
    return line[0], posting_dict


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


def weight_bm25(idf, term_freq, avg, doc_len):
    # calculate bm25 score for a query
    # @ param docid : docid required to retreive the size of the document
    # @ param idf : IDF is calculated in outer scope, since it's the same for each term
    # @ param term_freq : frequency read from posting list
    # @ param avg : average of the length of all the documents in the collection
    if term_freq == 0:
        return 0
    return idf * term_freq / (term_freq + BM_k_one * (1 - BM25_b + BM25_b * (doc_len / avg)))


def weight_bm15(idf, term_freq):
    # calculated like BM25 with BM25_b = 0
    return idf * term_freq / (term_freq * BM_k_one)


def weight_bm11(idf, term_freq, avg, doc_len):
    # calculated like BM25 with BM25_b = 1
    if term_freq == 0:
        return 0
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
