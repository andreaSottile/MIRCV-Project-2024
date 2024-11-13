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

from src.config import *
from src.modules.cache import cache_hit_or_miss, cache_get_posting_list, cache_push
from src.modules.compression import decode_posting_list
from src.modules.PostingList import PostingList
from src.modules.preprocessing import preprocess_text
from src.modules.utils import get_last_line, ternary_search, get_row_id, print_log, set_search_interval, next_GEQ_line
import time


def get_top_k(k, results):
    # rearrange a list of QueryResult and cut it to k elements
    print_log("Results list:", 4)
    print_log(results, 4)
    result_list = []
    if results != {}:
        print_log("ordering the results list", 4)
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
    else:
        return result_list


class QueryHandler:

    def __init__(self, index_file):
        self.index = index_file
        # file stats.txt: doc_id,doc_no,doc_length
        self.doc_lengths = 0  # read from stats.txt
        self.doc_ids = 0  # read from stats.txt
        self.num_docs = index_file.num_doc
        self.doc_len_average = self.compute_docs_average(index_file.num_doc)  # len (stats.txt)
        self.doc_stats_cache = {}

    def prepare_query(self, query_raw):
        # takes a query (string, in natural language) and apply the same preprocessing steps applied to the dataset
        return preprocess_query_string(query_raw, stem_flag=self.index.skip_stemming,
                                       stop_flag=self.index.allow_stop_words)

    def compute_docs_average(self, docs_count):
        # required for some scoring functions
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

    def query(self, query_string, search_file_algorithms):
        # executes a whole query, starting from natural language and outputting the top k results
        print_log("received query", 3)

        # preprocessing for the query string
        tic = time.perf_counter()
        query_terms = self.prepare_query(query_string)
        print_log("reading each posting lists for: ", 3)
        print_log(query_terms, 3)
        toc = time.perf_counter()
        print("prepare_query created in " + str(toc - tic))

        # access the lexicon, then the index
        tic = time.perf_counter()
        raw_posting_lists = self.fetch_posting_lists(query_terms, search_file_algorithms)
        # raw_posting_lists is a list of strings, where each string is a
        print_log("converting post list to dictionaries", 3)
        toc = time.perf_counter()
        print("fetch_raw_posting_list created in " + str(toc - tic))

        # conjunction/disjunction
        tic = time.perf_counter()

        posting_lists = make_posting_candidates(query_terms, raw_posting_lists)
        toc = time.perf_counter()
        print(" make posting candidates created in " + str(toc - tic))
        print_log("calculating relevance with algorithm: " + self.index.algorithm, 4)
        tic = time.perf_counter()

        related_documents = self.detect_related_documents(posting_lists)
        toc = time.perf_counter()
        print("detect related document, created in " + str(toc - tic))

        # calculating scoring functions for all related documents
        print_log("calculating scores for related documents", 3)
        tic = time.perf_counter()

        scores = self.compute_scoring_function(posting_lists, related_documents, search_file_algorithms)
        toc = time.perf_counter()
        print("compute scoring created in " + str(toc - tic))
        # cache usage: memory holds stats.txt to optimize hdd usage (saves time)
        if flush_doc_size_cache_after_query:
            self.doc_stats_cache = {}

        return get_top_k(self.index.topk, scores)

    def fetch_posting_lists(self, query_terms, search_algorithm):
        '''
        This function needs to:
        read lexicon file
        search query terms
        for each query term:
            read its posting list
            append the posting list to a list of results (strings)
        :param search_algorithm: pick an algorithm from the ones available (see config file)
        :param query_terms: list of words (strings) to search for
        :return:
        @ res : offset (as number of chars) in the index file to find the posting list
        @ doc_freq_array : number of documents that contains the token at least once
        '''
        res = []
        if not self.index.is_ready():
            print_log("Calling Search on unknown index", 0)
            print_log(self.index.index_file_path, 0)
            return []

        # required to open both lexicon and inverted index
        print_log("opening file " + str(self.index.index_file_path), 4)
        if self.index.compression != "no":
            index_file = open(self.index.index_file_path, "rb")
        else:
            index_file = open(self.index.index_file_path, "r+")
        with open(self.index.lexicon_path, 'r+') as f:
            for token in query_terms:  # for each token...

                # cache usage: evaluation is a lot shorter avoiding reading the same things multiple times
                cache_hit = cache_hit_or_miss(self.index.lexicon_path, token)

                if cache_hit == -1:  # cache missed: search it on hdd
                    # search in lexicon for the offsets (start and finish in the inv.index file)
                    res_offset_interval, doc_freq = search_in_lexicon(f, token, search_algorithm)
                    if doc_freq == -1:
                        # token not found in lexicon
                        res.append("")
                    else:
                        # fetch the posting list from inv. index file
                        res_posting_string = search_in_index(index_file, res_offset_interval, self.index.compression)
                        # store the posting list for each word
                        res.append(res_posting_string)

                        # cache update: add the new element
                        cache_push(self.index.lexicon_path, token, doc_freq, res_posting_string)

                else:  # cache hit
                    res_posting_string = cache_get_posting_list(self.index.lexicon_path, cache_hit)
                    res.append(res_posting_string)
        # we stored the docfreq but it's not returned because it's not used really frequently
        # and it's easier to use len(posting_list)
        return res

    def fetch_documents_size(self, keys, search_file_algorithms):
        # get the document size for each docids
        # @ param keys : list of docids (as strings)
        # @ param search_file_algorithms : algorithm to perform search in the documents statistics file
        # @ return : dictionary of {docids : size}

        #    expected row from the collection statistics file:
        #    docid + collection_separator + docno + collection_separator + size + chunk_line_separator
        checkpoint = 0
        results = {}
        last_docid_read = -1
        jobs_counter = 0
        with open(self.index.collection_statistics_path, "r+") as doc_stats_file:
            for docid in keys:
                jobs_counter += 1
                if jobs_counter % 20000 == 0:
                    print_log(f"document sizes left to fetch: {len(keys) - jobs_counter}")
                size = 0
                if docid in self.doc_stats_cache.keys():
                    # cache usage: if the document has already been retrieved, it's in memory already
                    results[docid] = self.doc_stats_cache[docid]
                elif (last_docid_read != -1) and (int(docid) == last_docid_read + 1):
                    # consecutive read: skip the search phase
                    checkpoint, line = next_GEQ_line(doc_stats_file, checkpoint + 1)
                    # line is a string like "docid,docno,doc_size"
                    content = line.strip().split(",")
                    if len(content) == 3:
                        size = content[2].strip()
                    else:
                        print_log("reading empty line in stats.txt", 3)
                else:
                    size, checkpoint = search_in_doc_stats_file(doc_stats_file, int(docid), search_file_algorithms,
                                                                checkpoint)

                if int(size) > 0:
                    last_docid_read = int(docid)
                    results[docid] = int(size)
                    if allow_doc_size_caching:
                        # cache is used when a docid is asked more than once
                        self.doc_stats_cache[docid] = size
        return results

    def detect_related_documents(self, posting_lists):
        # take all the token_keys from the first element
        if len(posting_lists) < 1:
            print_log("no posting list found: ", 3)
            print_log(posting_lists, 3)
            return []
        # initialize the candidates with the keys of the first posting list
        candidates = set(posting_lists[next(iter(posting_lists))].docids)
        print_log(f"first set of candidates: {len(candidates)} elements", 4)
        print_log(candidates, 5)
        for term in posting_lists:  # the first one could be skipped, since it's a copy
            if self.index.algorithm == "conjunctive":
                # intersection
                candidates &= set(posting_lists[term].docids)
                print_log("conjunctive candidates", 5)
                print_log(candidates, 5)
            elif self.index.algorithm == "disjunctive":
                # union
                print_log("disjunctive candidates", 5)
                print_log(candidates, 5)
                for new_token_doc_ids in posting_lists[term].docids:
                    candidates.add(new_token_doc_ids)
            else:
                print_log("CRITICAL ERROR: query algorithm not set", 0)
                return []
        # returns a list of related docids
        print_log(f"related documents: {len(candidates)}", 4)

        return sorted(list(candidates), key=lambda x: int(x))

    def compute_scoring_function(self, posting_lists, related_documents, search_file_algorithms):
        # return the scored list of the relevant documents {docid,score} as a dictionary
        scores = {}
        if not related_documents:
            print_log("Cannot compute scores, no relevant document detected", 1)
            return {}

        # fetch doc size if necessary
        doc_size = {}
        if self.index.scoring in ["BM11", "BM25"]:
            tic = time.perf_counter()

            doc_size = self.fetch_documents_size(related_documents, search_file_algorithms)
            toc = time.perf_counter()
            print("fetch document size in scoring created in " + str(toc - tic))
            # WARNING : we know this might load in memory millions of numbers
            # it's cheaper for our hardware to use the memory than accessing the disk at each number required
            # worst case: load the whole stats.txt file (180mb)
        tic = time.perf_counter()

        for token_key, postingListObj in posting_lists.items():
            # log ( N of docs in the collection / N of relevant docs )

            if postingListObj.size > 0:
                idf = math.log(self.num_docs / len(postingListObj.docids))
                print_log("calculated IDF : " + str(idf), 4)
                tic1 = time.perf_counter()
                index_for_tic1 = 0
                # read posting list, extract doc_ids from posting list
                for i in range(len(postingListObj.docids)):
                    index_for_tic1 += 1
                    if index_for_tic1 % 25000 == 0:
                        toc1 = time.perf_counter()
                        print("25000 doc scored in " + str(toc1 - tic1))
                        tic1 = time.perf_counter()
                    # WARNING: we know that sometimes this is going to loop through millions of scoring functions
                    # it's cheaper for our hardware to store the numbers as a very long list than keeping only the top k
                    # because of the comparison/ordering time

                    if postingListObj.docids[i] not in related_documents:
                        # check if document is flagged as related (at least one occurrence of token_key)
                        scores[postingListObj.docids[i]] = 0
                        print_log("discarded not relevant document : " + str(postingListObj.docids[i]), 5)
                        continue

                    # weigth of the token for the document relevance
                    w_t_d = 0
                    if self.index.scoring == "TFIDF":
                        w_t_d = weight_tfidf(idf, term_freq=postingListObj.freqs[i])
                    elif self.index.scoring == "BM11":
                        doc_len = doc_size[postingListObj.docids[i]]
                        w_t_d = weight_bm11(idf, term_freq=postingListObj.freqs[i], avg=self.doc_len_average,
                                            doc_len=doc_len)
                    elif self.index.scoring == "BM15":
                        w_t_d = weight_bm15(idf, term_freq=postingListObj.freqs[i])
                    elif self.index.scoring == "BM25":
                        doc_len = doc_size[postingListObj.docids[i]]
                        w_t_d = weight_bm25(idf, term_freq=postingListObj.freqs[i], avg=self.doc_len_average,
                                            doc_len=doc_len)
                    if postingListObj.docids[i] in scores.keys():
                        # add the weight to the document's score
                        scores[postingListObj.docids[i]] += w_t_d
                    else:
                        # new relevant document, initialize its score
                        scores[postingListObj.docids[i]] = w_t_d
            toc = time.perf_counter()
            print("token scored in " + str(toc - tic))
            tic = time.perf_counter()
        print_log("scores for related documents:", 3)
        print_log(scores, 3)
        return scores


def make_posting_candidates(tokens, raw_posting_lists):
    # make a dictionary of posting lists {token_key : posting_list_dictionary}
    # where each posting_list_dictionary is a { docid: term_freq}
    res = {}
    # each pl is a string representing a posting list
    for i in range(len(raw_posting_lists)):
        token_key = tokens[i]
        posting_list = create_posting_list_object(token_key, raw_posting_lists[i])
        res[token_key] = posting_list
    return res


def search_in_index(index_file, res_offset, compression):
    # extract one posting list from the index file, given the position (offset = [start,stop])
    offset_start = int(res_offset[0])
    offset_stop = int(res_offset[1])
    nbytes = offset_stop - offset_start
    index_file.seek(offset_start)
    compressed_bytes = index_file.read(nbytes)
    decoded_posting_list_string = decode_posting_list(compressed_bytes, compression)
    return decoded_posting_list_string


def search_in_lexicon(lexicon, token, search_algorithm):
    '''
    open the lexicon file given one query word, and return its entry
    :param lexicon: pointer to the lexicon file
    :param token: query word to look for
    :param search_algorithm: user can choose any one search algorithm implemented
    :return 1: offset interval <start,stop> relative to "token" for the lexicon file
    :return 2: doc frequency: number of documents that contain the token at least once
    '''

    results = []
    last_read_position = 0
    line_pos = -1
    line = ""
    if search_algorithm == "ternary":
        last_line_pos, last_line = get_last_line(lexicon)
        # important: lexicon is in lexicographic order (token must be cast to string)
        line_pos, line = ternary_search(lexicon, start_position=last_read_position, target_key=str(token),
                                        delimiter=element_separator, end_position=last_line_pos,
                                        last_key=get_row_id(last_line, element_separator), last_row=last_line,
                                        key_type="str")
    elif search_algorithm == "skipping":
        step = search_chunk_size_config
        line_pos, high, line = set_search_interval(lexicon, last_read_position, token, element_separator,
                                                   step_size=step, id_is_a_String=True)
        # skipping big chunks when reading lots of files, small chunks when search interval is smaller
        while step >= 1:
            step = step // 10
            line_pos, high, line = set_search_interval(lexicon, line_pos, token, element_separator,
                                                       step_size=step, id_is_a_String=True)
            if token == get_row_id(line, element_separator):
                line_pos = high
                break
        # precision scan
        while token != get_row_id(line, element_separator):
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
    else:
        print_log("Critical error: cannot search without a search algorithm", 0)
        return [], -1

    # search failed: return a blank
    if line_pos == -1:
        print_log("Cannot find token " + str(token) + " in lexicon", 2)
        return [], -1

    # search success: return offset and docfreq
    _, next_line = next_GEQ_line(lexicon, line_pos + 1)

    _, docfreq, start_offset = read_lexicon_line(line)
    _, _, stop_offset = read_lexicon_line(next_line)

    return [start_offset, stop_offset], docfreq


def search_in_doc_stats_file(doc_stats_file, docid, search_algorithm, last_checkpoint=0):
    '''
    open the doc stats file given one query word, and return its entry
    :param doc_stats_file: pointer to the doc stats file
    :param docid: docid to look for
    :param search_algorithm: user can choose any one search algorithm implemented
    :param last_checkpoint: skip some content at the beginning of the file if i know it doesnt contain the docid
    :return: size of the document, position of the cursor in the document
    '''

    results = []
    last_read_position = last_checkpoint
    line_pos = -1
    line = ""

    if search_algorithm == "ternary":
        last_line_pos, last_line = get_last_line(doc_stats_file)
        # important: stats is in cardinal order (token must be cast to integer)
        line_pos, line = ternary_search(doc_stats_file, start_position=last_read_position, target_key=int(docid),
                                        delimiter=collection_separator, end_position=last_line_pos,
                                        last_key=get_row_id(last_line, collection_separator), last_row=last_line,
                                        key_type="int")
    elif search_algorithm == "skipping":
        step = search_chunk_size_config
        line_pos, high, line = set_search_interval(doc_stats_file, last_read_position, docid, collection_separator,
                                                   step_size=step, id_is_a_String=False)
        # skipping big chunks when reading lots of files, small chunks when search interval is smaller
        while step >= 1:
            step = step // 10
            line_pos, high, line = set_search_interval(doc_stats_file, line_pos, docid, collection_separator,
                                                       step_size=step, id_is_a_String=False)
            if docid == get_row_id(line, collection_separator):
                line_pos = high
                break
        # precision scan
        while docid != get_row_id(line, collection_separator):
            line_pos, line = next_GEQ_line(doc_stats_file, doc_stats_file.tell())
            if doc_stats_file.tell() > high:
                # termination condition: nothing found
                line_pos = -1
                break

        if line_pos != -1:
            # i have found the correct word
            results.append(line.strip())
            # since i enforced the query_terms list to be sorted, there is no need to look the same lines again
            last_read_position = line_pos
    else:
        print_log("Critical error: cannot search without a search algorithm", 0)
        return 0, last_checkpoint

    # search failed: return a blank
    if line_pos == -1:
        print_log("Cannot find " + str(docid) + " in doc stats file", 2)
        return 0, last_checkpoint

    # search success: return offset and docfreq

    doc_len = line.split(",")[2].strip()

    return doc_len, line_pos


def read_lexicon_line(line):
    # lexicon line structure:
    # token_id;doc_freq;offset_in_index
    content = line.split(element_separator)
    return content[0], content[1], content[2]


def preprocess_query_string(query_raw, stem_flag, stop_flag):
    # check the preprocessing necessary in index
    words = preprocess_text(query_raw, skip_stemming=stem_flag, allow_stop_words=stop_flag)
    return words


def create_posting_list_object(token_key, posting_string):
    # convert a line from the index file to a dictionary with the useful info
    posting_list_obj = PostingList(token_key)
    if posting_string != '':
        # posting string: d,d,d,d,d,d,d f,f,f,f,f,f,f
        doc_id_list = posting_string.split()[0].split(",")
        freq_list = posting_string.split()[1].split(",")
        posting_list_obj.set_docids(doc_id_list)
        posting_list_obj.set_freqs(freq_list)
    # return : postingList class object
    return posting_list_obj


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
    if doc_len == 0:
        return 0  # this should not happen (trying to score a document not present)
    return idf * term_freq / (term_freq + BM_k_one * (1 - BM25_b + BM25_b * (doc_len / avg)))


def weight_bm15(idf, term_freq):
    # calculated like BM25 with BM25_b = 0
    return idf * term_freq / (term_freq * BM_k_one)


def weight_bm11(idf, term_freq, avg, doc_len):
    # calculated like BM25 with BM25_b = 1
    if term_freq == 0:
        return 0
    if doc_len == 0:
        return 0  # this should not happen (trying to score a document not present)
    return idf * term_freq / (term_freq + BM_k_one * (doc_len / avg))
