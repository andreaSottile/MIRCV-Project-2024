"""
IMPORTANT: THIS FILE IS NOT UPDATED BY GIT
IT REQUIRES MANUAL CONFIGURATION ON EACH MACHINE
"""
''' LOCAL PATHS '''
# path lorenzo
full_collection_compressed = "D:/Repositories/mircv/dataset/collection.tar.gz"
test_collection_uncompressed = "D:/Repositories/mircv/dataset/test.tsv"

'''collection_path : this is the LOCAL path to the dataset.'''
collection_path_config = full_collection_compressed
'''
Dataset Specification. You must use the MSMARCO Passages collection1 available on
this page: https://microsoft.github.io/msmarco/TREC-Deep-Learning-2020. Scroll
down the page until you come to a section titled “Passage ranking dataset”, and
download the first link in the table, collection.tar.gz. Note that this is a collection
of 8.8M documents, also called passages, about 2.2GB in size. The uncompressed
file contains one document per line. Each line has the following format:
<pid>\t<text>\n
where <pid> is the docno and <text> is the document content. These 8.8M documents
must be the minimum data set you should be able to index.
'''

k_returned_results_config = [10, 20]
query_processing_algorithm_config = ["conjunctive", "disjunctive"]
scoring_function_config = ["BM11", "BM25", "TFIDF"]
'''
Query Execution. 
Your program must execute ranked queries. Your program must
return the top 10 or 20 results according to the TFIDF scoring function. You must
implement conjunctive and disjunctive query processing algorithms.
Scoring Function. You should implement the BM25 or other scoring functions such as
those based on language models.
'''
evaluation_method_config = ["trec_dl_2019_queries", "trec_dl_2019_qrels", "trec_dl_2020_queries", "trec_dl_2020_qrels"]
'''
Evalution 
Your program must include an evaluation of the systems using a standard
collection such as the TREC DL 2019 queries and TREC DL 2019 qrels, or the
TREC DL 2020 queries and TREC DL 2020 qrels.
'''

"""Debug Verbosity"""
''' Raising the verbosity level modify the verbosity of the python console during the execution'''
verbosity_config = 5


# 0 : only errors
# 1 : processes and modules
# 2 : critical blocks
# 3 : function calls
# 4 : row by row (debug)
# 5 : loop iterations
def print_log(msg, priority=0):
    global verbosity_config
    if priority >= verbosity_config:
        print(msg)


'''
TEST MODE WITH LIMITED ROWS
Pick a maximum number of lines to be read from dataset. 
If limit is zero, then all the rows are going to  be read (no limit)
'''
# execution time: (compressed file)
# 1000 rows -> 12 s
# 3800 rows -> 18 s
# 100000 rows -> 47 s
limit_input_rows_config = 100000

'''
CHUNK SIZE CONFIG
split the input file in smaller chunks, to avoid processing it at once.
chunk size is the number of rows in each chunk
'''
chunk_size_config = 5
