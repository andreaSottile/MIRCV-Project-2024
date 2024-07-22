"""
IMPORTANT: THIS FILE IS NOT UPDATED BY GIT
VARIABLES TAGGED WITH "EDIT HERE" REQUIRE MANUAL CONFIGURATION ON EACH MACHINE
"""
''' LOCAL PATHS '''
# path lorenzo
# EDIT HERE
full_collection_compressed = "D:/Repositories/mircv/dataset/collection.tar.gz"
# EDIT HERE
test_collection_uncompressed = "D:/Repositories/mircv/dataset/test.tsv"
# EDIT HERE
full_collection_uncompressed = "D:/Repositories/mircv/dataset/collection.tsv"

'''collection_path : this is the LOCAL path to the dataset.'''
# EDIT HERE
collection_path_config = full_collection_compressed

'''index folder: this folder is going to contain all the files to work with inverted indexes'''
# EDIT HERE
index_config_path = "D:/Repositories/mircv/dataset/index_info_"  # file name is going to be added at the end
# EDIT HERE
index_folder_path = "D:/Repositories/mircv/dataset/index/"  # folder is going to contain several files for each index

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
# options available for k
k_returned_results_config = [10, 20]
# options available for query processing
query_processing_algorithm_config = ["conjunctive", "disjunctive"]
# options available for scoring function
scoring_function_config = ["BM11", "BM25", "TFIDF"]
'''
Query Execution. 
Your program must execute ranked queries. Your program must
return the top 10 or 20 results according to the TFIDF scoring function. You must
implement conjunctive and disjunctive query processing algorithms.
Scoring Function. You should implement the BM25 or other scoring functions such as
those based on language models.
'''
# options available for evaluation methods
evaluation_method_config = ["trec_dl_2019_queries", "trec_dl_2019_qrels", "trec_dl_2020_queries", "trec_dl_2020_qrels"]
'''
Evalution 
Your program must include an evaluation of the systems using a standard
collection such as the TREC DL 2019 queries and TREC DL 2019 qrels, or the
TREC DL 2020 queries and TREC DL 2020 qrels.
'''

"""Debug Verbosity"""
''' 
Raising the verbosity level modify the verbosity of the python console during the execution
This function is used to properly document and debug the whole project.
'''
# EDIT HERE
verbosity_config = 5


# 0 : only errors
# 1 : processes and modules
# 2 : critical blocks
# 3 : function calls
# 4 : row by row (debug)
# 5 : loop iterations
def print_log(msg, priority=0):
    global verbosity_config
    if priority <= verbosity_config:
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

# EDIT HERE
limit_input_rows_config = 100

'''
CHUNK SIZE FOR INDEXING
instead of writing index files frequently, add them in memory and write only when the buffer is full
size is counted in number of read documents
'''
# EDIT HERE
index_chunk_size = 20
