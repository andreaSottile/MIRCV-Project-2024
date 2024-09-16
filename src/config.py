"""
IMPORTANT: THIS FILE IS NOT UPDATED BY GIT
VARIABLES TAGGED WITH "EDIT HERE" REQUIRE MANUAL CONFIGURATION ON EACH MACHINE
"""
''' LOCAL PATHS '''
# EDIT HERE
full_collection_compressed = "D:/Repositories/mircv/dataset/collection.tar.gz"
# full_collection_compressed = "/Users/gianluca.cometa/Documents/mircv_dataset/collection.tar.gz"
# full_collection_compressed = "C:/Users/andre/Downloads/collection.tar.gz"

# EDIT HERE
test_collection_uncompressed = "D:/Repositories/mircv/dataset/test.tsv"
# test_collection_uncompressed = "D:/Repositories/mircv/dataset/test.tsv"

# EDIT HERE
full_collection_uncompressed = "D:/Repositories/mircv/dataset/collection.tsv"
# full_collection_uncompressed = "/Users/gianluca.cometa/Documents/mircv_dataset/collection.tsv"
# full_collection_uncompressed = "C:/Users/andre/Downloads/collection.tsv"

'''collection_path : this is the LOCAL path to the dataset.'''
# EDIT HERE
collection_path_config = full_collection_compressed

'''index folder: this folder is going to contain all the files to work with inverted indexes'''
# EDIT HERE
index_config_path = "D:/Repositories/mircv/dataset/index_info_"  # file name is going to be added at the end
# index_config_path = r"C:\Users\andre\Desktop\AIDE\mircv\index_info_"  # file name is going to be added at the end
# index_config_path = "/Users/gianluca.cometa/Documents/mircv_dataset/index_info_"  # file name is going to be added at the end
# EDIT HERE
index_folder_path = "D:/Repositories/mircv/dataset/index"  # folder is going to contain several files for each index
# index_folder_path = "/Users/gianluca.cometa/Documents/mircv_dataset/index"  # folder is going to contain several files for each index
# index_folder_path = r"C:\Users\andre\Desktop\AIDE\mircv\index"  # folder is going to contain several files for each index

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
scoring_function_config = ["BM11", "BM15", "BM25", "TFIDF"]
'''
Query Execution. 
Your program must execute ranked queries. Your program must
return the top 10 or 20 results according to the TFIDF scoring function. You must
implement conjunctive and disjunctive query processing algorithms.
Scoring Function. You should implement the BM25 or other scoring functions such as
those based on language models.
'''
'''
Evalution 
Your program must include an evaluation of the systems using a standard
collection such as the TREC DL 2019 queries and TREC DL 2019 qrels, or the
TREC DL 2020 queries and TREC DL 2020 qrels.
'''
# EDIT HERE
# qrel 2019
evaluation_trec_qrel_2019_path = "D:/Repositories/mircv/dataset/trec/2019qrels-pass.txt"
# evaluation_trec_qrel_2019_path = r"C:\Users\andre\Desktop\AIDE\mircv\2019qrels-pass.txt"

# EDIT HERE
# qrel 2020
evaluation_trec_qrel_2020_path = "D:/Repositories/mircv/dataset/trec/2020qrels-pass.txt"
# evaluation_trec_qrel_2020_path = r"C:\Users\andre\Desktop\AIDE\mircv\2020qrels-pass.txt"

# EDIT HERE
# queries 2019
evaluation_trec_queries_2019_path = "D:/Repositories/mircv/dataset/trec/msmarco-test2019-queries.tsv"
# evaluation_trec_queries_2019_path = r"C:\Users\andre\Desktop\AIDE\mircv\msmarco-test2019-queries.tsv"

# EDIT HERE
# queries 2020
evaluation_trec_queries_2020_path = "D:/Repositories/mircv/dataset/trec/msmarco-test2020-queries.tsv"
# evaluation_trec_queries_2020_path = r"C:\Users\andre\Desktop\AIDE\mircv\msmarco-test2020-queries.tsv"

"""
STRING FORMAT CONFIG: list of useful variables to easily remember string separators
"""
file_format = ".txt"
file_blank_tag = "missing"
member_blank_tag = "not set"
collection_separator = ","
posting_separator = ":"
element_separator = ";"
docid_separator = "|"
chunk_line_separator = " \n"

"""Debug Verbosity"""
''' 
Raising the verbosity level modify the verbosity of the python console during the execution
This function is used to properly document and debug the whole project.
'''
# EDIT HERE
verbosity_config = 2

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
limit_input_rows_config = 0

'''
CHUNK SIZE FOR INDEXING
instead of writing index files frequently, add them in memory and write only when the buffer is full.
size is counted in number of lines (one line is one posting list).
DISABLE CHUNK SPLIT: if this variable is -1, there is no splitting in chunks and all the data is loaded into memory 
'''
# EDIT HERE
index_chunk_size = -1

'''
BM 25 PARAMETERS
k_one in [1.2,2]
B is usually 0.75
'''
# EDIT HERE
BM_k_one = 1.2
BM25_b = 0.75

'''
COMPRESSION OPTIONS 
'''
compression_choices_config = ["no", "unary", "gamma"]
'''
QUERY PARAMETERS
query_step_size_config : how many rows the query is supposed to read at once
'''
# options for search algoritms
search_into_file_algorithms = ["ternary", "skipping"]
# other parameters
# EDIT HERE
query_step_size_config = 500
# EDIT HERE
search_chunk_size_config = 10000  # char size for a search chunk
