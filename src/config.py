"""
IMPORTANT: THIS FILE IS NOT UPDATED BY GIT
IT REQUIRES MANUAL CONFIGURATION ON EACH MACHINE
"""

'''collection_path : this is the LOCAL path to the dataset.'''
collection_path_config = "D:/Repositories/mircv/dataset/collection.tsv"  # lorenzo

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
