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