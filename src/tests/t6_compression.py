from src.config import *
import os
import time
from src.modules.InvertedIndex import index_setup, load_from_disk
from src.modules.compression import decode_posting_list, decode_posting_list_old


#docid = ['1','2','3','4','5','6','7','8','9','15','30','60']
#listfreq = ['1','4','5','6','3','7','9','40','80','90','50','40']
#token = "a"
#test = make_posting_list(docid,listfreq,encoding_type="unary")
#print("Test: ")
#print(test)
def test_index(index_name, flags):
    tic = time.perf_counter()
    test_index_element = load_from_disk(index_name)
    if test_index_element is None:
        test_index_element = index_setup(index_name, stemming_flag=flags[4], stop_words_flag=flags[5],
                                         compression_flag=flags[6],
                                         k=flags[3], algorithm=flags[1], scoring_f=flags[2], eval_f=0)
    if test_index_element.is_ready():
        if not test_index_element.content_check(int(flags[0] / 2)):
            test_index_element.scan_dataset(flags[0], delete_after_compression=False)
    else:
        print("index not ready")
    toc = time.perf_counter()
    return tic, toc

config = [300, query_processing_algorithm_config[0], scoring_function_config[0], 6, True, True, "gamma"]
test_index("indt4", config)

# Example usage: Decoding from a binary file
if config[6] != "no":
    with open("C:/Users/andre/Desktop/AIDE/mircv/indexindt4/index.txt", "rb") as index_file:
        #index_file.seek(7802)
        #compressed_bytes = index_file.read()
        #decoded_doc_ids, decoded_freq = decode_posting_list(compressed_bytes, config[6], encoding_type="unary")
        #print(decoded_doc_ids)
        nbytes = 1276 - 1249
        index_file.seek(1249)
        compressed_bytes = index_file.read(nbytes)
        decoded_doc_ids_opt, decoded_freq_opt = decode_posting_list(compressed_bytes, config[6])
        print(decoded_doc_ids_opt)
else:
    with open("C:/Users/andre/Desktop/AIDE/mircv/indexindt4/index.txt", "r+") as index_file:
        index_file.seek(0)
        compressed_bytes = index_file.readline()
        decoded_doc_ids, decoded_freq = decode_posting_list(compressed_bytes, config[6])
        print(decoded_doc_ids)

#print(decode_unary(test))

#print(decode_posting_list(test))

