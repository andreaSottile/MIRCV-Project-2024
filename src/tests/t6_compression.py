from src.config import *
import os

from src.modules.InvertedIndex import make_posting_list
from src.modules.compression import decode_unary, decode_posting_list

docid = {1,2,3,4,5,6,7,8,9,15,30,60}
listfreq = {1,4,5,6,3,7,9,40,80,90}
token = "a"
test = make_posting_list(token,docid,listfreq,encoding_type="unary")
print("Test: ")
print(test)

print(decode_unary(test))

print(decode_posting_list(test))

