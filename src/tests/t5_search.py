from src.config import *
import os

from src.modules.queryHandler import search_in_file
from src.modules.utils import get_last_line

file_target = r"C:\Users\andre\Desktop\AIDE\mircv\indexdefault_index\index.txt"

# NEW PARAMETER
search_chunk_size_config = 10000  # char size for a search chunk
search_algorithms = ["ternary", "skipping"]


def test_search(words_list, algorithm):
    global file_target
    file_size = 12320

    delimiter = ":"

    print(" --  Searching with " + algorithm + "  -- ")

    print(words_list)

    res = search_in_file(file_target, file_size, words_list, delimiter, algorithm)

    print(" -- Query completed -- ")
    print(res)
















print("get last line: ")
with open(file_target, 'rb') as f:
    print(get_last_line(f))

print("testing ternary search")
print("test query: between companies")
test_search(["between", "companies"], search_algorithms[0])
print("test query: third figur")
test_search(["final" ,"third", "figur"], search_algorithms[0])

print("testing skipping search")
print("test query: between companies")
test_search(["between", "companies"], search_algorithms[1])
print("test query: third figur")
test_search(["third", "figur"], search_algorithms[1])
