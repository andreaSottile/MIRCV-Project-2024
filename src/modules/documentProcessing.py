"""
Document Processing.
Note that the data may contain text from many different languages,
including text that uses non-ASCII character sets. You must read the
data using UNICODE instead of basic ASCII. Also, the data may be provided in
a fairly raw form with various errors (empty page, malformed lines, malformed
characters) that you must to be able to to deal with. You must remove the punctuation
signs.

Compressed Reading. The document collection should be uncompressed during parsing,
by loading one compressed file into memory and then calling the right library
function to uncompress it into another memory-based buffer.

Stemming & Stopword Removal.
Stemming and stopword removal should be implemented.
For stemming, you can use any third-party library providing the Porter
stemming algorithm or similar. For stopword removal, you can use any english
stopwords list available on the Web. Ideally, your program should have a compile
flag that allows you to enalbe/disble stemming & stopword removal.
"""
import pandas as pandas
from src.config import collection_path_config


def fetch_data_row_from_collection(row_index):
    # @param row_index : the id of the document in the collection
    return fetch_n_data_rows_from_collection(row_index, 1 + row_index)[0]


def fetch_n_data_rows_from_collection(start_row, stop_row):
    # @param start_row : start reading the collection from here (avoid reading all the collection)
    # @param end_row : stop reading after n rows (avoid reading all the collection)
    n_rows = stop_row - start_row
    print("fetching " + str(n_rows) + " from collection")
    res_dataframe = pandas.read_csv(collection_path_config, sep='\t', header=None, skiprows=start_row, nrows=n_rows)
    return res_dataframe.values
