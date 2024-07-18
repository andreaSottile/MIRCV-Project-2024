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
import csv

import pandas as pd
from pandas import DataFrame
import csv
import pandas as pd
from pandas import DataFrame
import tarfile
import io

from src.config import collection_path_config, print_log, limit_input_rows_config, chunk_size_config
import tarfile

read_rows = 0


def open_dataset():
    global read_rows
    # avoid pandas truncation
    pd.set_option('display.max_colwidth', None)
    # reset row counter
    read_rows = 0
    print_log("opening dataset file", priority=3)
    if collection_path_config.endswith(".gz"):
        # working with compressed file, required uncompression
        print_log("Opening tar.gz file", priority=2)

        tar = tarfile.open(collection_path_config, "r:gz")
        for member in tar.getmembers():
            print_log("tar.gz uncompression: starting", priority=4)
            dataset_raw = tar.extractfile(member)
            print_log("tar.gz uncompression: finished", priority=4)

            for chunk in split_tsv_chunks(dataset_raw, chunk_size_config):
                print_log("read progress: " + str(read_rows), priority=5)
                if 0 < limit_input_rows_config <= read_rows:
                    break
                process_dataset_chunk(chunk)
                #read_rows += chunk_size_config

            print_log("read finished", priority=4)

    if collection_path_config.endswith(".tsv"):
        # working with .tsv file
        print_log("Opening .tsv file", priority=2)

        for chunk in split_tsv_chunks(collection_path_config, chunk_size_config):
            print_log("read progress: " + str(read_rows), priority=5)
            if 0 < limit_input_rows_config <= read_rows:
                break
            process_dataset_chunk(chunk)
            #read_rows += chunk_size_config

        print_log("read finished", priority=4)

    print_log("input phase done, returning to parsing", priority=3)
    return "rows read from dataset: " + str(read_rows)


def split_tsv_chunks(file, chunk_size=1, is_stream=True):
    global read_rows
    excluded = []
    buffer = []

    if is_stream:
        file = io.TextIOWrapper(file, encoding='utf-8')

    while True:
        try:
            line = file.readline()
            if not line:
                # Se il buffer contiene ancora righe alla fine del file, restituisci l'ultimo chunk
                if buffer:
                    chunk = pd.DataFrame(buffer)
                    yield chunk
                break

            buffer.append(line.strip().split('\t'))
            if len(buffer) >= chunk_size:
                chunk = pd.DataFrame(buffer)
                yield chunk
                buffer = []  # Svuota il buffer per il prossimo chunk

        except Exception as e:
            print_log(f"error on parsing near row {read_rows}: {str(e)}", priority=3)
            excluded.append(read_rows)
            continue

        read_rows += 1  # Incrementa il contatore delle righe lette

    print(len(excluded))
    print(excluded)


def split_tsv_chunks2(file_path, chunk_size=1):
    global read_rows
    excluded = []
    while True:
        try:
            chunk = pd.read_csv(file_path, sep='\t', header=None, skiprows=read_rows,
                                nrows=chunk_size, encoding='utf-8', engine='python')
            if chunk.empty:
                break
            yield chunk
        except pd.errors.EmptyDataError:
            # Break the loop if there are no more data to read
            break
        except pd.errors.ParserError:
            print_log("error on parsing near row " + str(read_rows), priority=3)
            excluded.append(read_rows)
            continue
        except UnicodeDecodeError:
            print_log("unreadable char found near row " + str(read_rows), priority=3)
            excluded.append(read_rows)
            continue
    print(len(excluded))
    print(excluded)


def process_dataset_chunk(chunk):
    global read_rows
    if type(chunk) is DataFrame:
        # check that each row has two elements <id,text>
        if chunk.values.shape[1] == 2:
            for doc_id, doc_text in chunk.values:
                process_dataset_row(doc_id, doc_text)
        else:
            print_log("input error, invalid chunk \n == dumping invalid row ==", priority=3)
            print_log(str(chunk), priority=3)
            print_log("== end of dump ==", priority=3)
    else:
        print_log("invalid chunk format near line " + str(read_rows), priority=3)


def process_dataset_row(d_id, d_text):
    global read_rows
    if d_id:
        print(d_id)
        if d_text:
            print(d_text)
        else:  # invalid doc text
            print_log("no text found for docid " + str(d_id), priority=3)
    else:  # invalid doc id
        print_log("found invalid docid near row " + str(read_rows), priority=3)


def fetch_data_row_from_collection(row_index):
    # @param row_index : the id of the document in the collection
    return fetch_n_data_rows_from_collection(row_index, 1 + row_index)[0]


def fetch_n_data_rows_from_collection(start_row, stop_row):
    # @param start_row : start reading the collection from here (avoid reading all the collection)
    # @param end_row : stop reading after n rows (avoid reading all the collection)
    n_rows = stop_row - start_row
    print("fetching " + str(n_rows) + " from collection")
    res_dataframe = pd.read_csv(collection_path_config, sep='\t', header=None, skiprows=start_row, nrows=n_rows)
    return res_dataframe.values
