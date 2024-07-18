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
import io

import pandas as pd
from pandas import DataFrame

from src.config import collection_path_config, print_log, limit_input_rows_config, chunk_size_config
import tarfile


def open_dataset():
    # avoid pandas truncation
    pd.set_option('display.max_colwidth', None)
    # reset row counter
    read_rows = 0
    print_log("opening dataset file", priority=3)
    if tarfile.is_tarfile(collection_path_config):
        # working with compressed file, required uncompression
        print_log("Opening tar.gz file", priority=2)

        tar = tarfile.open(collection_path_config, "r:gz")

        for member in tar.getmembers():
            if member.isfile() and member.name.endswith(".tsv"):
                print_log("tar.gz uncompression: starting", priority=4)
                dataset_raw = tar.extractfile(member)
                # convert uncompressed stream into filelike object in memory
                buffer = io.TextIOWrapper(dataset_raw, encoding='utf-8')

                print_log("tar.gz uncompression: finished", priority=4)

                read_rows += ingest_dataset(buffer)

    elif collection_path_config.endswith(".tsv"):
        # working with .tsv file
        print_log("Opening .tsv file", priority=2)

        read_rows += ingest_dataset(collection_path_config)

    print_log("input phase done, returning to parsing", priority=3)
    return "rows read from dataset: " + str(read_rows)


def ingest_dataset(ds_path):
    row_counter = 0
    print_log("reading " + str(ds_path), priority=3)
    for chunk in split_tsv_chunks(ds_path, chunk_size_config):
        if 0 < limit_input_rows_config <= row_counter:
            break
        process_dataset_chunk(chunk)
        print_log("read progress: " + str(row_counter), priority=5)
        row_counter += len(chunk)
    print_log("read finished", priority=4)
    # returns number of read rows, to check dataset size
    return row_counter


def split_tsv_chunks(file_path, chunk_size=1):
    rows_progress = 0
    excluded = []
    while True:
        try:
            chunk = pd.read_csv(file_path, sep='\t', header=None, skiprows=rows_progress,
                                nrows=chunk_size, encoding='utf-8', engine='python')
            if chunk.empty:
                break
            print_log("Extracting chunk " + str(rows_progress / chunk_size), priority=5)

            rows_progress += len(chunk)
            yield chunk
        except pd.errors.EmptyDataError:
            # Break the loop if there are no more data to read
            break
        except pd.errors.ParserError:
            print_log("error on parsing near row " + str(rows_progress), priority=3)
            excluded.append(rows_progress)
            continue
        except UnicodeDecodeError:
            # without the ioWrapper, this error used to happen frequently
            print_log("unreadable char found near row " + str(rows_progress), priority=3)
            excluded.append(rows_progress)
            continue
    print(len(excluded))
    print(excluded)


def process_dataset_chunk(chunk):
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
        print_log("found invalid chunk format", priority=3)


def process_dataset_row(d_id, d_text):
    if d_id:
        print(d_id)
        if d_text:
            print(d_text)
        else:  # invalid doc text
            print_log("no text found for docid " + str(d_id), priority=3)
    else:  # invalid doc id
        print_log("found invalid docid", priority=3)


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
