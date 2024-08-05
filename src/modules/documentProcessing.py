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
import pandas as pd
import io
from src.config import collection_path_config, print_log, limit_input_rows_config
import tarfile


def open_dataset(count_limit=-1, index=None, process_function=None):
    if count_limit > 0 and 0 < limit_input_rows_config < count_limit:
        count_limit = limit_input_rows_config
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
            dataset_wrapped = io.TextIOWrapper(dataset_raw, encoding='utf-8')

            while True:
                line = dataset_wrapped.readline()
                if line:
                    print_log("read progress: " + str(read_rows), priority=5)
                    if 0 < count_limit <= read_rows:
                        break
                    content = line.strip().split("\t")
                    if len(content) == 2:
                        process_dataset_row(read_rows, content[0], content[1], process_function, index)
                    else:
                        print_log("invalid line len at row " + str(read_rows), priority=4)

                    read_rows += 1
                else:
                    break
            print_log("read finished", priority=4)

    elif collection_path_config.endswith(".tsv"):
        # working with .tsv file
        print_log("Opening .tsv file", priority=2)

        dataset = pd.read_csv(collection_path_config, sep='\t', header=None)
        for line in dataset.values:
            if 0 < count_limit <= read_rows:
                break
            print(line)
            if len(line) == 2:
                process_dataset_row(read_rows, line[0], line[1], process_function, index)
            else:
                print_log("invalid line len at row " + str(read_rows), priority=4)

            read_rows += 1
        print_log("read finished", priority=4)

    print_log("input phase done, returning to parsing", priority=3)
    return "rows read from dataset: " + str(read_rows)


def process_dataset_row(d_id, d_no, d_text, process_function=None, index=None):
    if d_no:
        print_log("processing row " + str(d_id), priority=5)
        if d_text:
            if index is not None and process_function is not None:
                # index structure is created outside
                process_function(index, [d_id, d_no, d_text])
            else:
                # this function is just a print if not associated with an index
                print(d_text)
        else:  # invalid doc text
            print_log("no text found for docid " + str(d_id), priority=3)
    else:  # invalid doc id
        print_log("found invalid docid near row " + str(d_id), priority=3)


def fetch_from_collection(requests):
    # generic function to retrieve data from the dataset
    # @ param requests : list of docids to retrieve from the dataset
    # @ returns two lists of docids,texts
    results_ids = []
    results_txt = []
    intervals = []
    print_log("fetching: ", 4)
    print_log(requests, 4)
    # transform each requested element into a 1-long interval
    for req in requests:
        if req < 0:
            print_log("Invalid docid fetch request", 1)
        else:
            intervals.append([req, req])

    # merge consecutive intervals
    for i in range(len(intervals)):
        if i == 0:
            continue
        if intervals[i][0] == intervals[i - 1][1]:
            # merge
            intervals[i - 1][1] = intervals[i][1]
            intervals.remove(intervals[i])
            i -= 1
    print_log("required intervals: ", 4)
    print_log(intervals, 4)

    # access the dataset
    for interval in intervals:
        if interval[0] == interval[1]:
            doc_id, doc_txt = fetch_data_row_from_collection(interval[0])
            results_ids.append(doc_id)
            results_txt.append(doc_txt)
        else:
            doc_id, doc_txt = fetch_n_data_rows_from_collection(interval[0], interval[1])
            results_ids.append(doc_id)
            results_txt.append(doc_txt)

    print_log("fetch completed: n of documents found = " + str(len(results_ids)), 3)
    print_log(results_ids, 3)
    return results_ids, results_txt


def fetch_data_row_from_collection(row_index):
    # retrieve one single row from the dataset, given the docid
    # @param row_index : the id of the document in the collection
    return fetch_n_data_rows_from_collection(row_index, 1 + row_index)


def fetch_n_data_rows_from_collection(start_row, stop_row):
    # fetch an interval of CONSECUTIVE rows from the dataset
    # @param start_row : start reading the collection from here (avoid reading all the collection)
    # @param end_row : stop reading after n rows (avoid reading all the collection)
    n_rows = stop_row - start_row
    result_id = []
    result_txt = []
    target_row = []
    for n in range(n_rows):
        target_row.append(n + start_row)
    print_log("fetching " + str(n_rows) + " from collection", priority=3)
    if collection_path_config.endswith(".gz"):
        # working with compressed file, required uncompression
        print_log("Opening tar.gz file", priority=2)

        tar = tarfile.open(collection_path_config, "r:gz")
        for member in tar.getmembers():
            print_log("tar.gz uncompression: starting", priority=4)
            dataset_raw = tar.extractfile(member)
            print_log("tar.gz uncompression: finished", priority=4)

            dataset = pd.read_csv(dataset_raw, sep='\t', header=None, skiprows=start_row, nrows=n_rows,
                                  encoding="utf-8", engine="python")

            for target in target_row:
                raw_result = dataset.loc[dataset[0] == target].values
                raw_result = raw_result[0]
                print(raw_result)
                if len(raw_result) == 2:
                    print_log("reading line " + str(target), priority=5)
                    result_id.append(raw_result[0])
                    result_txt.append(raw_result[1])
                else:
                    print_log("cannot find line " + str(target), priority=4)

    elif collection_path_config.endswith(".tsv"):
        dataset = pd.read_csv(collection_path_config, sep='\t', header=None, skiprows=start_row, nrows=n_rows,
                              encoding="utf-8", engine="python")

        for target in target_row:
            raw_result = dataset.loc[dataset[0] == target].values
            raw_result = raw_result[0]
            print(raw_result)
            if len(raw_result) == 2:
                print_log("reading line " + str(target), priority=5)
                result_id.append(raw_result[0])
                result_txt.append(raw_result[1])
            else:
                print_log("cannot find line " + str(target), priority=4)
    print_log("read finished", priority=4)
    return result_id, result_txt
