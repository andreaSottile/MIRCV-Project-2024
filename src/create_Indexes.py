import time
import os
import io
import tarfile

import pandas as pd
from src.config import scoring_function_config, query_processing_algorithm_config, search_into_file_algorithms
from src.modules.InvertedIndex import index_setup, load_from_disk
from src.modules.queryHandler import QueryHandler
import re
import nltk
from nltk.stem import PorterStemmer
from nltk.corpus import stopwords
from src.modules.utils import print_log
from collections import Counter
from src.config import *
from multiprocessing import Process
from src.modules.utils import get_last_line, ternary_search, get_row_id, print_log, set_search_interval, next_GEQ_line

stop_words = None
stemmer = None

names = ['America', 'Europe', 'Africa']
procs = []
'''
# instantiating process with arguments
for name in names:
    # print(name)
    proc = Process(target=print_func, args=(name,))
    procs.append(proc)
    proc.start()

# complete the processes
for proc in procs:
    proc.join()
'''

def extract_dataset_from_tar(path):
    print_log("Opening tar.gz file", priority=2)
    tar = tarfile.open(path, "r:gz")
    dataset_compressed = tar.getmember("collection.tsv")
    print_log("tar.gz uncompression: starting", priority=4)
    dataset_raw = tar.extractfile(dataset_compressed)
    print_log("tar.gz uncompression: finished", priority=4)
    return io.TextIOWrapper(dataset_raw, encoding='utf-8')


def read_portion_of_dataset(dataset, flags, start_subindex_pos, end_subindex_pos, process_function, delete_chunks,
                            delete_after_compression):
    read_rows = 0
    posting_buffer = []  # memory buffer
    posting_file_list = []  # list of file names
    index_name = f"indt_multiproc_stem{flags[4]}_stopword{flags[5]}_{start_subindex_pos}_{end_subindex_pos}"
    test_index_element = load_from_disk(index_name)
    if test_index_element is None:
        test_index_element = index_setup(index_name, stemming_flag=flags[4], stop_words_flag=flags[5],
                                         compression_flag="no",
                                         k=flags[3], join_algorithm=flags[1], scoring_f=flags[2])
    if test_index_element.is_ready():
        if not test_index_element.content_check(int(flags[0] / 2)):

            # lexicon_buffer = []  # memory buffer
            # lexicon_file_list = []  # list of file names

            print_log("scan limited to " + str(flags[0]) + " rows", priority=4)
            while True:
                line = dataset.readline()
                if line:
                    print_log("read progress: " + str(read_rows), priority=5)
                    if 0 < flags[0] <= read_rows:
                        break
                    content = line.strip().split("\t")
                    if len(content) == 2:
                        posting_buffer, posting_file_list = process_dataset_row(read_rows, content[0], content[1],
                                                                                posting_buffer, posting_file_list,
                                                                                process_function, test_index_element)
                    else:
                        print_log("invalid line len at row " + str(read_rows), priority=4)

                    read_rows += 1
                else:
                    break

            print_log("dataset scan completed", priority=3)

            if len(posting_file_list) > 0:
                # the last chunk is not full, but it's still important to write a file
                posting_buffer, posting_file_list = close_chunk(test_index_element)

                lines = merge_chunks(posting_file_list, test_index_element.index_file_path,
                                     test_index_element.lexicon_path,
                                     compression=test_index_element.compression,
                                     delete_after_merge=delete_chunks)
            else:
                # there is only one chunk, either for the size too big, the file count too small, or chunk splitting is
                # disabled
                lines = write_output_files(test_index_element.index_file_path, test_index_element.lexicon_path,
                                           compression=test_index_element.compression)
            test_index_element.index_len += lines

            test_index_element.lexicon_len += lines
            print_log("merged all chunks", priority=1)

            if delete_after_compression:
                print_log("deleted uncompressed index file", priority=1)
                for filename in os.listdir(index_folder_path + test_index_element.name):
                    # look for files with name starting with "chunk"
                    if filename.startswith("chunk"):
                        file_path = os.path.join(index_folder_path + test_index_element.name, filename)
                        try:
                            # delete file
                            os.remove(file_path)
                            print(f"File deleted: {file_path}")
                        except Exception as e:
                            print(f"Error deleting {file_path}: {e}")
            test_index_element.save_on_disk()
    else:
        print("index not ready")
    print_log("read finished", priority=4)


def open_dataset_multiprocess(flag, process_function, delete_chunks, delete_after_compression):
    # reset row counter
    global procs
    partitions_number = 8
    print_log("opening dataset file", priority=3)
    interval_sub_index = [0]
    if collection_path_config.endswith(".gz"):
        # working with compressed file, required uncompression
        dataset = extract_dataset_from_tar(collection_path_config)
        dataset_size = 3061567853
        subindex_size = int(dataset_size / partitions_number)
        for index in range(partitions_number):
            pos, _ = next_GEQ_line(dataset, index * subindex_size + subindex_size)
            interval_sub_index.append([pos])
        for i in range(partitions_number):
            proc = Process(target=read_portion_of_dataset, args=(
            dataset[interval_sub_index[i]:interval_sub_index[i + 1]], flag, interval_sub_index[i],
            interval_sub_index[i + 1], process_function, delete_chunks, delete_after_compression,))
            procs.append(proc)
            proc.start()


def process_dataset_row(d_id, d_no, d_text, posting_buffer, posting_file_list, process_function=None, index=None):
    if d_id % 100000 == 0:
        print_log("processed " + str(d_id), priority=1)
    else:
        if d_id % 10000 == 0:
            print_log("processed " + str(d_id), priority=2)
    if d_no:
        print_log("processing row " + str(d_id), priority=5)
        if d_text:
            if index is not None and process_function is not None:
                # index structure is created outside
                posting_buffer, posting_file_list = process_function(index, [d_id, d_no, d_text], posting_buffer,
                                                                     posting_file_list)
            else:
                # this function is just a print if not associated with an index
                print(d_text)
        else:  # invalid doc text
            print_log("no text found for docid " + str(d_id), priority=3)
    else:  # invalid doc id
        print_log("found invalid docid near row " + str(d_id), priority=3)
    return posting_buffer, posting_file_list


def close_chunk(index, posting_buffer, posting_file_list):
    new_chunk_post, posting_buffer, posting_file_list = create_posting_chunk(index, "chunk_posting_" + str(
        len(posting_file_list)) + file_format, posting_buffer, posting_file_list)
    posting_file_list.append(new_chunk_post)
    print_log("chunks created: ", 5)
    print_log(posting_file_list, 5)
    return posting_buffer, posting_file_list


def create_posting_chunk(index, filename, posting_buffer, posting_file_list):
    # write a chunk of posting lists to disk
    # @ param filename: output file path (complete with file format)
    # posting_buffer is a list where each element have this structure:
    #     [token_id, [docid, token_count]]

    # MANDATORY: every chunk must be ordinated
    posting_buffer_sorted = sorted(posting_buffer, key=lambda x: x[0])
    chunk_name = index_folder_path + index.name + "/" + filename
    try:
        with open(chunk_name, "w") as file:
            for row in posting_buffer_sorted:
                row_string = str(row[0]) + posting_separator
                for pair in row[1:]:
                    element = str(pair[0]) + docid_separator + str(pair[1])
                    if pair == row[-1]:
                        row_string += element
                    else:
                        row_string += element + element_separator
                file.write(row_string + chunk_line_separator)
        posting_buffer = []
    except IOError:
        print(IOError)
        print_log("Writing new posting file chunk to file. dumping chunk here: ", 5)
        print_log(posting_buffer_sorted, 5)
    return chunk_name, posting_buffer, posting_file_list


def clean_text(text):
    # remove punctuation signs (keep only letters and numbers)
    pattern = r'[^a-zA-Z0-9\s]'
    cleaned_text = re.sub(pattern, ' ', text)

    # Replace multiple spaces with a single space
    cleaned_text = re.sub(r'\s+', ' ', cleaned_text)
    # Strip leading and trailing spaces and convert to lower case
    cleaned_text = cleaned_text.lower().strip()
    return cleaned_text


def tokenizer(text):
    # transform a text into a list of tokens (words)
    tokens = []
    for word in text.split(" "):
        tokens.append(word)
    return tokens


def remove_stopwords(tokens):
    # clean a list of tokens using an external list of stop words (language dependant)
    global stop_words
    if stop_words is None:
        stop_words = set(stopwords.words('english'))
        if not stop_words:
            nltk.download('stopwords')
            stop_words = set(stopwords.words('english'))
    cleaned = []
    for word in tokens:
        if word not in stop_words:
            cleaned.append(word)
    return cleaned


def stemming(tokens):
    # perform stemming on a list of tokens
    global stemmer
    if stemmer is None:
        stemmer = PorterStemmer()
    stems = []
    for word in tokens:
        stems.append(stemmer.stem(word))
    return stems


def preprocess_text(text, skip_stemming=True, allow_stop_words=True):
    # execute all preprocessing steps, as required by flags
    clean = clean_text(text)
    tokens = tokenizer(clean)
    if not allow_stop_words:
        tokens = remove_stopwords(tokens)
    if not skip_stemming:
        tokens = stemming(tokens)
    return tokens


def add_document_to_index(index, posting_buffer, posting_file_list, args):
    # this function is called for each document (row) in the collection
    # global lexicon_file_list  # list of file names
    if len(args) != 3:
        print_log("CRITICAL ERROR: missing arguments to add document to index : " + str(args), 0)
        return
    if not os.path.exists(index_folder_path + index.name):
        os.mkdir(index_folder_path + index.name)
        print_log("created new directory for index files", priority=3)
    docid, docno, doctext = args[0], args[1], args[2]

    if not index.is_ready():
        print_log("cannot add document with uninitialized index", priority=0)

    is_duplicate = index.add_content_id(int(docno))
    if is_duplicate:
        print_log("duplicate document " + str(docno), priority=4)
        return
    print_log("adding row " + str(docid) + " as document " + str(docno) + " to index " + str(index.name), priority=5)

    tokens = preprocess_text(doctext, index.skip_stemming, index.allow_stop_words)
    token_counts = count_token_occurrences(tokens).items()

    # document length to be saved in collection statistics
    word_count = len(tokens)
    index.add_to_collection_stats(docid, docno, word_count)
    for token_id, token_count in token_counts:
        # inverted index is based on posting lists
        full, posting_buffer, posting_file_list = add_posting_list(token_id, token_count, docid, posting_buffer,
                                                                   posting_file_list)
        if full:  # this check is always false without using the chunk splitting
            # write chunk in a file, and clean the memory buffer to move on
            posting_buffer, posting_file_list = close_chunk(index, posting_buffer, posting_file_list)
    return posting_buffer, posting_file_list

    '''
     Possible improvement: disaster recovery
     Calling the "save_on_disk" function after each update could be a good idea to save the progress in the scan_dataset procedure. 
     if properly handled, it could be a way to split the execution flow in more instances, like a crash of the code or a 
     multithreading environment. in order to make it work, it is necessary to remove the lists of file names, and make 
     the "merge" function infer the files itself. it could work with os.listdir, making a regex on the naming pattern 
     we have used to rebuild lexicon_file_list and posting_file_list.
    '''


def add_posting_list(token_id, token_count, docid, posting_buffer, posting_file_list):
    # add new entries in posting list.
    # print_log("adding new posting list: " + str(token_id), priority=5)
    stats = [docid, token_count]
    found_token = False
    found_doc = False
    for posting_list in posting_buffer:
        if posting_list[0] == token_id:
            found_token = True
            for pair in posting_list[1:]:
                if pair[0] == docid:
                    found_doc = True
                    pair[1] += token_count
                    break
            if not found_doc:
                posting_list.append(stats)
            break
    if not found_token:
        posting_buffer.append([token_id, stats])
    if index_chunk_size < 1:  # no limit on the chunk size
        return False, posting_buffer, posting_file_list
    if len(posting_buffer) > index_chunk_size:
        print_log("posting memory buffer is full", priority=4)
        return True, posting_buffer, posting_file_list  # chunk is big, time to write it on disk
    else:
        return False, posting_buffer, posting_file_list  # no need to write it on disk yet


def merge_chunks(file_list, index_file_path, lexicon_file_path, compression="no", delete_after_merge=True):
    written_lines = 0
    reader_list = []  # list of pointers to files
    doc_list = []
    occurrence_list = []
    first_element_list = []  # list of the next (lowest) element taken from each file
    for file in file_list:
        reader = open(file, "r+")
        reader_list.append(reader)
        first_element_list.append(reader.readline())

    if os.path.exists(index_file_path):
        # delete the file if any previous duplicate was present
        os.remove(index_file_path)

    if os.path.exists(lexicon_file_path):
        # delete the file if any previous duplicate was present
        os.remove(lexicon_file_path)
    if compression != "no":
        index_file = open(index_file_path, "wb+")
    else:
        index_file = open(index_file_path, "w+")
    lexicon_file = open(lexicon_file_path, "w+")

    while True:
        next_chunk_index = []
        # i have to compare every element of firstelemlist
        i = 0
        output_row = ""  # list of elements for the posting list
        # example of output row :: ["0|1" , "1|2"]
        output_key = ""  # token as string
        # look at each first element, search the lowest, write its index in next_index
        for element in first_element_list:
            # make sure the list is not empty
            if element != "empty":
                element = element.replace(" \n", "")
                element_splitted = element.split(sep=posting_separator)
                # "lowest" is in alphabetical order
                if output_key == "" or element_splitted[0] < output_key:
                    output_key, output_row = element_splitted[0], element_splitted[1].split(element_separator)
                    next_chunk_index = [i]
                elif element_splitted[0] == output_key:
                    # example of "element_splitted[i]: token1:docid1|count;docid2|count;................docidN|count
                    for ep in element_splitted[1].split(element_separator):
                        output_row.append(ep)
                    output_row = sorted(output_row,
                                        key=lambda x: x.split(docid_separator)[0])
                    next_chunk_index.append(i)

            i += 1
        if len(next_chunk_index) > 0:
            # move the list pointer to the next element
            for index in next_chunk_index:
                first_element_list[index] = reader_list[index].readline()
                if len(first_element_list[index]) == 0:
                    first_element_list[index] = "empty"

            # OUTPUT: writing the index file
            line = ""
            doc_list = []
            occurrence_list = []
            for e in output_row:
                elem = e.split("|")
                doc_list.append(elem[0])
                occurrence_list.append(elem[1])
            posting_offset = index_file.tell()
            index_file.write(make_posting_list(doc_list, occurrence_list, compression))
            written_lines += 1
            lexicon_file.write(output_key + element_separator + str(len(doc_list)) + element_separator + str(
                posting_offset) + chunk_line_separator)
        else:
            # all lists are empty
            break

    print_log("Chunks merge finished for ", 1)
    for file in reader_list:
        file.close()
    if delete_after_merge:
        print_log("Deleting chunks after merge", 2)
        for file_name in file_list:
            os.remove(file_name)
    index_file.close()
    lexicon_file.close()
    return written_lines


def make_posting_list(list_doc_id, list_freq, compression="no"):
    # Step 1: Encode the number of doc IDs
    combined = list(zip(list_doc_id, list_freq))
    combined.sort(key=lambda x: int(x[0]))

    # split combined list
    list_doc_id_sorted, list_freq_sorted = zip(*combined)

    # convert them into lists
    list_doc_id_sorted = list(list_doc_id_sorted)
    list_freq_sorted = list(list_freq_sorted)
    gap_list = []
    previous_doc_id = 0
    for doc_id in list_doc_id_sorted:
        gap_list.append(int(doc_id) - previous_doc_id)
        previous_doc_id = int(doc_id)
    if compression != "no":
        # Step 2: Encode the doc IDs using gap encoding
        if compression == "unary":
            encoded_gap_list = [to_unary(gap) for gap in gap_list]
            encoded_freq_list = [to_unary(int(freq)) for freq in list_freq_sorted]
        elif compression == "gamma":
            encoded_gap_list = [to_gamma(gap) for gap in gap_list]
            encoded_freq_list = [to_gamma(int(freq)) for freq in list_freq_sorted]
        else:
            raise ValueError(f"Unsupported encoding type: {compression}")

        # Combine the bit streams: number of doc IDs, doc IDs, and frequencies
        bit_stream = ''.join(encoded_gap_list) + ''.join(encoded_freq_list)

        # return bit_stream

        # Convert the bit stream into bytes
        compressed_bytes = bit_stream_to_bytes(bit_stream)
        return compressed_bytes
    else:
        posting_string = ",".join(list(map(str, gap_list))) + " " + ",".join(list_freq_sorted) + chunk_line_separator
        return posting_string


def bit_stream_to_bytes(bit_stream):
    # Pad the bit stream so that its length is a multiple of 8
    padding_length = (8 - len(bit_stream) % 8) % 8
    bit_stream = bit_stream + '1' * padding_length

    # Convert each 8-bit chunk into a byte
    byte_array = bytearray()
    for i in range(0, len(bit_stream), 8):
        byte_chunk = bit_stream[i:i + 8]
        byte_array.append(int(byte_chunk, 2))

    return bytes(byte_array)


def to_unary(n):
    # Unary encoding: n-1 ones followed by a final zero
    return '1' * (n - 1) + '0'


def to_gamma(n):
    # Compute the binary representation of n
    binary_repr = bin(n)[2:]  # Binary representation without the '0b' prefix

    # First part: Unary encoding of the length of the binary representation
    length = len(binary_repr)
    unary_length = to_unary(length)

    # Second part: Offset, i.e., the binary representation without the most significant bit
    offset = binary_repr[1:]

    # Combine the two parts
    return unary_length + offset


def count_token_occurrences(tokens):
    return Counter(tokens)


def write_output_files(index_file_path, lexicon_path, compression="no"):
    global posting_buffer
    written_lines = 0

    # MANDATORY: every chunk must be ordinated
    posting_buffer_sorted = sorted(posting_buffer, key=lambda x: x[0])

    with open(index_file_path, "w") as index_file:
        with open(lexicon_path, "w") as lexicon_file:
            for element in posting_buffer_sorted:
                token = str(element[0])
                occurrence_list = []
                doc_list = []
                for doc, f in element[1:]:
                    doc_list.append(str(doc))
                    occurrence_list.append(str(f))
                posting_offset = index_file.tell()
                index_file.write(make_posting_list(doc_list, occurrence_list, compression))
                written_lines += 1
                lexicon_file.write(token + element_separator + str(len(doc_list)) + element_separator + str(
                    posting_offset) + chunk_line_separator)

    return written_lines


# disjunctive test
config = [-1, "query_processing_algorithm_config[1]", "scoring_function_config[0]", 4, True, True, "No"]

open_dataset_multiprocess(config, "add_document_to_index", delete_chunks=False, delete_after_compression=False)
