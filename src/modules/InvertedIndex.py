"""
Inverted Index.
You must create an inverted index structure, plus structures for the
lexicon (to store the vocabulary terms and their information) and for the document
table (to store docid to docno mapping and document lengths). You must not load the
posting data into a relational database. At the end of the program,
all structures must be stored on disk in some suitable format.

Index Compression.
You should implement at least some basic integer compression
method to decrease the index size. Do not use standard compressors such as
gzip for index compression. Ideally, your program should have a compile flag
that allows you to use ASCII format during debugging and binary format for
performance.
"""
from src.config import *
import os

from src.modules.compression import to_unary, to_gamma, bit_stream_to_bytes
from src.modules.document_processing import open_dataset
from src.modules.preprocessing import preprocess_text, count_token_occurrences
from src.modules.utils import readline_with_strip, print_log

posting_buffer = []  # memory buffer
posting_file_list = []  # list of file names


# REMINDER STRUCTURE LEXICON
# token ; docFreq; offset

class InvertedIndex:
    def __init__(self):
        self.name = member_blank_tag
        self.skip_stemming = True
        self.allow_stop_words = True
        self.compression = "no"
        self.content = []
        self.topk = 0
        self.algorithm = member_blank_tag
        self.scoring = member_blank_tag
        self.collection_statistics_path = file_blank_tag
        self.index_file_path = file_blank_tag
        self.lexicon_path = file_blank_tag
        self.config_path = file_blank_tag
        self.num_doc = 0
        self.index_len = 0
        self.lexicon_len = 0
        print_log("created new index", priority=2)

    def rename(self, name):
        # each index has a name (unique string) decided by the user
        if name == member_blank_tag:
            print_log("forbidden name. please pick a different one")
            return
        old = self.name
        self.name = str(name)
        print_log("index \'" + str(old) + "\' renamed as \'" + str(self.name) + "\'", priority=3)

    def is_ready(self):
        if self.name != member_blank_tag:
            if self.collection_statistics_path != file_blank_tag:
                if self.index_file_path != file_blank_tag:
                    if self.lexicon_path != file_blank_tag:
                        if self.config_path != file_blank_tag:
                            return True
        return False

    def delete_from_disk(self):
        # delete function that manage safe remove
        if self.collection_statistics_path != file_blank_tag:
            os.remove(self.collection_statistics_path)
            self.collection_statistics_path = file_blank_tag
        if self.index_file_path != file_blank_tag:
            os.remove(self.index_file_path)
            self.index_file_path = file_blank_tag
        if self.lexicon_path != file_blank_tag:
            os.remove(self.lexicon_path)
            self.lexicon_path = file_blank_tag
        if self.config_path != file_blank_tag:
            os.remove(self.config_path)
            self.config_path = file_blank_tag

    def save_on_disk(self):
        # index config and options are saved on disk, generating a filename based on its name
        if self.name == member_blank_tag:
            print_log("cannot save index config without a name", priority=2)
            return
        if self.config_path == file_blank_tag:
            print_log("saving " + str(self.name) + " for the first time", priority=4)
            self.config_path = index_config_path + str(self.name) + file_format
        with open(self.config_path, mode="w") as config_file:
            # the index config file is overwritten each time it's modified
            print_log("saving index config", priority=1)
            config_file.write(self.name + chunk_line_separator)
            if self.skip_stemming:
                config_file.write("skip_stem" + chunk_line_separator)
            else:
                config_file.write("do_stem" + chunk_line_separator)
            if self.allow_stop_words:
                config_file.write("allow_sw" + chunk_line_separator)
            else:
                config_file.write("remove_sw" + chunk_line_separator)
            config_file.write(self.compression + chunk_line_separator)

            config_file.write(self.content_to_str() + chunk_line_separator)
            config_file.write(str(self.topk) + chunk_line_separator)
            config_file.write(str(self.algorithm) + chunk_line_separator)
            config_file.write(str(self.scoring) + chunk_line_separator)

            self.collection_statistics_path = index_folder_path + self.name + "/" + "stats" + file_format
            self.index_file_path = index_folder_path + self.name + "/" + "index" + file_format
            self.lexicon_path = index_folder_path + self.name + "/" + "lexicon" + file_format

            config_file.write(self.collection_statistics_path + chunk_line_separator)
            config_file.write(self.index_file_path + chunk_line_separator)
            config_file.write(self.lexicon_path + chunk_line_separator)

            config_file.write(str(self.num_doc) + chunk_line_separator)
            config_file.write(str(self.index_len) + chunk_line_separator)
            config_file.write(str(self.lexicon_len) + chunk_line_separator)
            print_log("index config saved successfully", priority=3)

    def reload_from_disk(self):
        # function called to load a previously created index, by reading a config file from HD
        # returns True if the loading was successful
        print_log("loading index config file", priority=3)
        if self.config_path == file_blank_tag:
            # config file path not set yet
            if self.name == member_blank_tag:
                # cannot retrieve a config file without its name
                print_log("cannot load index config for index with no name", priority=1)
                return False
            else:
                # in the first execution, self.name has been set, but self.config_path has not (yet)
                print_log("config file set", priority=3)
                self.config_path = index_config_path + str(self.name) + file_format
        # reading from disk
        if os.path.exists(self.config_path):
            with open(self.config_path, mode="r") as config_file:
                if config_file:
                    # the order of the member variables MUST be the same of the save_on_disk function
                    local_name = readline_with_strip(config_file)
                    if str(self.name) == local_name:
                        print_log("index " + str(self.name) + " is about to be loaded", priority=2)
                        self.skip_stemming = (readline_with_strip(config_file) == "skip_stem")
                        self.allow_stop_words = (readline_with_strip(config_file) == "allow_sw")
                        self.compression = readline_with_strip(config_file)

                        self.content_reinit(readline_with_strip(config_file))
                        self.topk = int(readline_with_strip(config_file))
                        self.algorithm = readline_with_strip(config_file)
                        self.scoring = readline_with_strip(config_file)

                        self.collection_statistics_path = readline_with_strip(config_file)
                        self.index_file_path = readline_with_strip(config_file)
                        self.lexicon_path = readline_with_strip(config_file)

                        self.num_doc = int(readline_with_strip(config_file))
                        self.index_len = int(readline_with_strip(config_file))
                        self.lexicon_len = int(readline_with_strip(config_file))
                        print_log("index " + str(self.name) + " loaded successfully", priority=1)
                    else:
                        # this should never happen
                        print_log("conflict loading " + str(self.name) + " from file named " + local_name, priority=0)
                else:
                    # file not found
                    print_log("ERROR: cannot find index config " + str(self.name), priority=0)
                    return False
            print_log("closing config file", priority=4)
            return True
        else:
            print_log("ERROR: cannot find index config " + str(self.name), priority=0)
            return False

    def content_to_str(self):
        # content is a list made of (x,y) couples, representing that each document between x and y are included in
        # the index. this function converts it to a string to make it easier to save it on disk
        line = ""
        for element in self.content:
            if line != "":
                line += element_separator
            line += str(element[0]) + collection_separator + str(element[1])
        if line == "":
            return "empty"

        return line

    def content_reinit(self, content_string):
        # content is a list made of (x,y) couples, representing that each document between x and y are included in
        # the index. this function takes a string (read from disk) and decode it
        self.content = []
        pairs = content_string.split(element_separator)
        if content_string == "empty":
            self.content = []
        else:
            for pair in pairs:
                delimiters = pair.split(collection_separator)
                self.content.append([int(delimiters[0]), int(delimiters[1])])

    def content_flush(self):
        # remove all docids marked as read
        self.content = []

    def content_check(self, docid):
        # check if the index contains the specified docid
        # @ param docid : id (row number of the original dataset)
        # @ return : True if the index contains docid, False if the docid is not in the content range
        if docid < 0:
            docid = 123  # a random number, we are just checking if there is at least one line
        for interval in self.content:
            if int(interval[0]) <= docid <= int(interval[1]):
                return True
        else:
            return False

    def add_content_id(self, docno):
        # content is a list of couples, each couple is a pair <low,high> of docids that have already been processed
        # returns True if @param docid is duplicate
        # return False if new docid has been added
        out_of_interval = True
        docid = int(docno)
        for interval in self.content:
            if interval[0] <= docid <= interval[1]:
                return True  # mark as duplicate
            elif docid == interval[1] + 1:
                interval[1] += 1
                out_of_interval = False
            elif docid == interval[0] - 1:
                interval[0] -= 1
                out_of_interval = False
        # after checking all intervals, i might have not found where to add the docid yet
        if out_of_interval:
            # out of known intervals, add a new interval
            self.content.append([docid, docid])
        return False  # mark as new element

    def flush_collection_stats(self):
        # clean all the content of the content statistics file
        if self.collection_statistics_path != file_blank_tag:
            os.remove(self.collection_statistics_path)
            print_log("flushed collection for " + self.name, priority=1)

    def add_to_collection_stats(self, docid, docno, stats=0):
        # create stats file with docid, docno, wordcount
        if self.collection_statistics_path == file_blank_tag:
            print_log("CRITICAL ERROR: unable to access collection statistics for " + self.name, priority=0)
            return
        with open(self.collection_statistics_path, mode="a") as collection:
            collection.write(str(docid) + collection_separator + str(docno) + collection_separator + str(
                stats) + chunk_line_separator)
            self.num_doc += 1

    def create_posting_chunk(self, filename):
        # write a chunk of posting lists to disk
        # @ param filename: output file path (complete with file format)
        global posting_buffer
        # posting_buffer is a list where each element have this structure:
        #     [token_id, [docid, token_count]]

        # MANDATORY: every chunk must be ordinated
        posting_buffer_sorted = sorted(posting_buffer, key=lambda x: x[0])
        chunk_name = index_folder_path + self.name + "/" + filename
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
        return chunk_name

    def scan_dataset(self, limit_row_size=-1, delete_chunks=False, delete_after_compression=False):
        # main functions that handle the chunk files, merge and provide the inverted index file
        global posting_buffer
        global posting_file_list
        print_log("starting dataset scan", priority=1)

        posting_buffer = []  # memory buffer
        posting_file_list = []  # list of file names

        print_log("scan limited to " + str(limit_row_size) + " rows", priority=4)
        open_dataset(limit_row_size, self, add_document_to_index)
        print_log("dataset scan completed", priority=3)

        if len(posting_file_list) > 0:
            # the last chunk is not full, but it's still important to write a file
            close_chunk(self)

            lines = merge_chunks(posting_file_list, self.index_file_path, self.lexicon_path,
                                 compression=self.compression,
                                 delete_after_merge=delete_chunks)
        else:
            # there is only one chunk, either for the size too big, the file count too small, or chunk splitting is
            # disabled
            lines = write_output_files(self.index_file_path, self.lexicon_path,
                                       compression=self.compression)
        self.index_len += lines

        self.lexicon_len += lines
        print_log("merged all chunks", priority=1)

        if delete_after_compression:
            print_log("deleted uncompressed index file", priority=1)
            for filename in os.listdir(index_folder_path + self.name):
                # look for files with name starting with "chunk"
                if filename.startswith("chunk"):
                    file_path = os.path.join(index_folder_path + self.name, filename)
                    try:
                        # delete file
                        os.remove(file_path)
                        print(f"File deleted: {file_path}")
                    except Exception as e:
                        print(f"Error deleting {file_path}: {e}")
        self.save_on_disk()


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

        # Convert the bit stream into bytes
        compressed_bytes = bit_stream_to_bytes(bit_stream)
        return compressed_bytes
    else:
        posting_string = ",".join(list(map(str, gap_list))) + " " + ",".join(list_freq_sorted) + chunk_line_separator
        return posting_string


def load_from_disk(name):
    # function used to initialise a new invertedIndex by loading it from disk.
    # @ param name: user is required to remember the name of the file on disk
    # important: name is not the path, it's just the file name
    index = InvertedIndex()
    index.rename(name)
    if index.reload_from_disk():
        return index
    else:
        print_log("cannot load index from disk: Index not found on disk", 1)
        return None


def index_setup(name, stemming_flag, stop_words_flag, compression_flag, k, join_algorithm, scoring_f):
    # function used to setup the inverted index class with relative options and save on disk.
    print_log("setup for new index", 1)
    ind = InvertedIndex()
    print_log("created index", 1)
    ind.rename(name)  # string
    print_log("setting flags", 1)
    ind.skip_stemming = stemming_flag  # boolean
    ind.allow_stop_words = stop_words_flag  # boolean
    ind.compression = compression_flag  # string, see config
    ind.topk = k  # number of results to return
    ind.algorithm = join_algorithm  # conjunctive or disjunctive
    ind.scoring = scoring_f  # string, see config
    print_log("setup completed, saving to disk", 3)
    ind.save_on_disk()
    print_log("saved complete", 1)
    print_log(ind.config_path, 4)
    print_log(ind.index_file_path, 4)
    print_log(ind.collection_statistics_path, 4)
    print_log(ind.lexicon_path, 4)

    return ind


def add_posting_list(token_id, token_count, docid):
    # add new entries in posting list.
    global posting_buffer
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
        return False
    if len(posting_buffer) > index_chunk_size:
        print_log("posting memory buffer is full", priority=4)
        return True  # chunk is big, time to write it on disk
    else:
        return False  # no need to write it on disk yet


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


def merge_chunks(file_list, index_file_path, lexicon_file_path, compression="no", delete_after_merge=True):
    written_lines = 0
    reader_list = []  # list of pointers to files
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


def close_chunk(index):
    global posting_file_list
    new_chunk_post = index.create_posting_chunk("chunk_posting_" + str(len(posting_file_list)) + file_format)
    posting_file_list.append(new_chunk_post)
    print_log("chunks created: ", 5)
    print_log(posting_file_list, 5)


def add_document_to_index(index, args):
    # this function is called for each document (row) in the collection
    global posting_file_list  # list of file names
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
        full = add_posting_list(token_id, token_count, docid)
        if full:  # this check is always false without using the chunk splitting
            # write chunk in a file, and clean the memory buffer to move on
            close_chunk(index)

    '''
     Possible improvement: disaster recovery
     Calling the "save_on_disk" function after each update could be a good idea to save the progress in the scan_dataset procedure. 
     if properly handled, it could be a way to split the execution flow in more instances, like a crash of the code or a 
     multithreading environment. in order to make it work, it is necessary to remove the lists of file names, and make 
     the "merge" function infer the files itself. it could work with os.listdir, making a regex on the naming pattern 
     we have used to rebuild lexicon_file_list and posting_file_list.
    '''
