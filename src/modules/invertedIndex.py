'''
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
'''
from src.config import print_log, index_folder_path, index_config_path, index_chunk_size
import os

from src.modules.compression import compress_index
from src.modules.documentProcessing import open_dataset
from src.modules.preprocessing import preprocess_text, count_token_occurrences
from src.modules.searchResult import searchResult

file_format = ".txt"
file_blank_tag = "missing"
member_blank_tag = "not set"
collection_separator = ","
posting_separator = ":"
element_separator = ";"
docid_separator = "|"
lexicon_buffer = []
posting_buffer = []


class invertedIndex:
    def __init__(self):
        self.name = member_blank_tag
        self.skip_stemming = True
        self.allow_stop_words = True
        self.compression = False
        self.content = []
        self.topk = 0
        self.algorithm = member_blank_tag
        self.scoring = member_blank_tag
        self.evaluation = member_blank_tag
        self.collection_statistics_path = file_blank_tag
        self.index_file_path = file_blank_tag
        self.lexicon_path = file_blank_tag
        self.config_path = file_blank_tag
        print_log("created new index", priority=2)

    def rename(self, name):
        # each index has a name (unique string) decided by the user
        if name == member_blank_tag:
            print_log("forbidden name. please pick a different one")
            return
        old = self.name
        self.name = str(name)
        print_log("index " + str(old) + " renamed as " + str(name), priority=3)

    def is_ready(self):
        if self.name != member_blank_tag and self.collection_statistics_path != file_blank_tag and \
                self.index_file_path != file_blank_tag and self.lexicon_path != file_blank_tag and \
                self.config_path != file_blank_tag:
            return True
        return False

    def save_on_disk(self):
        global file_format
        # index config and options are saved on disk, generating a filename based on its name
        if self.config_path == file_blank_tag:
            print_log("cannot save index config without a name", priority=2)
            return
        with open(self.config_path, mode="w") as config_file:
            # the index config file is overwritten each time it's modified
            print_log("saving index config", priority=1)
            config_file.write(self.name)
            if self.skip_stemming:
                config_file.write("skip_stem")
            else:
                config_file.write("do_stem")
            if self.allow_stop_words:
                config_file.write("allow_sw")
            else:
                config_file.write("remove_sw")
            if self.compression:
                config_file.write("compress")
            else:
                config_file.write("uncompressed")
            config_file.write(self.content_to_str())
            config_file.write(str(self.topk))
            config_file.write(self.algorithm)
            config_file.write(self.scoring)
            config_file.write(self.evaluation)

            config_file.write(os.path.join(index_config_path, "stats" + file_format))
            self.collection_statistics_path = os.path.join(index_config_path, "stats" + file_format)
            config_file.write(os.path.join(index_config_path, "index" + file_format))
            self.index_file_path = (index_folder_path + "index" + file_format)
            config_file.write(os.path.join(index_config_path, "lexicon" + file_format))
            self.lexicon_path = os.path.join(index_config_path, "lexicon" + file_format)

            self.config_path = index_config_path + str(self.name) + file_format
            print_log("index config saved successfully", priority=3)

    def load_from_disk(self, name):
        # function used to initialise a new invertedIndex by loading it from disk.
        # @ param name: user is required to remember the name of the file on disk
        self.rename(name)
        self.reload_from_disk()

    def reload_from_disk(self):
        # function called to load a previously created index, by reading a config file
        print_log("loading index config file", priority=3)
        if self.config_path == file_blank_tag:
            # config file path not set yet
            if self.name == member_blank_tag:
                # cannot retrieve a config file without its name
                print_log("cannot load index config for index with no name", priority=1)
                return
            else:
                # in the first execution, self.name has been set, but self.config_path has not (yet)
                print_log("config file set", priority=3)
                self.config_path = index_config_path + str(self.name) + file_format
        # reading from disk
        with open(self.config_path, mode="r") as config_file:
            if config_file:
                # the order of the member variables MUST be the same of the save_on_disk function
                local_name = str(config_file.readline())
                if str(self.name) == local_name:
                    print_log("index " + str(self.name) + " is about to be loaded", priority=2)
                    self.skip_stemming = (config_file.readline() == "skip_stem")
                    self.allow_stop_words = (config_file.readline() == "allow_sw")
                    self.compression = (config_file.readline() == "compress")

                    self.content_reinit(config_file.readline())
                    self.topk = int(config_file.readline())
                    self.algorithm = config_file.readline()
                    self.scoring = config_file.readline()
                    self.evaluation = config_file.readline()

                    self.collection_statistics_path = config_file.readline()
                    self.index_file_path = config_file.readline()
                    self.lexicon_path = config_file.readline()
                    print_log("index " + str(self.name) + " loaded successfully", priority=1)
                else:
                    print_log("conflict loading " + str(self.name) + " from file named " + local_name, priority=0)
            else:
                # file not found
                print_log("cannot find index config " + str(self.name), priority=0)
                return
        print_log("closing config file", priority=4)

    def content_to_str(self):
        # content is a list made of (x,y) couples, representing that each document between x and y are included in
        # the index. this function converts it to a string to make it easier to save it on disk
        line = ""
        for element in self.content:
            if line != "":
                line += element_separator
            line += str(element[0]) + collection_separator + str(element[1])
        return line

    def content_reinit(self, content_string):
        # content is a list made of (x,y) couples, representing that each document between x and y are included in
        # the index. this function takes a string (read from disk) and decode it
        self.content = []
        pairs = content_string.split(element_separator)
        for pair in pairs:
            delimiters = pair.split(collection_separator)
            self.content.append([delimiters[0], delimiters[1]])

    def content_flush(self):
        # remove all docids marked as read
        self.content = []

    def add_content_id(self, docid):
        # content is a list of couples, each couple is a pair <low,high> of docids that have already been processed
        # returns True if @param docid is duplicate
        # return False if new docid has been added
        out_of_interval = True
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
        if self.collection_statistics_path == file_blank_tag:
            print_log("CRITICAL ERROR: unable to access collection statistics for " + self.name, priority=0)
            return
        with open(self.collection_statistics_path, mode="a") as collection:
            collection.write(str(docid) + collection_separator + str(docno) + collection_separator + str(stats))

    def update_posting_list(self, filename):
        # write a chunk of posting lists to disk
        global posting_buffer
        # posting_buffer is a list where each element have this structure:
        #     [token_id, [docid, token_count]]
        posting_buffer_sorted = sorted(posting_buffer, key=lambda x: x[0])
        chunk_name = os.path.join(index_folder_path, self.name + filename)
        with open(chunk_name, "w") as file:
            for row in posting_buffer_sorted:
                row_string = str(row[0]) + posting_separator
                for pair in row[1:]:
                    element = docid_separator.join(pair)
                    if pair == row[-1]:
                        row_string += element
                    else:
                        row_string += element + element_separator
                file.write(row_string)
        posting_buffer = []
        return chunk_name

    def update_to_lexicon(self, filename):
        # write a chunk of lexicon words to disk
        global lexicon_buffer
        # lexicon_buffer is a list where each element have this structure:
        #     [token_id, token_count]
        lexicon_buffer_sorted = sorted(lexicon_buffer, key=lambda x: x[0])
        chunk_name = os.path.join(index_folder_path, self.name + filename)
        with open(chunk_name, "w") as file:
            for row in lexicon_buffer_sorted:
                file.write(element_separator.join(row))
        lexicon_buffer = []
        return chunk_name

    def scan_dataset(self, limit_row_size=-1, delete_after_compression=False):
        print_log("starting dataset scan", priority=1)
        print_log("scan limited to " + str(limit_row_size) + " rows", priority=4)
        open_dataset(limit_row_size, self)
        print_log("dataset scan completed", priority=3)
        if self.compression:
            print_log("compressing index file", priority=1)
            compress_index(self.name, self.index_file_path, self.lexicon_path)
            print_log("compression finished", priority=3)
            if delete_after_compression:
                print_log("deleted uncompressed index file", priority=1)
                os.remove(self.index_file_path)

    def query(self, query_string):
        res = searchResult(self.topk)
        # TODO
        if not self.is_ready():
            print_log("CRITICAL ERROR: query on uninitialized index", priority=0)

        # TODO prepare query_string

        if self.compression:
            # TODO compressed read
            pass
        else:
            # TODO uncompressed read
            pass

        # TODO ranking
        # usare res.append_result(item,score)

        # TODO: return best results
        return res


def add_posting_list(token_id, token_count, docid):
    # add new entries in posting list
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
    if len(posting_buffer) > index_chunk_size:
        return True  # chunk is big, time to write it on disk
    else:
        return False  # no need to write it on disk yet


def add_to_lexicon(token_id, token_count):
    # add new word into lexicon
    global lexicon_buffer
    found = False
    for token in lexicon_buffer:
        if token[0] == token_id:
            found = True
            token[1] += token_count
            break
    if found == False:
        lexicon_buffer.append([token_id, token_count])
    if len(lexicon_buffer) > index_chunk_size:
        return True  # chunk is big, time to write it on disk
    else:
        return False  # no need to write it on disk yet


def merge_chunks(file_list, output_file_path, mode=""):
    reader_list = []
    first_element_list = []
    for file in file_list:
        reader = open(file, "r+")
        reader_list.append(reader)
        first_element_list.append(reader.readline())
    output_file = open(output_file_path, "w+")
    while True:
        next_index = -1
        # cerco in ogni elemento di firstelemlist
        i = 0
        output_row = ""
        output_key = ""
        for element in first_element_list:
            # controllo che quella lista abbia ancora elementi
            if element is not "empty":
                if mode == "posting":
                    element_splitted = element.split(sep=posting_separator)
                    # estraggo l'elemento alfabeticamente minore
                    if output_key == "" or element_splitted[0] < output_key:
                        output_key, output_row = element_splitted[0], element
                        next_index = i
                elif mode == "lexicon":
                    pass
                else:
                    print_log("CRITICAL ERROR: Unknown merge mode")
            i += 1
        if next_index > -1:
            # rimpiazzo l'elemento estratto leggendo il successivo
            first_element_list[i] = reader_list[i].readline()
            if len(first_element_list[i]) == 0:
                first_element_list[i] = "empty"
            # lo scrivo nel file output
            output_file.write(output_row)
        else:
            # se tutte le liste sono vuote, ho finito
            break
    for file in file_list:
        file.close()
    output_file.close()

def add_document_to_index(index, docid, docno, doctext):
    posting_file_list = []
    lexicon_file_list = []
    if not index.is_ready():
        print_log("cannot add document with uninitialized index", priority=0)

    is_duplicate = index.add_content_id(docid)
    if is_duplicate:
        print_log("duplicate document " + str(docid), priority=4)
        return
    print_log("adding row " + str(docid) + "as document " + str(docno) + " to index " + str(index.name), priority=5)

    tokens = preprocess_text(doctext, index.skip_stemming, index.allow_stop_words)
    token_counts = count_token_occurrences(tokens).items()

    # document length to be saved in collection statistics
    word_count = len(tokens)
    index.add_to_collection_stats(docid, docno, word_count)

    for token_id, token_count in token_counts:
        # inverted index is based on posting lists
        full = add_posting_list(token_id, token_count, docid)
        if full:
            posting_file_list.append(index.update_posting_list())
        # update lexicon at each new word
        full = add_to_lexicon(token_id)
        if full:
            lexicon_file_list.append(index.update_lexicon())
    merge_chunks(posting_file_list, index.index_file_path, mode="posting")
    merge_chunks(lexicon_file_list, index.lexicon_path, mode="lexicon")
    # TODO MERGE LEXICON
    # TODO MERGE POSTING
