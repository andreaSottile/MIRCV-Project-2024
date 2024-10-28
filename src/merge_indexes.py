import os

from src.config import index_folder_path, collection_separator, posting_separator, element_separator
from src.modules.InvertedIndex import make_posting_list, index_setup, add_document_to_index, close_chunk, \
    load_from_disk, merge_chunks
from src.modules.document_processing import fetch_data_row_from_collection
from src.modules.utils import read_file_to_dict, find_missing_contents

'''
merge two (or more) indexes in one. this program is used to merge portions made with multiprocessing.
'''

# path lorenzo
source_folder = index_folder_path.replace("index", "multiprocessing")

# MANUALLY input the target file
target_folder = "indexes_TRUE-TRUE"
merge_folder = source_folder + "/" + target_folder
lexicons_list = []
indexes_list = []
write_stats_file_flag = True  # skip a step if it's already done
compression = "no"

index_stem = ""
index_stopw = ""

output_lexicon_path = source_folder + "/lexicon.txt"
output_stats_path = source_folder + "/stats.txt"
output_index_path = source_folder + "/index.txt"

# list of dictionaries (each dict is the content of a stats.txt file)
global_stats_list = []

print("merging index files. source folder: ")
print(source_folder)

print("opening input files")
# OPEN all input files
global_content = []
for f in os.scandir(merge_folder):
    if not f.name.startswith("index"):
        continue
    elif f.name.endswith(".txt"):
        with open(f.path, "rb") as cfg_file:
            content = cfg_file.readlines()
            local_content = content[4].decode("utf-8").strip().split(",")
            if index_stem == "":
                index_stem = content[1].decode("utf-8").strip() == "skip_stem"
            if index_stopw == "":
                index_stopw = content[2].decode("utf-8").strip() == "allow_sw"
            global_content.append([int(local_content[0]), int(local_content[1])])
    else:
        lexicons_list.append(open(merge_folder + "/" + f.name + "/lexicon.txt", "r"))
        indexes_list.append(open(merge_folder + "/" + f.name + "/index.txt", "r"))

        global_stats_list.append(
            read_file_to_dict(merge_folder + "/" + f.name + "/stats.txt", separator=collection_separator))

print("checking collection integrity")

missing_docids = find_missing_contents(global_content)

index_name = f"indt_missing_"
for d in missing_docids:
    temp_index_element = ""
    result_id, result_txt = fetch_data_row_from_collection(d)
    temp_index_name = merge_folder + "/" + index_name + str(d)
    if result_txt != [] and result_id[0] == d:
        #Check if the index is already presente we can skip the decompression and load from disk directly
        if os.path.exists(temp_index_name):
            temp_index_element = load_from_disk(temp_index_name)
            if temp_index_element.allow_stop_words != index_stopw or temp_index_element.skip_stemming != index_stem:
                temp_index_element = ""
        if temp_index_element == "":
            #Create new indexes for each missing documents, to guarantee the correct order in the global index
            temp_index_element = index_setup(index_name + str(d), index_stem, index_stopw, "no", 3, 0, 0)
            add_document_to_index(temp_index_element, [0, d, result_txt[0]])
            close_chunk(temp_index_element)
            # TODO verifica
            find_chunk_files = lambda directory: [os.path.join(index_folder_path + index_name + str(d), v) for v in
                                                  os.listdir(index_folder_path + index_name + str(d)) if
                                                  v.startswith("chunk")]
            merge_chunks(os.listdir(index_folder_path + index_name + str(d) + "/" + find_chunk_files),
                         temp_index_element.index_file_path, temp_index_element.lexicon_path)
            temp_index_element.save_on_disk()
    else:
        print("Fetch data row from collection failed")
        #Merge lexicon, index and stats file with the other ones
        lexicons_list.append(open(temp_index_element.lexicon_path, "r"))
        indexes_list.append(open(temp_index_element.index_file_path, "r"))

    global_stats_list.append(
        read_file_to_dict(temp_index_element.collection_statistics_path, separator=collection_separator))

print(global_stats_list)
for s in global_stats_list:
    print(s)

print("analyzing: stats files")
# first step
# make the global stats.txt file
# merge of all stats (each stats.txt file is open as dictionary)
if write_stats_file_flag:
    written_lines = 0
    last_written_value = -1  # initialize with a low number, lesser than any possible docid
    finished = False
    output_stats_file = open(output_stats_path, "w")
    while not finished:  # iterate once for every object in the global_stats_list
        finished = True  # termination condition

        # look for the lowest value that has not already been written

        # initialize minimum search
        candidate_value = 13553  # a random number, just to initialize the variable
        candidate_index = -1  # partition i
        for i in range(len(global_stats_list)):
            # minimum search: two conditions
            # (1) : no candidates found yet. any candidate with a bigger (=not written) value than last iteration is ok
            # (2) : at least one candidate found. take the best one (bigger than the ones already written)
            if (candidate_index < 0 and last_written_value < int(global_stats_list[i]["0"][0])) or (
                    candidate_value > int(global_stats_list[i]["0"][0]) > last_written_value):
                candidate_value = int(global_stats_list[i]["0"][0])
                candidate_index = i
                finished = False
        print(candidate_value)
        # if there is no value greater than the last_written_value, it's finished
        if not finished:
            last_written_value = candidate_value
            # write dictionary to file
            for name, size in global_stats_list[candidate_index].values():
                output_line = str(written_lines) + collection_separator + str(name) + collection_separator + str(
                    size) + "\n"
                output_stats_file.write(output_line)
                # update the counter
                written_lines += 1

    output_stats_file.close()
    print("stats written successfully. documents found:")
    print(written_lines)

else:
    print("stats loaded in memory")

# initialize lexicon tops: the first line from each one
first_element_list = []
for lex in lexicons_list:
    first_element_list.append(lex.readline())

print("opening output files")
# open all output files
output_lexicon_file = open(output_lexicon_path, "w+")
if compression != "no":
    output_index_file = open(output_index_path, "wb+")
else:
    output_index_file = open(output_index_path, "w+")

# work flow
# 1 - read from lexicon files the lower (alphabetically) token
# 2 - read the posting lists for that token (may have more than one)
# remember: tokens may be repeated but each document appears only once
# 3 - make the merged posting list (decode the gaps)
# 4 - fetch the doc names from the stats document (the real docid)
# 5 - order the d,d,d,d,d in the correct order
# 6 - check the .tell() position, then write the posting list
# 7 - add the token in the output lexicon

written_lines = 0

print(f"starting merge phase for {len(lexicons_list)} partitions")
while True:
    next_chunk_index = []
    # i have to compare every element of firstelemlist

    i = 0  # counter at partition's index in the input files list

    output_key = ""  # token as string

    # no more needed: i'll read the index file in the same order of the lexicon
    # key_offset_list = ""  # where to retrieve the posting lists

    # look at each first element, search the lowest
    for element in first_element_list:
        # make sure the list is not empty
        if element != "empty":
            element = element.replace(" \n", "")
            element_splitted = element.split(sep=element_separator)
            # "lowest" is in alphabetical order
            if output_key == "" or element_splitted[0] < output_key:
                output_key = element_splitted[0]  # set current token
                # element_splitted[1] is the token local frequency, and i dont need that
                # key_offset_list = [element_splitted[2]]  # not needed: i'll read the index file in order
                next_chunk_index = [i]  # initialize list
            elif element_splitted[0] == output_key:
                # token found in more than one partition (merge required)
                next_chunk_index.append(i)
                # key_offset_list.append(element_splitted[2]) # not needed: i'll read the index file in order
        i += 1

    if len(next_chunk_index) == 0:
        # no minimum found: all lexicons are empty
        break  # job finished
    # else minimum found (might have more than one partition with the same token)

    # update the first element of the lexicons of the partitions i've extracted the current elements
    # move the list pointer to the next element
    for i in next_chunk_index:
        first_element_list[i] = lexicons_list[i].readline()
        if len(first_element_list[i]) == 0:
            first_element_list[i] = "empty"
            print(f"partition{i} is now empty")

    # next_chunk_index is the list of which partitions contain that token
    # key_offset_list is the list of offsets to get the posting lists
    # let's now get the posting lists
    docid_list = []
    freq_list = []
    for i in next_chunk_index:
        # lexicon and index files have the same order: one row for each token
        line = indexes_list[i].readline()
        gaps, freqs = line.split()

        gap_list = map(int, gaps.split(","))
        frequencies = freqs.split(",")  # keep them as strings, i dont have to make the sums because docids are unique

        # Convert the gaps back to doc IDs
        previous_doc_id = 0
        for gap in gap_list:
            doc_id = previous_doc_id + gap
            # WARNING: the extracted docid is local in the partition
            # must convert the local docid in the global one
            docid_list.append(global_stats_list[i][str(doc_id)][0])
            # still using the local one for the gaps conversion
            previous_doc_id = doc_id
            # append freq
        for f in frequencies:
            freq_list.append(f)

    # time to prepare the output
    index_line = make_posting_list(docid_list, freq_list, compression)

    last_written_position = output_index_file.tell()
    lexicon_line = str(
        output_key + element_separator + str(len(docid_list)) + element_separator + str(last_written_position) + "\n")

    # OUTPUT: writing one row in index file and in the lexicon
    output_index_file.write(index_line)
    output_lexicon_file.write(lexicon_line)
    written_lines += 1

# end: cleaning the things left open
for f in lexicons_list:
    f.close()
for f in indexes_list:
    f.close()

output_lexicon_file.close()
output_index_file.close()

'''

print_log("Chunks merge finished for ", 1)
for file in reader_list:
    file.close()
if delete_after_merge:
    print_log("Deleting chunks after merge", 2)
    for file_name in file_list:
        os.remove(file_name)
index_file.close()
lexicon_file.close()

'''
