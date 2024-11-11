import os
import ast

from src.config import verbosity_config, output_query_trec_evaluation_file


def print_log(msg, priority=0):
    # 0 : only errors
    # 1 : processes and modules
    # 2 : critical blocks
    # 3 : function calls
    # 4 : row by row (debug)
    # 5 : loop iterations
    if priority <= verbosity_config:
        print(msg)


def readline_with_strip(file):
    string = file.readline().strip()
    return string


def get_last_line(file_pointer):
    # Get the position of the end of the file
    # returns the cursor position where the last line starts, and the last line

    file_pointer.seek(0, 2)  # 2 means SEEK_END
    end_pos = file_pointer.tell()  # tell() method returns the current file position in a file stream
    # Start from the end of the file
    file_pointer.seek(end_pos - 3, 0)  # last byte is a \n, so move it back by 2 chars

    pos = end_pos - 1  # The current position in the file
    while pos >= 0:  # hopefully it won't reach the beginning of the file
        byte = file_pointer.read(1)
        if type(byte) is str:
            byte = byte.encode('utf-8')
        if byte == b'\n':  # Check for newline character
            # Read the line after the newline character
            line_position = pos
            file_pointer.seek(line_position)
            last_line = file_pointer.readline()  # Read the last line
            if type(last_line) is str:
                last_line = last_line.strip()
            else:
                last_line = last_line.decode().strip()
            return line_position, last_line
        pos = file_pointer.tell()
        file_pointer.seek(pos - 2, 0)  # Move one byte backwards from the current position
        pos -= 1

    # If no newline character is found, the file has one line
    file_pointer.seek(0)
    last_line = file_pointer.readline()  # Read the last line
    if type(last_line) is str:
        last_line = last_line.strip()
    else:
        last_line = last_line.decode().strip()
    return 0, last_line  # The position of the first line


def get_row_id(row_string, key_delimiter):
    # cut the row to read only the key (which i'm going to need for comparisons)
    return row_string.split(key_delimiter)[0]


def set_search_interval(file_pointer, start, key, key_delimiter, step_size, id_is_a_String=True):
    # divide the file in chunks, and look for the correct one where to look for the key-word
    # @ param file_pointer : file where to look for
    # @ param start : position (byte number) where to start the search
    # @ param key : word to look for (warning: int or str)
    # @ param key_delimiter : char separating the key-word from the rest of the line
    # @ returns : low,high as positions in the file
    low = start
    high, top_row = next_GEQ_line(file_pointer, start + step_size)
    print_log("Reading the last line of the chunk", 8)
    print_log(top_row, 8)
    skipped = 0
    if id_is_a_String:
        while key > get_row_id(top_row, key_delimiter):
            print_log("Reading between " + str(low) + " and " + str(high), 5)
            low = high
            high, top_row = next_GEQ_line(file_pointer, low + step_size)
            skipped += 1
    else:
        while int(key) > int(get_row_id(top_row, key_delimiter)):
            print_log("Reading between " + str(low) + " and " + str(high), 5)
            low = high
            high, top_row = next_GEQ_line(file_pointer, low + step_size)
            skipped += 1
    print_log("Skipped " + str(skipped) + " chunks", 5)
    return low, high, top_row


def next_GEQ_line(file_pointer, position):
    '''
    given a position (bytes in a file), read the next whole line available there
    IMPORTANT: it works with cursor positions, and NOT with IDs
    :param file_pointer: pointer to the Lexicon file
    :param position: start position
    :return:
    '''
    #
    print_log("opening file at position " + str(position), 7)
    # no way to read only one line from a file, the closest thing we have is .seek (move the cursor to j-th byte)

    file_EOF_pos = file_pointer.seek(0, 2)
    # special case, position is beyond the file size
    if position >= file_EOF_pos:
        # avoid returning empty stuff, for easier handling
        return get_last_line(file_pointer)

    # set the pointer position
    file_pointer.seek(position)

    if position == 0:  # special case, no need to skip positions
        line = file_pointer.readline()  # Read the last line
        if type(line) is not str:
            line = line.decode()
        return 0, line

    # move the pointer to the next row start
    file_pointer.readline()
    # returns the position of the row start, and the read row
    line_start = file_pointer.tell()
    line = file_pointer.readline()
    if type(line) is not str:
        line = line.decode()
    print_log("found something: " + str(line[:8]) + "...", 7)
    return line_start, line


def enforce_key_type(value, key_type):
    if key_type == "int":
        if value == '':
            return 0
        return int(value)
    else:
        return str(value)


def ternary_search(file_pointer, start_position, target_key, delimiter, end_position, last_key, last_row,
                   key_type="int"):
    '''
      search the key-word in a file, using a 3-nary search
     @ param file_pointer : file where to look for the word
     @ param start_position : start of the search interval at iteration 0
     @ param target_key : token/string to look for (important: could be string or int)
     @ param delimiter : char separating the key-word from the rest of the line
     @ end_position : last character of the file
     @ last_Key : key of the last line of the file
     @ last_row : last row of the file
     @ returns : pair position_in_file,line if success. -1,"" if failure
    '''
    # ternary search: split the interval into 3, and keep reducing the 3 segments in size until finding the element
    #  low   <  checkpoint_1  <  checkpoint_2 < high
    low_pos, low_row = next_GEQ_line(file_pointer, start_position)
    low_key = get_row_id(low_row, delimiter)
    print_log("first and last rows", 7)
    high_pos, high_row = end_position, last_row
    high_key = last_key

    print_log(low_row, 7)
    print_log(high_row, 7)
    print_log("ternary search started for word \'" + str(target_key) + "\'", 6)

    high_key = enforce_key_type(high_key, key_type)
    low_key = enforce_key_type(low_key, key_type)
    # check the extremes
    if target_key == low_key:
        return low_pos, low_row
    elif target_key == high_key:
        return high_pos, high_row
    else:
        print_log("key is not one of the extremes", 5)
    first_time = 0
    previous_high = previous_low = -1
    while True:
        # safety check: key is out of boundaries
        if not (low_key < target_key < high_key):
            break

        # make the 3 intervals working with the position in the file
        # about 1/3 of the interval size
        cut1 = low_pos + (high_pos - low_pos) // 3  # flooring division

        pivot_1_pos, pivot_1_row = next_GEQ_line(file_pointer, cut1)
        pivot_1_key = enforce_key_type(get_row_id(pivot_1_row, delimiter), key_type)
        # check lower border
        while low_key >= pivot_1_key:
            print_log("gap too low, repositioning pivot 1", 5)
            # enforce keys are different and ordered
            pivot_1_pos += 1
            pivot_1_pos, pivot_1_row = next_GEQ_line(file_pointer, pivot_1_pos)
            pivot_1_key = enforce_key_type(get_row_id(pivot_1_row, delimiter), key_type)
            if pivot_1_key >= target_key:
                # lucky! key is near the pivot1
                break
        # check lower interval content (lower border has already been checked)
        if low_key < target_key <= pivot_1_key:
            print_log("lower interval is the correct one", 5)
            print_log(str(low_key) + "<" + str(target_key) + "<" + str(pivot_1_key), 6)
            # check if i found the word i need
            if target_key == pivot_1_key:
                return pivot_1_pos, pivot_1_row

            if previous_low == low_pos and previous_high == pivot_1_pos:
                if first_time > 0:
                    first_time = 0
                    # avoid infinite loop if the term is not present
                    break
                pivot_1_pos, pivot_1_row = next_GEQ_line(file_pointer, low_pos)
                pivot_1_key = enforce_key_type(get_row_id(pivot_1_row, delimiter), key_type)
                first_time += 1
            else:
                previous_high = pivot_1_pos
                previous_low = low_pos

            high_pos = pivot_1_pos
            high_key = pivot_1_key

            continue  # skip the check of the other intervals

        # about 2/3 of the interval size
        cut2 = pivot_1_pos + (high_pos - pivot_1_pos) // 2  # flooring division

        pivot_2_pos, pivot_2_row = next_GEQ_line(file_pointer, cut2)
        pivot_2_key = enforce_key_type(get_row_id(pivot_2_row, delimiter), key_type)

        # check middle border
        while pivot_1_key >= pivot_2_key:
            print_log("gap too low, repositioning pivot 2", 5)
            # enforce keys are different and ordered
            if pivot_1_pos > pivot_2_pos:
                pivot_2_pos = pivot_1_pos
            pivot_2_pos += 1
            pivot_2_pos, pivot_2_row = next_GEQ_line(file_pointer, pivot_2_pos)
            pivot_2_key = enforce_key_type(get_row_id(pivot_2_row, delimiter), key_type)
            if pivot_2_key >= target_key:
                # lucky! key is near pivot2
                break
        # check middle interval content (lower border has already been checked)
        if pivot_1_key < target_key <= pivot_2_key:
            print_log("middle interval is the correct one", 5)
            print_log(str(pivot_1_key) + "<" + str(target_key) + "<" + str(pivot_2_key), 6)
            if target_key == pivot_2_key:
                return pivot_2_pos, pivot_2_row

            if previous_low == pivot_1_pos and previous_high == pivot_2_pos:
                if first_time > 0:
                    first_time = 0
                    # avoid infinite loop if the term is not present
                    break
                pivot_2_pos, pivot_2_row = next_GEQ_line(file_pointer, pivot_1_pos)
                pivot_2_key = enforce_key_type(get_row_id(pivot_2_row, delimiter), key_type)
                first_time += 1

            else:
                previous_high = pivot_2_pos
                previous_low = pivot_1_pos

            low_pos = pivot_1_pos
            low_key = pivot_1_key
            high_pos = pivot_2_pos
            high_key = pivot_2_key
            continue  # skip the check of the other interval

        # check upper border : termination condition
        if pivot_2_key >= high_key:
            print_log("upper interval has negative size, nothing found", 5)
            break

        print_log("upper interval is the correct one", 5)
        print_log(str(pivot_2_key) + "<" + str(target_key) + "<" + str(high_key), 6)

        if previous_low == pivot_2_pos and previous_high == high_pos:
            # avoid infinite loop if the term is not present
            break
        else:
            previous_high = high_pos
            previous_low = pivot_2_pos

        low_pos = pivot_2_pos
        low_key = pivot_2_key
    print_log("found nothing", 4)
    return -1, ""


def read_file_to_dict(file_path, separator=',', output_type="mix"):
    # read all files in a folder, and return a list of dictionaries (one for each file)
    # @ param file_path: file to open
    # @ param separator: char used to split the values in the document
    # @ param output_type: "int" or "str" to cast
    # @ return: list of dicts [{(keys: first colulm of doc1),(values: other two columns)},...]
    file_data = {}
    with open(file_path, 'r') as file:
        for line in file:
            line = line.strip()
            if line:
                parts = line.split(separator)
                # expecting to find 3 values
                # Lexicon.txt row: token, token_freq, offset
                # Stats.txt row: docid(local), docname, docsize
                key = str(parts[0])
                # optional: cast the values
                if output_type == "str":
                    value = [str(parts[1]), str(parts[2])]
                elif output_type == "int":
                    value = [int(parts[1]), int(parts[2])]
                else:
                    value = [str(parts[1]), int(parts[2])]
                # each value has a tuple as value
                file_data[key] = value
    # remember: docids are not in order
    return file_data


def find_missing_contents(ranges):
    # Order and merge the intervals
    merged_ranges = merge_ranges(ranges)

    # documents in the collection
    max_value = 8841822 # last docid

    missing_numbers = []
    current = 0

    for range_start, range_end in merged_ranges:
        # Find the numbers between `current` and the actual intervals
        if current < range_start:
            missing_numbers.extend(range(current, range_start))

        # Next point to run
        current = max(current, range_end + 1)

    # Add any missing numbers after the last interval up to `max_value`
    if current <= max_value:
        missing_numbers.extend(range(current, max_value + 1))

    return missing_numbers


def merge_ranges(ranges):
    # Order and merge the intervals
    ranges.sort()
    merged = [ranges[0]]

    for current in ranges[1:]:
        last = merged[-1]
        if current[0] <= last[1] + 1:
            merged[-1] = [last[0], max(last[1], current[1])]
        else:
            merged.append(current)

    return merged


def export_dict_to_file(trec_score_dicts_list):
    '''
    This code creates a text file (with the name specified by output_query_trec_evaluation_file) and writes entries from
    a dictionary structure into it. For each unique qid key in trec_score_dicts_list, it iterates over the associated list of entries.
    Each entry in this list is written as a new line in the file.
    This effectively saves each entry related to each qid into the file, with each dictionary formatted as a single line.

    :param trec_score_dicts_list: List of trec_eval dictionaries already grouped by Qid
    '''

    # We create a text file for every "qid" and we write the corresponding dictionaries
    with open(output_query_trec_evaluation_file, 'w') as file:
        for qid, entries in trec_score_dicts_list.items():
                for entry in entries:
                    file.write(f"{entry}\n")

def search_in_file(name, qid):
    '''
    This function checks if a given text file contains a line with specific values for the name and qid keys
    within a dictionary structure. It takes a file path and two target strings (string1 and string2) as input.
    For each line, it converts the line from a dictionary-like string to an actual dictionary using ast.literal_eval().
    If a dictionary in the file has a name value matching name and a qid value matching qid, the function returns True.
    If no such match is found after reading all lines, it returns False.

    We used ast.literal_eval() instead of eval() for security reasons.
    Specifically, eval() can execute arbitrary code, which poses a security risk if the file contents are untrusted.
    For example, if someone put malicious code in the file, eval() could execute it, potentially harming your system or data.
    ast.literal_eval() is a safer alternative that only evaluates strings that contain basic Python
    literals (such as dictionaries, lists, numbers, strings, etc.) and prevents execution of code.

    :param file_path: Output query trec eval file path
    :param name: index name
    :param qid: QID
    :return:
    '''
    # Open the file and read it line by line
    with open(output_query_trec_evaluation_file, 'r') as file:
        for line in file:
            # Attempt to convert the line from a string to a Python dictionary
            try:
                entry = ast.literal_eval(line.strip())
            except:
                continue  # Skip lines that cannot be converted to a dictionary

            # Check if the values of 'name' and 'qid' match string1 and string2
            if entry.get('name') == name and entry.get('qid') == qid:
                return True  # Return True if a match is found

    # If no matching line is found, return False
    return False