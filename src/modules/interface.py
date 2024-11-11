'''
Demo Interface. Your program must read user queries via simple command line prompt,
and must return the <pid> of each result. After returning the result, your program
should wait for the next query to be input.
'''

# Function to print the menu
import os

from colorama import Fore

# Global variables (defaults)
from src.config import query_processing_algorithm_config, scoring_function_config, index_config_path, file_format, \
    search_into_file_algorithms, default_index_title
from src.modules.InvertedIndex import load_from_disk, index_setup
from src.modules.QueryHandler import QueryHandler

file_count = 500  # user is going to edit this
method = 'disjunctive'
scoring = 'BM25'
k = 10
evaluation = "trec_dl_2019_queries"
compression = "no"
skip_stemming = False
allow_stop_words = True
index_restart_needed = True
first_step = 0
search_algorithm = "ternary"
index_title = default_index_title

def get_parameter(parameter_name):
    if parameter_name == "file_count":
        return file_count
    if parameter_name == "method":
        return method
    if parameter_name == "scoring":
        return scoring
    if parameter_name == "k":
        return k
    if parameter_name == "stem":
        return skip_stemming
    if parameter_name == "stop":
        return allow_stop_words
    if parameter_name == "compression":
        return compression
    if parameter_name == "search_algorithm":
        return search_algorithm


def get_index_title():
    return str(index_title)


def flag_restart_needed():
    global index_restart_needed
    index_restart_needed = True


def clear_restart_needed():
    global index_restart_needed
    index_restart_needed = False


def is_restart_needed():
    return index_restart_needed


def get_index_name():
    global index_title
    return index_title


def print_menu(menu, level=0):
    indent = ' ' * (level * 4)
    for key, value in menu.items():
        color = Fore.GREEN if 'submenu' in value else Fore.CYAN
        print(f"{indent}{color}{key}: {value['title']}")
        if 'submenu' in value:
            print_menu(value['submenu'], level + 1)


# Function to handle menu selection
def handle_selection(selection, choice, step):
    if choice in selection.keys():
        selected = selection[choice]
        print(f"\n{Fore.YELLOW}Selected: {selected['title']}")
        if 'action' in selected:
            selected['action']()
        if 'submenu' in selected:
            return selected['submenu'], step
    else:
        print(f"{Fore.RED}Choice not valid. Try again.")
    # first_step = 0
    return selection, 0


# Function to get integer input
def get_int_input(prompt):
    while True:
        try:
            # print(int(input(prompt)))

            return int(input(prompt))
        except ValueError:
            print(Fore.RED + "Select a valid number.")


# Function to get boolean input
def get_bool_input(prompt):
    while True:
        value = input(prompt).strip().lower()
        if value in ['true', 'false']:
            return value == 'true'
        print(Fore.RED + "Type 'true' or 'false'.")


# Function to get choice input
def get_choice_input(prompt, choices):
    while True:
        value = input(prompt).strip().lower()
        if value in choices:
            return value
        print(Fore.RED + f"Type the number of your choice ({', '.join(choices)}).")


def get_string_input(prompt):
    while True:
        try:
            return input(prompt)
        except ValueError:
            print(Fore.RED + "Type the number of your choice.")


def print_query_result(results):
    for document, relevance in results:
        print("Relevance: " + str(relevance) + " ; Document: " + str(document))


def update_parameter(parameter, main_query_handler):
    # updates one parameter using in the index/query
    # warning: parameters on the query do NOT requires to re-index the whole collection
    # some parameters affects the indexing, so they require to re-scan the collection
    global index_restart_needed
    if parameter == 'file_count':
        globals().update({'file_count': get_int_input(
            Fore.YELLOW + "Current value: " + str(
                file_count) + "\n" + " Type a new value for file_count (integer) or '-1' for no count limit: ")})
        # notify the user that is necessary to scan the collection from scratch
        index_restart_needed = True
    elif parameter == 'method':
        opts = "/".join(query_processing_algorithm_config)
        globals().update({'method': get_choice_input(
            Fore.YELLOW + "Current value: " + str(
                method) + "\n" + "Choose your algorithm (" + opts + "): ",
            query_processing_algorithm_config)})
        main_query_handler.index.algorithm = method
    elif parameter == 'scoring':
        opts = "/".join(scoring_function_config)
        globals().update({'scoring': get_choice_input(
            Fore.YELLOW + "Current value: " + str(
                scoring) + "\n" + "Choose the scoring function (" + opts + "): ", scoring_function_config)})
        main_query_handler.index.scoring = scoring
    elif parameter == 'k':
        globals().update({'k': get_int_input(Fore.YELLOW + "Current value: " + str(
            k) + "\n" + "Choose your k  value (integer): ")})
        main_query_handler.index.topk = k
    elif parameter == 'skip_stemming':
        globals().update(
            {'skip_stemming': get_bool_input(Fore.YELLOW + "Current value: " + str(
                file_count) + "\n" + "Skip stemming (true/false)? ")})
        # notify the user that is necessary to scan the collection from scratch
        index_restart_needed = True
    elif parameter == 'allow_stop_words':
        globals().update({'allow_stop_words': get_bool_input(
            Fore.YELLOW + "Current value: " + str(
                file_count) + "\n" + "Keep stop words while indexing (true/false)? ")})
        # notify the user that is necessary to scan the collection from scratch
        index_restart_needed = True
    elif parameter == 'compression':
        globals().update({'compression': get_bool_input(
            Fore.YELLOW + "Current value: " + str(compression) + "Compress the index file? (true/false)? ")})
        # notify the user that is necessary to scan the collection from scratch
        index_restart_needed = True
    elif parameter == 'index_title':
        globals().update({'index_title': get_string_input(
            Fore.YELLOW + "Current value: " + str(index_title) + "\n" + "Insert title of the index: ")})
        # notify the user that is necessary to scan the collection from scratch
        index_restart_needed = True
    elif parameter == 'search_algorithm':
        opts = "/".join(search_into_file_algorithms)
        globals().update({'search_algorithm': get_choice_input(
            Fore.YELLOW + "Current value: " + str(
                scoring) + "\n" + "Choose the search function (" + opts + "): ", search_into_file_algorithms)})

    else:
        print("Parameter not found: " + str(parameter))


def setup_main_index(index_name):
    global evaluation, index_restart_needed, file_count, method, scoring, k, \
        compression, skip_stemming, allow_stop_words

    main_index_element = load_from_disk(index_name)
    if main_index_element is None:
        # first execution: nothing has been loaded from disk, so use default parameters
        main_index_element = index_setup(index_name, stemming_flag=skip_stemming, stop_words_flag=allow_stop_words,
                                         compression_flag=compression,
                                         k=k, join_algorithm=method, scoring_f=scoring)
    if main_index_element.is_ready():
        # check if index is empty or not
        if not main_index_element.content_check(int(file_count / 2)):
            main_index_element.scan_dataset(file_count, delete_chunks=True, delete_after_compression=True)
    else:
        print("index not ready")

    # since we need the index parameters as global variables (for the UI), we refresh their values
    main_query_handler = QueryHandler(main_index_element)
    index_restart_needed = False
    file_count = main_index_element.num_doc
    method = main_index_element.algorithm
    scoring = main_index_element.scoring
    k = main_index_element.topk
    compression = main_index_element.compression
    skip_stemming = main_index_element.skip_stemming
    allow_stop_words = main_index_element.allow_stop_words
    return main_index_element, main_query_handler


def user_write_parameters(main_query_handler):
    # save on HD all the parameters for the current index / query
    if not is_restart_needed():
        # save only the index_info file for the parameters
        # all the parameters have already been updated both in the user interface and in the index structure
        main_query_handler.index.save_on_disk()
    else:
        # Check for duplicates
        if os.path.exists(index_config_path + get_index_name() + file_format):
            # duplicate found, clean the files used in "append" mode
            main_query_handler.index.flush_collection_stats()

        # some parameters are affecting the indexing algorithm, so it's required to re-scan the collection
        main_query_handler.index.rename(get_index_name())
        main_query_handler.index.skip_stemming = skip_stemming
        main_query_handler.index.allow_stop_words = allow_stop_words
        main_query_handler.index.compression = compression

        main_query_handler.index.content_flush()  # reindexing requires to flush the content
        main_query_handler.index.save_on_disk()  # this overwrites the previous index_info file

        # reindex the collection, time intensive
        main_query_handler.index.scan_dataset(file_count, delete_chunks=True, delete_after_compression=True)
        clear_restart_needed()
