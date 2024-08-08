import os
from colorama import Fore, Style, init

from src.config import file_format, index_config_path, evaluation_method_config, scoring_function_config, \
    query_processing_algorithm_config
from src.modules.InvertedIndex import index_setup, load_from_disk
from src.modules.queryHandler import QueryHandler

# Initialize colorama
init(autoreset=True)


# Function to clear the console
def clear_console():
    os.system('cls' if os.name == 'nt' else 'clear')


# Function to print the menu
def print_menu(menu, level=0):
    indent = ' ' * (level * 4)
    for key, value in menu.items():
        color = Fore.GREEN if 'submenu' in value else Fore.CYAN
        print(f"{indent}{color}{key}: {value['title']}")
        if 'submenu' in value:
            print_menu(value['submenu'], level + 1)


# Function to handle menu selection
def handle_selection(selection, choice):
    global first_step
    if choice in selection.keys():
        selected = selection[choice]
        print(f"\n{Fore.YELLOW}Selezionato: {selected['title']}")
        if 'action' in selected:
            selected['action']()
        if 'submenu' in selected:
            return selected['submenu']
    else:
        print(f"{Fore.RED}Scelta non valida. Riprova.")
    first_step = 0
    return selection


# Function to get integer input
def get_int_input(prompt):
    while True:
        try:
            # print(int(input(prompt)))

            return int(input(prompt))
        except ValueError:
            print(Fore.RED + "Inserisci un numero intero valido.")


# Function to get boolean input
def get_bool_input(prompt):
    while True:
        value = input(prompt).strip().lower()
        if value in ['true', 'false']:
            return value == 'true'
        print(Fore.RED + "Inserisci 'true' o 'false'.")


# Function to get choice input
def get_choice_input(prompt, choices):
    while True:
        value = input(prompt).strip().lower()
        if value in choices:
            return value
        print(Fore.RED + f"Inserisci un valore valido ({', '.join(choices)}).")


def get_string_input(prompt):
    while True:
        try:
            return input(prompt)
        except ValueError:
            print(Fore.RED + "Inserisci un numero intero valido.")


# Global variables
file_count = 50  # TODO da rivedere per opzione ALL
method = 'disjunctive'
scoring = 'BM25'
k = 10
evaluation = "trec_dl_2019_queries"
compression = False
skip_stemming = False
allow_stop_words = True
query_input = ''
index_title = 'default_index'
index_restart_needed = True
first_step = 0
main_query_handler = None
main_index_element = None


def print_query_result(results):
    for document, relevance in results:
        print("Relevance: " + str(relevance) + " ; Document: " + str(document))


def user_write_parameters():
    # save on HD all the parameters for the current index / query
    global index_restart_needed, index_title, main_query_handler
    if not index_restart_needed:
        # save only the index_info file for the parameters
        # all the parameters have already been updated both in the user interface and in the index structure
        main_query_handler.index.save_on_disk()
    else:
        # Check for duplicates
        if os.path.exists(index_config_path + index_title + file_format):
            # duplicate found, clean the files used in "append" mode
            main_query_handler.index.flush_collection_stats()

        # some parameters are affecting the indexing algorithm, so it's required to re-scan the collection
        main_query_handler.index.rename(index_title)
        main_query_handler.index.skip_stemming = skip_stemming
        main_query_handler.index.allow_stop_words = allow_stop_words
        main_query_handler.index.compression = compression

        main_query_handler.index.content_flush()  # reindexing requires to flush the content
        main_query_handler.index.save_on_disk()  # this overwrites the previous index_info file

        # reindex the collection, time intensive
        main_query_handler.index.scan_dataset(file_count, delete_chunks=True, delete_after_compression=True)
        index_restart_needed = False


def user_load_parameters():
    global index_title, main_index_element, main_query_handler
    # read from HD all the parameters for the current index / query
    if os.path.exists(index_config_path + index_title + file_format):
        # all the previous parameters (client side) are lost, take the ones read from disk
        main_index_element = load_from_disk(index_title)
        main_query_handler = QueryHandler(main_index_element)
    else:
        print("no index found with name " + str(index_title))


def setup_main_index(index_name):
    global main_query_handler, evaluation, main_index_element, index_restart_needed, file_count, method, scoring, k, \
        compression, skip_stemming, allow_stop_words

    main_index_element = load_from_disk(index_name)
    if main_index_element is None:
        # first execution: nothing has been loaded from disk, so use default parameters
        main_index_element = index_setup(index_name, stemming_flag=skip_stemming, stop_words_flag=allow_stop_words,
                                         compression_flag=compression,
                                         k=k, algorithm=method, scoring_f=scoring, eval_f=evaluation)
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


def query_execution(query_string):
    global main_query_handler, index_restart_needed
    if not index_restart_needed:
        res = main_query_handler.query(query_string)
        print_query_result(res)
    else:
        print("You have to reindex the collection before querying")


def update_parameter(parameter):
    # updates one parameter using in the index/query
    # warning: parameters on the query do NOT requires to re-index the whole collection
    # some parameters affects the indexing, so they require to re-scan the collection
    global index_restart_needed, main_query_handler
    if parameter == 'file_count':
        globals().update({'file_count': get_int_input(
            Fore.YELLOW + "Valore attuale: " + str(
                file_count) + "\n" + " Inserisci il nuovo valore per file_count (intero): ")})
        # notify the user that is necessary to scan the collection from scratch
        index_restart_needed = True
    elif parameter == 'method':
        opts = "/".join(query_processing_algorithm_config)
        globals().update({'method': get_choice_input(
            Fore.YELLOW + "Valore attuale: " + str(
                method) + "\n" + "Inserisci il metodo ("+opts+"): ",
            query_processing_algorithm_config)})
        main_query_handler.index.algorithm = method
    elif parameter == 'scoring':
        opts = "/".join(scoring_function_config)
        globals().update({'scoring': get_choice_input(
            Fore.YELLOW + "Valore attuale: " + str(
                scoring) + "\n" + "Inserisci il metodo di scoring (" + opts + "): ", scoring_function_config)})
        main_query_handler.index.scoring = scoring
    elif parameter == 'k':
        globals().update({'k': get_int_input(Fore.YELLOW + "Valore attuale: " + str(
            k) + "\n" + "Inserisci il valore per k (intero): ")})
        main_query_handler.index.topk = k
    elif parameter == 'evaluation':
        opts = "/".join(evaluation_method_config)
        globals().update({'evaluation': get_choice_input(
            Fore.YELLOW + "Valore attuale: " + evaluation + "\n" + "choose the evaluation method (" + opts + "): ",
            evaluation_method_config)})
        main_query_handler.index.evaluation = evaluation
    elif parameter == 'skip_stemming':
        globals().update(
            {'skip_stemming': get_bool_input(Fore.YELLOW + "Valore attuale: " + str(
                file_count) + "\n" + "Vuoi saltare lo stemming (true/false)? ")})
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
            Fore.YELLOW + "Valore attuale: " + str(compression) + "Compress the index file? (true/false)? ")})
        # notify the user that is necessary to scan the collection from scratch
        index_restart_needed = True
    elif parameter == 'index_title':
        globals().update({'index_title': get_string_input(
            Fore.YELLOW + "Valore attuale: " + str(index_title) + "\n" + "Insert title of the index: ")})
        # notify the user that is necessary to scan the collection from scratch
        index_restart_needed = True

    else:
        print("Parameter not found: " + str(parameter))


# Main function
def main():
    global file_count, method, scoring, k, skip_stemming, allow_stop_words, query_input, index_title, first_step

    # initialize the index default and the query handler structures
    setup_main_index(index_title)

    current_menu = {}
    while True:
        # Menu structure
        menu = {
            '1': {
                'title': 'Parametri',
                'submenu': {
                    '1': {
                        'title': 'file_count: ' + str(file_count),
                        'action': lambda: update_parameter('file_count'),
                    },
                    '2': {
                        'title': 'method: ' + str(method),
                        'action': lambda: update_parameter('method'),
                    },
                    '3': {
                        'title': 'scoring: ' + str(scoring),
                        'action': lambda: update_parameter('scoring'),
                    },
                    '4': {
                        'title': 'k: ' + str(k),
                        'action': lambda: update_parameter('k'),
                    },
                    '5': {
                        'title': 'skip_stemming: ' + str(skip_stemming),
                        'action': lambda: update_parameter('skip_stemming'),
                    },
                    '6': {
                        'title': 'allow_stop_words: ' + str(allow_stop_words),
                        'action': lambda: update_parameter('allow_stop_words'),
                    },
                    '7': {
                        'title': 'compression: ' + str(compression),
                        'action': lambda: update_parameter('compression'),
                    },
                    '8': {
                        'title': 'evaluation method: ' + str(evaluation),
                        'action': lambda: update_parameter('evaluation'),
                    },
                    '9': {
                        'title': 'index_title: ' + str(index_title),
                        'action': lambda: update_parameter('index_title'),
                    },
                    '10': {
                        'title': 'Torna al menu principale',
                        'action': lambda: print(Fore.MAGENTA + "Tornato al menu principale"),
                    }
                }
            },
            '2': {
                'title': 'Save parameters (Actual Index Name: ' + str(index_title) + ' | Restart Needed:' + str(
                    index_restart_needed) + ')',
                'action': lambda: user_write_parameters(),
            },
            '3': {
                'title': 'Load parameters (Actual Index Name: ' + str(index_title) + ')',
                'action': lambda: user_load_parameters(),
            },
            '4': {
                'title': 'Query: \"' + str(query_input) + "\"",
                'action': lambda: globals().update({'query_input': input(Fore.YELLOW + "Inserisci la query: ")}),
            },
            '5': {
                'title': 'Execute' + ' | Restart Needed:' + str(index_restart_needed) + ')',
                'action': lambda: query_execution(query_input),
            },
            '6': {
                'title': 'Esci',
                'action': lambda: print(Fore.MAGENTA + "Uscita dal programma"),
            }
        }

        # Main loop to display the menu and handle user input
        if first_step == 0:
            current_menu = menu
            first_step += 1
        clear_console()
        print(f"{Fore.BLUE}{Style.BRIGHT}Menu:")
        print_menu(current_menu)
        choice = input(f"{Fore.YELLOW}\nScegli un'opzione: {Style.RESET_ALL}")
        if choice == '53' and current_menu == menu:
            handle_selection(menu, choice)
            break
        else:
            current_menu = handle_selection(current_menu, choice)
        input(f"\n{Fore.GREEN}Premi Invio per continuare...")


if __name__ == "__main__":
    main()
