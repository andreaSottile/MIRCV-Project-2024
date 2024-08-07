import argparse

from src.modules.queryHandler import QueryHandler
# from src.modules.utils import Menu as menu


# config = [50, query_processing_algorithm, scoring_function, 6, True, True]

import os
from colorama import Fore, Style, init
import time

from src.config import scoring_function_config, query_processing_algorithm_config, file_format, index_config_path
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
skip_stemming = False
allow_stop_words = True
query_input = ''
index_title = 'default_index'
index_restart_needed=True
first_step = 0
main_query_handler = None
main_index_element = None



def print_query_result(results):
    for document, relevance in results:
        print("Relevance: " + str(relevance) + " ; Document: " + str(document))


# TODO implementare chiamata su execute
# TODO fare load and save di nuovo index
def setup_main_index(index_name, flags):
    global main_query_handler, main_index_element, index_restart_needed
    if index_restart_needed== False:
        main_query_handler.index.save_on_disk()
    else:
        #Check for duplicates
        if os.path.exists(index_config_path + index_name + file_format) and index_title is not 'default_index':
            print()
            main_index_element = load_from_disk(index_name)
            if main_index_element is None:
                main_index_element = index_setup(index_name, stemming_flag=flags[4], stop_words_flag=flags[5],
                                                 compression_flag=False,
                                                 k=flags[3], algorithm=flags[1], scoring_f=flags[2], eval_f=0)
            if main_index_element.is_ready():
                if not main_index_element.content_check(int(flags[0] / 2)):
                    main_index_element.scan_dataset(flags[0], delete_after_compression=False)
            else:
                print("index not ready")
    main_query_handler = QueryHandler(main_index_element)
    index_restart_needed = False


def query_execution(query_string):
    global main_query_handler, index_restart_needed
    if not index_restart_needed:
        res = main_query_handler.query(query_string)
        print_query_result(res)
    else:
        print("You have to reindex the collection before querying")

def update_parameter(parameter):
    global index_restart_needed
    if parameter is 'file_count':
        globals().update({'file_count': get_int_input(
            Fore.YELLOW + "Valore attuale: " + str(
                file_count) + "\n" + " Inserisci il nuovo valore per file_count (intero): ")})
        index_restart_needed=True
    elif parameter is 'method':
        globals().update({'method': get_choice_input(
        Fore.YELLOW + "Valore attuale: " + str(
            method) + "\n" + "Inserisci il metodo (conjunctive/disjunctive): ",
        ['conjunctive', 'disjunctive'])})
        main_query_handler.index.algorithm = method
    elif parameter is 'scoring':
        globals().update({'scoring': get_choice_input(
        Fore.YELLOW + "Valore attuale: " + str(
            scoring) + "\n" + "Inserisci il metodo di scoring (BM11/BM15/BM25/TFIDF): ",
        ['BM11', 'BM15', 'BM25', 'TFIDF'])})
        main_query_handler.index.scoring = scoring
    elif parameter is 'k':
        globals().update({'k': get_int_input(Fore.YELLOW + "Valore attuale: " + str(
            k) + "\n" + "Inserisci il valore per k (intero): ")})
        main_query_handler.index.topk = k
    elif parameter is 'skip_stemming':
        globals().update(
        {'skip_stemming': get_bool_input(Fore.YELLOW + "Valore attuale: " + str(
            file_count) + "\n" + "Vuoi saltare lo stemming (true/false)? ")})
        index_restart_needed=True
    elif parameter is 'allow_stop_words':
        globals().update({'allow_stop_words': get_bool_input(
        Fore.YELLOW + "Valore attuale: " + str(
            file_count) + "\n" + "Vuoi consentire stop words (true/false)? ")}),
        index_restart_needed = True
    elif parameter is 'index_title':
        globals().update({'index_title': get_string_input(
        Fore.YELLOW + "Valore attuale: " + str(
            index_title) + "\n" + "Insert title of the index: ")}),
        index_restart_needed = True


# Main function
def main():
    global file_count, method, scoring, k, skip_stemming, allow_stop_words, query_input, index_title, first_step
    config = [file_count, method, scoring, k, skip_stemming, allow_stop_words]
    setup_main_index(index_title, config)
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
                        'title': 'index_title: ' + str(index_title),
                        'action': lambda: update_parameter('index_title'),
                    },
                    '8': {
                        'title': 'Torna al menu principale',
                        'action': lambda: print(Fore.MAGENTA + "Tornato al menu principale"),
                    }
                }
            },
            '2': {
                'title': 'Recreate Main Index (Actual Index Name: ' + str(index_title) + ' | Restart Needed:'+ str(index_restart_needed) + ')',
                'action': lambda: setup_main_index(index_title, config),
            },
            '3': {
                'title': 'Query: ' + str(query_input),
                'action': lambda: globals().update({'query_input': input(Fore.YELLOW + "Inserisci la query: ")}),
            },
            '4': {
                'title': 'Execute',
                'action': lambda: query_execution(query_input),
            },
            '5': {
                'title': 'Esci',
                'action': lambda: print(Fore.MAGENTA + "Uscita dal programma"),
            }
        }

        # Main loop to display the menu and handle user input
        if first_step == 0:
            current_menu = menu
            first_step += 1
            config = [file_count, method, scoring, k, skip_stemming, allow_stop_words]
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
