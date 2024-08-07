import argparse

from src.modules.queryHandler import QueryHandler
#from src.modules.utils import Menu as menu


#config = [50, query_processing_algorithm, scoring_function, 6, True, True]

import os
from colorama import Fore, Style, init
import time

from src.config import scoring_function_config, query_processing_algorithm_config
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
            #print(int(input(prompt)))

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


# Global variables
file_count = 50 #TODO da rivedere per opzione ALL
method = 'disjunctive'
scoring = 'BM25'
k = 10
skip_stemming = False
allow_stop_words = True
query_input = ''
first_step = 0

def print_query_result(results):
    for document, relevance in results:
        print("Relevance: " + str(relevance) + " ; Document: " + str(document))

#TODO implementare chiamata su execute
def test_index(query_string, index_name, flags):
    tic = time.perf_counter()
    test_index_element = load_from_disk(index_name)
    if test_index_element is None:
        test_index_element = index_setup(index_name, stemming_flag=flags[4], stop_words_flag=flags[5],
                                         compression_flag=False,
                                         k=flags[3], algorithm=flags[1], scoring_f=flags[2], eval_f=0)
    if test_index_element.is_ready():
        if not test_index_element.content_check(int(flags[0] / 2)):
            test_index_element.scan_dataset(flags[0], delete_after_compression=False)
    else:
        print("index not ready")

    # TEST QUERY
    handler = QueryHandler(test_index_element)
    res = handler.query(query_string)

    print_query_result(res)

    toc = time.perf_counter()
    return tic, toc


# Main function
def main():
    global file_count, method, scoring, k, skip_stemming, allow_stop_words, query_input, first_step
    while True:

        # Menu structure
        menu = {
            '1': {
                'title': 'Parametri',
                'submenu': {
                    '1': {
                        'title': 'file_count: ' + str(file_count),
                        'action': lambda: globals().update({'file_count': get_int_input(
                            Fore.YELLOW + "Valore attuale: " + str(
                                file_count) + "\n" + " Inserisci il nuovo valore per file_count (intero): ")}),
                    },
                    '2': {
                        'title': 'method: ' + str(method),
                        'action': lambda: globals().update({'method': get_choice_input(
                            Fore.YELLOW + "Valore attuale: " + str(
                                method) + "\n" + "Inserisci il metodo (conjunctive/disjunctive): ",
                            ['conjunctive', 'disjunctive'])}),
                    },
                    '3': {
                        'title': 'scoring: ' + str(scoring),
                        'action': lambda: globals().update({'scoring': get_choice_input(
                            Fore.YELLOW + "Valore attuale: " + str(
                                scoring) + "\n" + "Inserisci il metodo di scoring (BM11/BM15/BM25/TFIDF): ",
                            ['BM11', 'BM15', 'BM25', 'TFIDF'])}),
                    },
                    '4': {
                        'title': 'k: ' + str(k),
                        'action': lambda: globals().update(
                            {'k': get_int_input(Fore.YELLOW + "Valore attuale: " + str(
                                k) + "\n" + "Inserisci il valore per k (intero): ")}),
                    },
                    '5': {
                        'title': 'skip_stemming: ' + str(skip_stemming),
                        'action': lambda: globals().update(
                            {'skip_stemming': get_bool_input(Fore.YELLOW + "Valore attuale: " + str(
                                file_count) + "\n" + "Vuoi saltare lo stemming (true/false)? ")}),
                    },
                    '6': {
                        'title': 'allow_stop_words: ' + str(allow_stop_words),
                        'action': lambda: globals().update({'allow_stop_words': get_bool_input(
                            Fore.YELLOW + "Valore attuale: " + str(
                                file_count) + "\n" + "Vuoi consentire stop words (true/false)? ")}),
                    },
                    '7': {
                        'title': 'Torna al menu principale',
                        'action': lambda: print(Fore.MAGENTA + "Tornato al menu principale"),
                    }
                }
            },
            '2': {
                'title': 'Query: ' + str(query_input),
                'action': lambda: globals().update({'query_input': input(Fore.YELLOW + "Inserisci la query: ")}),
            },
            '3': {
                'title': 'Execute',
                'action': lambda: print(Fore.MAGENTA + "Esecuzione"),
            },
            '4': {
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
        if choice == '3' and current_menu == menu:
            handle_selection(menu, choice)
            break
        else:
            current_menu = handle_selection(current_menu, choice)
        input(f"\n{Fore.GREEN}Premi Invio per continuare...")


if __name__ == "__main__":
    main()
