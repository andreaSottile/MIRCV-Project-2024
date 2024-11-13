import os
import time

from colorama import Fore, Style, init

from src.config import file_format, index_config_path
from src.modules.InvertedIndex import load_from_disk
from src.modules.interface import handle_selection, print_menu, update_parameter, print_query_result, is_restart_needed, \
    get_index_name, setup_main_index, get_index_title, user_write_parameters, get_parameter, set_parameter
from src.modules.QueryHandler import QueryHandler

# Initialize colorama
init(autoreset=True)
query_input = ''
main_query_handler = None
main_index_element = None


# Function to clear the console
def clear_console():
    os.system('cls' if os.name == 'nt' else 'clear')


def user_load_parameters():
    global main_index_element, main_query_handler
    # read from HD all the parameters for the current index / query
    if os.path.exists(index_config_path + get_index_name() + file_format):
        # all the previous parameters (client side) are lost, take the ones read from disk
        main_index_element = load_from_disk(get_index_name())
        main_query_handler = QueryHandler(main_index_element)

        # move the loaded parameters in the user interface
        set_parameter("file_count", main_index_element.num_doc)
        set_parameter("method", main_index_element.algorithm)
        set_parameter("scoring", main_index_element.scoring)
        set_parameter("k", main_index_element.topk)
        set_parameter("skip_stemming", main_index_element.skip_stemming)
        set_parameter("allow_stop_words", main_index_element.allow_stop_words)
        set_parameter("compression", main_index_element.compression)

        # index loaded, clean the restart flag
        set_parameter("restart", False)


    else:
        print("no index found with name " + str(get_index_name()))


def query_execution(query_string):
    global main_query_handler
    if not is_restart_needed():

        print("starting timer")
        tic = time.perf_counter()

        res = main_query_handler.query(query_string, get_parameter("search_algorithm"))
        print_query_result(res)

        toc = time.perf_counter()
        print(f"query completed in {toc-tic} s")

    else:
        print("You have to reindex the collection before querying")


# Main function
def main():
    global main_index_element, main_query_handler
    first_step = 0
    # initialize the index default and the query handler structures
    main_index_element, main_query_handler = setup_main_index(get_index_title())

    current_menu = {}
    while True:
        # Menu structure
        menu = {
            '1': {
                'title': 'Change parameters',
                'submenu': {
                    '1': {
                        'title': 'file_count: ' + str(get_parameter("file_count")),
                        'action': lambda: update_parameter('file_count', main_query_handler),
                    },
                    '2': {
                        'title': 'method: ' + str(get_parameter("method")),
                        'action': lambda: update_parameter('method', main_query_handler),
                    },
                    '3': {
                        'title': 'scoring: ' + str(get_parameter("scoring")),
                        'action': lambda: update_parameter('scoring', main_query_handler),
                    },
                    '4': {
                        'title': 'k: ' + str(get_parameter("k")),
                        'action': lambda: update_parameter('k', main_query_handler),
                    },
                    '5': {
                        'title': 'skip_stemming: ' + str(get_parameter("stem")),
                        'action': lambda: update_parameter('skip_stemming', main_query_handler),
                    },
                    '6': {
                        'title': 'allow_stop_words: ' + str(get_parameter("stop")),
                        'action': lambda: update_parameter('allow_stop_words', main_query_handler),
                    },
                    '7': {
                        'title': 'compression: ' + str(get_parameter("compression")),
                        'action': lambda: update_parameter('compression', main_query_handler),
                    },
                    '8': {
                        'title': 'index_title: ' + get_index_title(),
                        'action': lambda: update_parameter('index_title', main_query_handler),
                    },
                    '9': {
                        'title': 'Search algorithm: ' + str(get_parameter("search_algorithm")),
                        'action': lambda: update_parameter('search_algorithm', main_query_handler),
                    },
                    '10': {
                        'title': 'Back to main menu',
                        'action': lambda: print(Fore.MAGENTA + "Returned to main menu"),
                    }
                }
            },
            '2': {
                'title': 'Save parameters (Actual Index Name: ' + get_index_title() + ' | Restart Needed:' + str(
                    is_restart_needed()) + ')',
                'action': lambda: user_write_parameters(main_query_handler),
            },
            '3': {
                'title': 'Load parameters (Actual Index Name: ' + get_index_title() + ')',
                'action': lambda: user_load_parameters(),
            },
            '4': {
                'title': 'Query: \"' + str(query_input) + "\"",
                'action': lambda: globals().update({'query_input': input(Fore.YELLOW + "Type your query: ")}),
            },
            '5': {
                'title': 'Execute' + ' | Restart Needed:' + str(is_restart_needed()) + ')',
                'action': lambda: query_execution(query_input),
            },
            '6': {
                'title': 'Exit',
                'action': lambda: print(Fore.MAGENTA + "Quit execution"),
            }
        }

        # Main loop to display the menu and handle user input
        if first_step == 0:
            current_menu = menu
            first_step += 1
        clear_console()
        print(f"{Fore.BLUE}{Style.BRIGHT}Menu:")
        print_menu(current_menu)
        choice = input(f"{Fore.YELLOW}\nSelect an option: {Style.RESET_ALL}")
        if choice == '5' and current_menu == menu:
            _, first_step = handle_selection(menu, choice, first_step)
            break
        else:
            current_menu, first_step = handle_selection(current_menu, choice, first_step)

        input(f"\n{Fore.GREEN}Press Enter to continue...")


if __name__ == "__main__":
    main()
