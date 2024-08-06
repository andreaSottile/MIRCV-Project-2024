import argparse

from src.modules.queryHandler import QueryHandler
#from src.modules.utils import Menu as menu



#config = [50, query_processing_algorithm, scoring_function, 6, True, True]


def print_menu(menu, level=0):
    indent = ' ' * (level * 4)
    for key, value in menu.items():
        print(f"{indent}{key}: {value['title']}")
        if 'submenu' in value:
            print_menu(value['submenu'], level + 1)


def handle_selection(menu, choice):
    if choice in menu:
        selected = menu[choice]
        print(f"Selezionato: {selected['title']}")
        if 'action' in selected:
            selected['action']()
        if 'submenu' in selected:
            return selected['submenu']
    else:
        print("Scelta non valida. Riprova.")
    return menu


def main():
    menu = {
        '1': {
            'title': 'Opzione 1',
            'action': lambda: print("Eseguito Opzione 1"),
            'submenu': {
                '1': {
                    'title': 'Opzione 1.1',
                    'action': lambda: print("Eseguito Opzione 1.1"),
                },
                '2': {
                    'title': 'Opzione 1.2',
                    'action': lambda: print("Eseguito Opzione 1.2"),
                },
            }
        },
        '2': {
            'title': 'Opzione 2',
            'submenu': {
                '1': {
                    'title': 'Opzione 2.1',
                    'action': lambda: print("Eseguito Opzione 2.1"),
                },
                '2': {
                    'title': 'Opzione 2.2',
                    'action': lambda: print("Eseguito Opzione 2.2"),
                },
                '3': {
                    'title': 'Torna al menu principale',
                    'action': lambda: print("Tornato al menu principale"),
                }
            }
        },
        '3': {
            'title': 'Esci',
            'action': lambda: print("Uscita dal programma"),
        }
    }

    current_menu = menu
    while True:
        print("\nMenu:")
        print_menu(current_menu)
        choice = input("Scegli un'opzione: ")
        if choice == '3':
            if current_menu == menu:
                current_menu = handle_selection(menu, choice)
                break
            else:
                current_menu = menu
        else:
            current_menu = handle_selection(current_menu, choice)


if __name__ == "__main__":
    main()
