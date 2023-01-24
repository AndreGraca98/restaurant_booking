import logging
import sqlite3
from functools import partial
from typing import Dict, List

from src.kitchen import Kitchen

from .bookings import Bookings
from .db import add_item_to_menu
from .log import add_console_handler
from .orders import Orders

optionsLogger = logging.getLogger(__name__)
add_console_handler(optionsLogger)


class InvalidOptionError(Exception):
    """Invalid option exception"""


def select_option(options: Dict[int, str], menu: str) -> int:
    print(menu)
    try:
        option = int(input("Select option: "))
        if option not in options.keys():
            raise InvalidOptionError

    except (ValueError, InvalidOptionError):
        optionsLogger.warn("Please choose a valid option.")
        return select_option(options, menu)

    except KeyboardInterrupt:
        optionsLogger.warn("Ending session...")
        exit(0)

    print("\n" + "»" * 40 + "«" * 40)
    print("»" * 40 + "«" * 40 + "\n")
    return option


def input_kwargs(args: List[str]) -> Dict[str, str]:
    d = dict()

    if not args:
        return d

    for k, type_ in args:
        inp = input(f"{k}: ")

        if inp == "&START":
            optionsLogger.info("Starting multi-item input. Use &END to end input.")
            item_list = []
            i = 0
            while True:
                inp = input(f"Item {i} for {k}: ")
                if inp == "&END":
                    break
                item_list.append(inp)
                i += 1

            d[k] = item_list
            continue

        # If input is empty, set value to None
        d[k] = type_(inp) if inp else None

    return d


def options(conn: sqlite3.Connection):
    main_menu = """MAIN MENU:
  1. Booking
  2. Menu
  3. Order
  4. Kitchen"""

    main_options = {i: opt for i, opt in enumerate(main_menu.splitlines()[1:], 1)}

    opt = select_option(main_options, main_menu)

    def booking():
        booking_menu = """BOOKING MENU:
  1. Book a table
  2. Change booking
  3. cancel booking
  4. show all bookings
  5. Show tables for date"""

        booking_methods = {
            1: Bookings(conn).add,
            2: Bookings(conn).update,
            3: Bookings(conn).delete,
            4: Bookings(conn).show,
            5: Bookings(conn).show_available_tables,
        }

        booking_kwargs = {
            1: (
                ["reservation_datetime", str],
                ["table_number", int],
                ["client_name", str],
                ["client_contact", str],
            ),
            2: (
                ["booking_id", int],
                ["client_name", str],
                ["client_contact", str],
                ["reservation_datetime", str],
                ["table_number", int],
            ),
            3: [
                ["booking_id", int],
                ["client_name", str],
                ["client_contact", str],
            ],
            4: None,
            5: [["reservation_datetime", str]],
        }

        opt = select_option(booking_methods, booking_menu)

        booking_methods[opt](**input_kwargs(booking_kwargs[opt]))

    def menu():
        menu_menu = """RESTAURANT MENU:
  1. Add item to menu
  2. Show menu"""

        menu_methods = {
            1: partial(add_item_to_menu, conn=conn),
            2: Orders(conn).menu,
        }

        menu_kwargs = {
            1: (
                ["name", str],
                ["price", int],
            ),
            2: None,
        }

        opt = select_option(menu_methods, menu_menu)

        menu_methods[opt](**input_kwargs(menu_kwargs[opt]))

    def order():
        order_menu = """ORDER MENU:
  1. Show menu
  2. Create an order
  3. Pay the order"""

        order_methods = {
            1: Orders(conn).menu,
            2: Orders(conn).add,
            3: Orders(conn).pay,
        }

        order_kwargs = {
            1: [],
            2: [["table_number", int], ["items", str]],
            3: [["order_id", int]],
        }

        opt = select_option(order_methods, order_menu)

        order_methods[opt](**input_kwargs(order_kwargs[opt]))

    def kitchen():

        kitchen_menu = """ORDER MENU:
  1. Show orders
  2. Update order status"""

        kitchen_methods = {
            1: Kitchen(conn).orders,
            2: Kitchen(conn).update_status,
        }

        kitchen_kwargs = {
            1: None,
            2: [["order_id", int]],
        }

        opt = select_option(kitchen_methods, kitchen_menu)

        kitchen_methods[opt](**input_kwargs(kitchen_kwargs[opt]))

    options = {
        1: booking,
        2: menu,
        3: order,
        4: kitchen,
    }

    options[opt]()

    return


# ENDFILE
