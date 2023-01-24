import sqlite3
from typing import Dict, List

try:
    from .bookings import Bookings
except ImportError:
    exit("Please run this script from the root directory")


class InvalidOptionError(Exception):
    """Invalid option exception"""


def select_option(options: Dict[int, str], menu: str) -> int:
    print(menu)
    try:
        option = int(input("Select option: "))
        if option not in options.keys():
            raise InvalidOptionError

    except (ValueError, InvalidOptionError):
        print("\nPlease choose a valid option.")
        return select_option(options, menu)

    except KeyboardInterrupt:
        print("\nExiting...")
        exit(0)

    return option


def input_kwargs(args: List[str]) -> Dict[str, str]:
    return {k: input(f"{k}: ") for k in args}


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

        book_a_table = Bookings(conn).add
        change_booking = Bookings(conn).update
        cancel_booking = ...
        show_all_bookings = Bookings(conn).show
        show_tables_for_date = ...

        booking_options = {
            1: book_a_table,
            2: change_booking,
            3: cancel_booking,
            4: show_all_bookings,
            5: show_tables_for_date,
        }

        booking_kwargs = {
            1: (
                "reservation_datetime",
                "table_number",
                "client_name",
                "client_contact",
            ),
            2: (
                "booking_id",
                "client_name",
                "client_contact",
                "reservation_datetime",
                "table_number",
            ),
            3: ...,
            4: [],
            5: ...,
        }

        opt = select_option(booking_options, booking_menu)

        booking_options[opt](**input_kwargs(booking_kwargs[opt]))

    def menu():
        add_item_to_menu = ...

    def order():
        show_menu = ...
        order_from_menu = ...
        pay_order = ...

    def kitchen():
        show_orders = ...
        update_order_status = ...

    options = {
        1: booking,
        2: menu,
        3: order,
        4: kitchen,
    }

    options[opt]()

    return


# ENDFILE
