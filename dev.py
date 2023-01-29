import datetime
import logging
import sqlite3

from src.bookings import Bookings, create_dummy_bookings
from src.clients import Clients, create_client
from src.db import Database, Table, create_restaurant_menu
from src.kitchen import Kitchen
from src.log import add_console_handler, set_log_cfg
from src.orders import Orders, create_dummy_orders
from src.rest_tables import RestaurantTables, create_restaurant_tables

devLogger = logging.getLogger(__name__)
add_console_handler(devLogger)

from booking_parser import get_parser


def bookings_example(conn: sqlite3.Connection):
    bookings = Bookings(conn)

    # Get all bookings
    bookings.show()

    # Book tables
    bookings.add(
        client_name="André Graça",
        client_contact="+351 967 515 355",
        reservation_datetime="2023-02-01 12:30:00",
        table_number=4,
    )
    bookings.show()

    bookings.add(
        reservation_datetime="2023-02-02 12:30:00",
        table_number=11,
    )
    bookings.show()

    # Changing bookings
    bookings.update(
        client_name="André Graça", client_contact="+351 967 515 355", table_number=2
    )
    bookings.update(
        client_name="André Graça", client_contact="+351 967 515 355", table_number=3
    )
    bookings.show()

    # Cancel bookings
    bookings.delete(client_name="André Graça", client_contact="+351 967 515 355")
    bookings.show()

    # See available tables
    bookings.show_available_tables(
        datetime.datetime.now().isoformat(sep=" ", timespec="seconds")
    )
    bookings.show_available_tables("2023-02-01 13:00:00")


def orders_example(conn: sqlite3.Connection):
    orders = Orders(conn)

    # See menu
    orders.menu()

    # Get all orders
    orders.show()

    # Add orders
    orders.add(11, [1, 3, 4, "Fries"])
    orders.add(1, [1, 2, "chicken", "salad", 7])
    orders.add(3, [5, "coke"])
    orders.add(2, [1, 2, "chicken", "salad", 7])
    orders.add(1, [999, "item that doesnt exist"])
    orders.show()

    # Delete orders
    orders.delete(2)
    orders.show()
    orders.delete(order_datetime="2023-01-24 09:23:11.618")
    orders.show()


def kitchen_example(conn: sqlite3.Connection):
    kitchen = Kitchen(conn)

    # Show kitchen orders
    kitchen.show()

    # Get orders sort by timestamp
    kitchen.orders()

    # Update order status
    kitchen.update_status(1)
    kitchen.update_status(1)
    kitchen.update_status(1)
    kitchen.update_status(2)
    kitchen.update_status(2)
    kitchen.update_status(2)
    kitchen.update_status(3)
    kitchen.update_status(3)
    kitchen.update_status(3)
    kitchen.update_status(4)
    kitchen.update_status(4)
    kitchen.update_status(4)

    kitchen.orders()


def main():
    parser = get_parser()
    args = parser.parse_args()

    set_log_cfg(".dev.log", args.log_level.upper())

    devLogger.info("Starting restaurant management system ...")

    with Database("restaurant_dev") as db:
        # if args.clean:
        #     devLogger.debug("Deleting current database tables")
        #     db.delete().create()

        # create_restaurant_tables(db.conn)
        # create_client(db.conn)

        # devLogger.debug("Creating restaurant menu")
        # create_restaurant_menu(db.conn)

        # devLogger.debug("Database ready")

        # r_tables = RestaurantTables(db.conn)
        # r_tables.show()
        # r_tables.update(11, "table_number", 12)
        # r_tables.show()

        # clients = Clients(db.conn)
        # clients.add("andré graça", "+351 967 51 53 55")

        # bookings = Bookings(db.conn)
        # bookings.add(
        #     reservation_datetime="2023-02-01 16:00:00",
        #     table_number=11,
        #     client_name="someone less cool",
        #     # client_name="andré graça",
        #     # client_contact="+351 967 51 53 55",
        # )

        # print(str(bookings))
        create_dummy_bookings(db.conn)
        # bookings_example(db.conn)
        # orders_example(db.conn)
        # kitchen_example(db.conn)

        ...


if __name__ == "__main__":
    main()
    # set_log_cfg(".dev.log", "DEBUG")

    # with Database("restaurant_dev") as db:
    #     table = Table("clients", db.conn)
    #     table.add(client_name="André Graça", client_contact="+351 967 515 355")

    #     print(str(table))
    ...
