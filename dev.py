import datetime
import logging
import sqlite3

from src.bookings import Bookings, create_dummy_bookings
from src.db import Database, Table, prepare_database
from src.kitchen import Kitchen
from src.log import add_console_handler, set_log_cfg
from src.orders import Orders, OrderStatus, create_dummy_orders

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

    # See menu
    orders.menu()


def kitchen_example(conn: sqlite3.Connection):
    kitchen = Kitchen(conn)

    # Show kitchen orders
    kitchen.show()

    # Get orders sort by timestamp
    kitchen.orders()

    # Update order status
    kitchen.update_status(1, OrderStatus.COOKING)
    kitchen.update_status(2, OrderStatus.SERVED)
    kitchen.update_status(3, OrderStatus.PENDING)
    kitchen.update_status(4, OrderStatus.READY)

    kitchen.orders()


def main():
    parser = get_parser()
    args = parser.parse_args()
    # print(args)

    # args = parser.parse_args("--log-level DEBUG".split())

    set_log_cfg(".dev.log", args.log_level.upper())

    devLogger.info("Starting restaurant management system ...")

    prepare_database(clean=args.clean)

    with Database() as db:
        create_dummy_bookings(db.conn)
        bookings_example(db.conn)
        orders_example(db.conn)
        kitchen_example(db.conn)

        ...


if __name__ == "__main__":
    main()
