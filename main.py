import argparse
import datetime
import logging
import os
import sqlite3
import time
from pathlib import Path
from typing import List, Union

from src.bookings import Bookings, create_dummy_bookings
from src.db import Database, Table, prepare_database
from src.kitchen import Kitchen
from src.log import add_console_handler, set_log_cfg
from src.orders import Orders, OrderStatus, create_dummy_orders

rootLogger = logging.getLogger(__name__)
add_console_handler(rootLogger)


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Syncronize files between two directories"
    )

    def add_common_arguments(parser):
        parser.add_argument(
            "--log-path",
            "--log-file",
            type=str,
            nargs="?",
            dest="log_file",
            const=str(Path(__file__).parent / ".restaurant.log"),
            default=None,
            help="Log file or directory path. If --log-file is not specified it will only display log to console. If using --log-file without a path it will use the current directory. Otherwise it will use the specified path. ",
        )
        parser.add_argument(
            "--log-lvl",
            "--log-level",
            type=str,
            dest="log_level",
            default="INFO",
            help="Log level. Defaults to INFO.",
        )
        parser.add_argument(
            "-d",
            "--dry",
            "--dry-run",
            action="store_true",
            dest="dry_run",
            help="Dry run the syncronization",
        )

        parser.add_argument("-c", "--clean", action="store_true", help="Clean database")

        # TODO: implement exclude pattern
        # parser.add_argument(
        #     "--exclude", type=str, help="Exclude files matching this pattern"
        # )

    add_common_arguments(parser)

    return parser


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
    orders.add(1, [1, 3, 4, "Fries"])
    orders.add(1, [1, 2, "chicken", "salad", 7])
    orders.add(2, [1, 2, "chicken", "salad", 7])
    orders.add(1, [999, "item that doesnt exist"])
    orders.show()

    # Delete orders
    orders.delete(1)
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
    kitchen.update_status(3, OrderStatus.SERVED)
    kitchen.update_status(4, OrderStatus.SERVED)

    kitchen.orders()


def main():
    parser = get_parser()
    args = parser.parse_args()
    # print(args)

    # args = parser.parse_args("--log-level DEBUG".split())

    set_log_cfg(args.log_file, args.log_level.upper())

    rootLogger.info("Starting restaurant management system ...")

    prepare_database(clean=args.clean)

    with Database() as db:
        create_dummy_bookings(db.conn)
        bookings_example(db.conn)
        orders_example(db.conn)
        kitchen_example(db.conn)

        ...


if __name__ == "__main__":
    main()
