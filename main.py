import argparse
import datetime
import logging
import os
import time
from pathlib import Path

from src.db import Database, Table, prepare_database
from src.log import add_console_handler, set_log_cfg
from src.bookings import Bookings, create_dummy_bookings

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


def bookings_example(db: Database):
    bookings = Bookings(db)

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
    ...


def main():
    parser = get_parser()
    args = parser.parse_args()
    print(args)
    # args = parser.parse_args("--log-level DEBUG".split())

    set_log_cfg(args.log_file, args.log_level.upper())

    rootLogger.info("Starting restaurant management system ...")

    prepare_database(clean=args.clean)

    with Database() as db:
        create_dummy_bookings(db)
        # bookings_example(db)

        # menu = Table("menu", db.conn)
        # menu.delete()
        # menu.delete("price > 400")
        # menu.update('"name" LIKE "% Burger"', col="price", value=500)
        # menu.update('name = "Pork Burger"', col="price", value=550)
        # rootLogger.info(menu.show())

        # Bookings(db).show().update(
        #     client_name="John",
        #     client_contact="+351 123 456 789",
        #     reservation_datetime="2023-02-01 13:00:01",
        # ).show()
        # Bookings(db).show().update(2, reservation_datetime="2023-02-01 18:00:00").show()
        # Bookings(db).show().show_available_tables("2023-02-01 12:00:00")
        # Bookings(db).show().delete(1)
        # Bookings(db).show().add(
        #     client_name="André Graça",
        #     client_contact="+351 967 515 355",
        #     reservation_datetime="2023-02-01 12:30:00",
        #     table_id=1,
        # ).show()

        ...


if __name__ == "__main__":
    main()
