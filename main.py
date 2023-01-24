import argparse
import logging
import os
import time
from pathlib import Path

from src.db import Database, Table
from src.log import set_log_cfg
from src.restaurant import Bookings, create_bookings, create_menu, create_tables

rootLogger = logging.getLogger(__name__)


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Syncronize files between two directories"
    )

    def add_common_arguments(parser):
        parser.add_argument(
            "--log-path",
            "--log-file",
            type=str,
            dest="log_file",
            default=str(Path(__file__).parent / ".restaurant.log"),
            help="Log file or directory path. Defaults to /home/user/.folders_sync.log",
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

        # TODO: implement exclude pattern
        # parser.add_argument(
        #     "--exclude", type=str, help="Exclude files matching this pattern"
        # )

    add_common_arguments(parser)

    return parser


def main():
    parser = get_parser()
    args = parser.parse_args("--log-level DEBUG".split())

    set_log_cfg(args.log_file, args.log_level)

    rootLogger.info("Starting restaurant ...")

    # Database().delete().close()
    with Database() as db:
        # menu = Table("menu", db.conn)
        # menu.delete()
        # menu.delete("price > 400")
        # menu.update('"name" LIKE "% Burger"', col="price", value=500)
        # menu.update('name = "Pork Burger"', col="price", value=550)

        # create_menu(db)
        # create_tables(db)
        # create_bookings(db)

        # menu = Table("menu", db.conn)
        # rootLogger.info(menu.show())

        # Bookings(db).show().update(
        #     client_name="John",
        #     client_contact="+351 123 456 789",
        #     reservation_datetime="2023-02-01 13:00:01",
        # ).show()
        # Bookings(db).show().update(2, reservation_datetime="2023-02-01 18:00:00").show()
        Bookings(db).show()
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
