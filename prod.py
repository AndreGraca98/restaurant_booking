import logging

from booking_parser import get_parser
from src.db import Database, create_restaurant_menu, create_restaurant_tables
from src.log import add_console_handler, set_log_cfg
from src.options import options

prodLogger = logging.getLogger(__name__)
add_console_handler(prodLogger)


def main():
    parser = get_parser()
    args = parser.parse_args()

    set_log_cfg(".prod.log", args.log_level.upper())

    prodLogger.info("Starting restaurant management system ...")

    with Database() as db:
        if args.clean:
            prodLogger.debug("Deleting current database tables")
            db.delete().create()

        prodLogger.debug("Creating restaurant tables")
        create_restaurant_tables(db.conn)

        prodLogger.debug("Creating restaurant menu")
        create_restaurant_menu(db.conn)

        prodLogger.debug("Database ready")
        while True:
            try:
                options(db.conn)

                print("»" * 40 + "«" * 40)
                print("»" * 40 + "«" * 40 + "\n")
            except KeyboardInterrupt:
                prodLogger.warn("Ending session...")
                break


if __name__ == "__main__":
    main()
