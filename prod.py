import logging

from booking_parser import get_parser
from src.bookings import create_dummy_bookings
from src.clients import create_client
from src.rest_tables import create_restaurant_tables
from src.db import Database, create_restaurant_menu
from src.log import add_console_handler, set_log_cfg
from src.options import options

prodLogger = logging.getLogger(__name__)
add_console_handler(prodLogger)


def main():
    parser = get_parser()
    args = parser.parse_args()

    set_log_cfg(".prod.log", args.log_level)

    prodLogger.info("Starting restaurant management system ...")

    with Database("restaurant_prod", args.clean) as db:
        prodLogger.debug("Creating restaurant tables")
        create_restaurant_tables(db.conn)

        # prodLogger.debug("Creating clients")
        # create_client(db.conn)

        prodLogger.debug("Creating restaurant menu")
        create_restaurant_menu(db.conn)

        # prodLogger.debug("Creating dummy bookings")
        # create_dummy_bookings(db.conn)

        prodLogger.debug("Database ready...")

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
