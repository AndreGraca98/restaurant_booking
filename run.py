import logging

from booking_parser import get_parser
from src.db import Database, prepare_database
from src.log import add_console_handler, set_log_cfg
from src.options import options

prodLogger = logging.getLogger(__name__)
add_console_handler(prodLogger)


def main():
    parser = get_parser()
    args = parser.parse_args()

    set_log_cfg(args.log_file, args.log_level.upper())

    prodLogger.info("Starting restaurant management system ...")

    prepare_database(clean=args.clean)

    with Database() as db:
        while True:
            try:
                options(db.conn)

                print("»" * 80)
            except KeyboardInterrupt:
                print("\nExiting...")
                break


if __name__ == "__main__":
    main()
