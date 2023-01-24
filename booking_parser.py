import argparse
from pathlib import Path


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Syncronize files between two directories"
    )

    def add_common_arguments(parser):
        # parser.add_argument(
        #     "--log-path",
        #     "--log-file",
        #     type=str,
        #     nargs="?",
        #     dest="log_file",
        #     const=str(Path(__file__).parent / ".restaurant.log"),
        #     default=None,
        #     help="Log file or directory path. If --log-file is not specified it will only display log to console. If using --log-file without a path it will use the current directory. Otherwise it will use the specified path. ",
        # )
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

    add_common_arguments(parser)

    return parser


# ENDFILE
