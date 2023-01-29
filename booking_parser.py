import argparse
from pathlib import Path


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Booking system for a restaurant.")

    def add_common_arguments(parser):
        class UpperAction(argparse.Action):
            def __call__(self, parser, namespace, values: str, option_string=None):
                setattr(
                    namespace,
                    self.dest,
                    int(values) if values.isnumeric() else values.upper(),
                )

        parser.add_argument(
            "--log-lvl",
            "--log-level",
            type=str,
            dest="log_level",
            default="INFO",
            action=UpperAction,
            help="Log level. Defaults to INFO.",
        )

        parser.add_argument(
            "-c", "--clean", action="store_true", help="Clean database tables"
        )

    add_common_arguments(parser)

    return parser


if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()

    print("Args:", args)

    import logging

    print(logging._nameToLevel)

# ENDFILE
