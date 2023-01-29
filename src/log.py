import logging
from pathlib import Path
from typing import Union

__all__ = ["add_console_handler", "set_log_cfg", "fmt"]
fmt = "%(asctime)s : %(levelname)-8s :: %(name)s::%(funcName)s::line_%(lineno)-3d : %(message)s"


def add_console_handler(logger):

    consoleHandler = logging.StreamHandler()
    logFormatter = logging.Formatter(fmt)
    consoleHandler.setFormatter(logFormatter)
    logger.addHandler(consoleHandler)

    # print(logger.handlers)
    # if False:
    #     logger.propagate = False


def set_log_cfg(log_file: Union[str, Path] = None, log_level: str = "INFO"):
    # Create directory if it does not exist

    if log_file is None:
        logging.basicConfig(
            level=log_level.upper(),
            format=fmt,
        )
        return

    log_file = Path(log_file).resolve()
    if log_file.is_dir():
        log_file = log_file / ".restaurant.log"

    log_file.parent.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        filename=str(log_file),
        filemode="a",
        level=log_level.upper(),
        format=fmt,
    )

    print(f"Logging to {log_file} ...")


# ENDFILE
