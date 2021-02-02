import glob
import os
from datetime import datetime
import logging

from libs import BNB_DATE_FORMAT

logger = logging.getLogger("utils")


def list_statement_files(dir, file_extension):
    statement_files = []

    if not os.path.exists(dir):
        raise Exception(f"Statement directory[{dir}] doesn't exists.")

    expresion = os.path.join(dir, "**", f"*.{file_extension}")
    for file in glob.glob(expresion, recursive=True):
        if not os.path.isfile(file):
            continue
        statement_files.append(file)

    return statement_files


def humanize_date(list_object):
    result = []
    for elements in list_object:
        item = {}
        for key, value in elements.items():
            if isinstance(value, datetime):
                item[key] = value.strftime(BNB_DATE_FORMAT)
                continue

            item[key] = value

        result.append(item)
    return result


def get_parsers(supported_parsers, parsers, input_dir=None):
    if not parsers:
        parsers = ["revolut"]

    if len(parsers) == 1:
        if parsers[0] not in supported_parsers:
            return [], [parsers[0]]

        if input_dir is None:
            logger.error(f"No input direcory provided. Please, use -i argument.")
            raise SystemExit(1)

        return [(parsers[0], supported_parsers[parsers[0]].Parser(input_dir))], []

    supported_parsers = []
    unsupported_parsers = []
    for parser in parsers:
        parser_name, input_dir = parsers.split(":")
        if parser not in supported_parsers:
            unsupported_parsers.append(parser_name)
            continue

        supported_parsers.append((parser_name, supported_parsers[parser].Parser(input_dir)))

    return supported_parsers, unsupported_parsers


def get_unsupported_activity_types(supported_parsers, statements):
    unsupported_activity_types = []
    for parser_name, statements in statements.items():
        parser_unsupported_activity_types = supported_parsers[parser_name].Parser.get_unsupported_activity_types(statements)
        if len(parser_unsupported_activity_types) > 0:
            unsupported_activity_types.append({"parser": parser_name, "unsupported_types": parser_unsupported_activity_types})

    return unsupported_activity_types