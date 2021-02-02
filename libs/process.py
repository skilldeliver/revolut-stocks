from libs.exchange_rates import populate_exchange_rates
from libs.calculators.default import calculate_sales, calculate_remaining_purchases, calculate_dividends, calculate_dividends_tax
from libs.csv import export_statements, export_app8_part1, export_app5_table2, export_app8_part4_1
from libs.xml import export_to_xml
import libs.parsers

import sys
import os
import logging
import importlib
from pkgutil import iter_modules

logger = logging.getLogger("process")

supported_parsers = {}
if getattr(sys, "frozen", False):
    import libs.parsers.revolut as revolut
    import libs.parsers.trading212 as trading212
    import libs.parsers.csv as csv

    supported_parsers = {"revolut": revolut.Parser, "trading212": trading212.Parser, "csv": csv.Parser}
else:
    parser_modules = [mod.name for mod in iter_modules(libs.parsers.__path__, "libs.parsers.")]
    for parser_module in parser_modules:
        if not parser_module.endswith("parser"):
            parser = importlib.import_module(parser_module).Parser
            parser_name = parser_module.split(".")[-1]
            supported_parsers[parser_name] = parser


def get_unsupported_parsers(parser_names):
    unsupported_parsers = []
    for parser_name in parser_names:
        if parser_name not in supported_parsers:
            unsupported_parsers.append(parser_name)
    return unsupported_parsers


def calculate_win_loss(sales):
    win_loss = 0
    win_loss_in_currency = 0
    for sale in sales:
        win_loss += sale["profit"] + sale["loss"]
        win_loss_in_currency += sale["profit_in_currency"] + sale["loss_in_currency"]
    return win_loss, win_loss_in_currency


def merge_dict_of_dicts(parser_statements):
    merged_dict = {}
    for _, statements in parser_statements.items():
        for dict_key, dict_list in statements.items():
            if dict_key in merged_dict:
                merged_dict[dict_key].extend(dict_list)
                continue

            merged_dict[dict_key] = dict_list

    return merged_dict


def merge_dict_of_lists(parser_statements):
    merged_list = []
    for _, statements in parser_statements.items():
        merged_list.extend(statements)

    return merged_list


def for_each_parser(func, iter_arg_name, statements, combine, filename=None, output_dir=None, **kwargs):
    if combine:
        merged_statements = None

        if isinstance(next(iter(statements.values())), dict):
            merged_statements = merge_dict_of_dicts(statements)

        if isinstance(next(iter(statements.values())), list):
            merged_statements = merge_dict_of_lists(statements)

        if filename is not None:
            kwargs["file_path"] = os.path.join(output_dir, filename)
            os.makedirs(output_dir, exist_ok=True)

        return func(**{iter_arg_name: merged_statements}, **kwargs)
    else:
        result = {}
        for parser_name, parser_statements in statements.items():
            if filename is not None:
                if len(statements) > 1:
                    parser_output_dir = os.path.join(output_dir, parser_name)
                    kwargs["file_path"] = os.path.join(parser_output_dir, filename)
                    os.makedirs(parser_output_dir, exist_ok=True)
                else:
                    kwargs["file_path"] = os.path.join(output_dir, filename)
                    os.makedirs(output_dir, exist_ok=True)

            result[parser_name] = func(**{iter_arg_name: parser_statements}, **kwargs)

        return result


def get_unsupported_activity_types(parser_statements):
    unsupported_activity_types = {}
    for parser_name, statements in parser_statements.items():
        parser_unsupported_activity_types = supported_parsers[parser_name].get_unsupported_activity_types(statements)
        if parser_unsupported_activity_types:
            unsupported_activity_types[parser_name] = parser_unsupported_activity_types
    return unsupported_activity_types


def process(input_dir, output_dir, parser_names, use_bnb, in_currency=False):
    logger.debug(f"Supported parsers: [{supported_parsers}]")

    unsupported_parsers = get_unsupported_parsers(parser_names)
    if unsupported_parsers:
        logger.error(f"Unsupported parsers: {unsupported_parsers}.")
        raise SystemExit(1)

    logger.info(f"Parsing statement files.")
    statements = {}
    for parser_name in parser_names:
        parser_input_dir = input_dir
        if len(parser_names) > 1:
            parser_input_dir = os.path.join(parser_input_dir, parser_name)

        statements[parser_name] = supported_parsers[parser_name](parser_input_dir).parse()

        if not statements[parser_name]:
            logger.error(f"Not activities found with parser[{parser_name}]. Please, check your statement files.")
            raise SystemExit(1)

    logger.info(f"Generating statements file.")
    for_each_parser(
        export_statements,
        "statements",
        statements,
        False,
        filename="statements.csv",
        output_dir=output_dir,
    )

    logger.info(f"Populating exchange rates.")
    for_each_parser(populate_exchange_rates, "statements", statements, False, use_bnb=use_bnb)

    logger.info(f"Calculating dividends information.")
    dividends = for_each_parser(calculate_dividends, "statements", statements, False)
    dividend_taxes = for_each_parser(calculate_dividends_tax, "dividends", dividends, True)

    logger.info(f"Generating [app8-part4-1.csv] file.")
    export_app8_part4_1(os.path.join(output_dir, "app8-part4-1.csv"), dividend_taxes)

    parsers_calculations = None
    merged_sales = None
    remaining_purchases = None

    unsupported_activity_types = get_unsupported_activity_types(statements)

    if len(unsupported_activity_types.keys()) == 0:
        logger.info(f"Calculating sales information.")
        parsers_calculations = for_each_parser(calculate_sales, "statements", statements, False)

        logger.info(f"Generating [app5-table2.csv] file.")
        sales = {parser_name: parser_calculations[0] for parser_name, parser_calculations in parsers_calculations.items()}
        purchases = {parser_name: parser_calculations[1] for parser_name, parser_calculations in parsers_calculations.items()}

        merged_sales = merge_dict_of_lists(sales)
        remaining_purchases = for_each_parser(calculate_remaining_purchases, "purchases", purchases, True)

        for_each_parser(
            export_app5_table2,
            "sales",
            sales,
            True,
            filename="app5-table2.csv",
            output_dir=output_dir,
        )

    logger.info(f"Generating [dec50_2020_data.xml] file.")
    export_to_xml(
        os.path.join(output_dir, "dec50_2020_data.xml"),
        dividend_taxes,
        merged_sales if merged_sales is not None else None,
        remaining_purchases if remaining_purchases is not None else None,
    )

    if remaining_purchases is not None:
        logger.info(f"Generating [app8-part1.csv] file.")
        export_app8_part1(os.path.join(output_dir, "app8-part1.csv"), remaining_purchases)

    if merged_sales is not None:
        win_loss, win_loss_in_currency = calculate_win_loss(merged_sales)
        if in_currency:
            logger.info(f"Profit/Loss: {win_loss_in_currency} USD.")

        logger.info(f"Profit/Loss: {win_loss} lev.")

    if len(unsupported_activity_types.keys()) > 0:
        logger.warning(f"Statements contain unsupported activity types: {unsupported_activity_types}. Only dividends related data was calculated.")