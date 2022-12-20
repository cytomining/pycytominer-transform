"""
Utility functions for pycytominer-transform
"""

import duckdb


# custom sort for resulting columns
def column_sort(value: str):
    """
    A custom sort for column values as a list.
    To be used with sorted and Pyarrow tables.
    """

    # lowercase str which will be used for comparisons
    # to avoid any capitalization challenges
    value_lower = value.lower()

    # first sorted values (by list index)
    sort_first = [
        "tablenumber",
        "metadata_tablenumber",
        "imagenumber",
        "metadata_imagenumber",
        "objectnumber",
        "object_number",
    ]

    # middle sort value
    sort_middle = "metadata"

    # sorted last (by list order enumeration)
    sort_later = [
        "image",
        "cytoplasm",
        "cells",
        "nuclei",
    ]

    # if value is in the sort_first list
    # return the index from that list
    if value_lower in sort_first:
        return sort_first.index(value_lower)

    # if sort_middle is anywhere in value return
    # next index value after sort_first values
    if sort_middle in value_lower:
        return len(sort_first)

    # if any sort_later are found as the first part of value
    # return enumerated index of sort_later value (starting from
    # relative len based on the above conditionals and lists)
    if any(value_lower.startswith(val) for val in sort_later):
        for _k, _v in enumerate(sort_later, start=len(sort_first) + 1):
            if value_lower.startswith(_v):
                return _k

    # else we return the total length of all sort values
    return len(sort_first) + len(sort_later) + 1


def duckdb_with_sqlite() -> duckdb.DuckDBPyConnection:
    """
    Creates a DuckDB connection with the
    sqlite_scanner installed and loaded.

    Returns:
        duckdb.DuckDBPyConnection
    """

    return duckdb.connect().execute(
        """
        /* install and load sqlite plugin for duckdb */
        INSTALL sqlite_scanner;
        LOAD sqlite_scanner;
        """
    )
