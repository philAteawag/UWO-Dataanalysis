# coding: utf-8


import pendulum
import argparse
import pathlib
import sqlite3
import contextlib as ctlib

import pandas as pd


@ctlib.contextmanager
def open_sqlite(db_file):
    conn = sqlite3.connect(db_file)
    try:
        yield conn
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()


def _query_plain(conn, sql_query):
    cur = conn.cursor()
    cur.execute(sql_query)
    res = cur.fetchall()
    cur.close()
    return res


def _query_df(conn, sql_query, query_args):
    return pd.read_sql_query(
        sql_query,
        conn,
        params=query_args,
    )


def query(db_file: pathlib.PosixPath, sql_query: str, return_dataframe: bool=True, query_args: list[str]=None):

    with open_sqlite(db_file) as conn:
        if return_dataframe:
            return _query_df(conn, sql_query, query_args)
        else:
            return _query_plain(conn, sql_query)
        

def example_query_0(db_file: str) -> pd.DataFrame:
    """List all special values."""

    example_query = f"""

    SELECT * FROM special_value_definition;

    """

    return query(db_file, example_query)


def example_query_1(db_file: str) -> pd.DataFrame:
    """List all datapoints recorded between 'start_date' and 'end_date' at location '11e_russikerstr'."""

    end_date = pendulum.datetime(year=2021, month=9, day=30)
    start_date = end_date.subtract(days=1)

    location = "rub_morg"

    example_query = f"""

    SELECT
        signal.timestamp,
        value,
        unit,
        variable.name,
        source_type.name,
        source.name
    FROM signal
        INNER JOIN site ON signal.site_id = site.site_id
        INNER JOIN variable ON signal.variable_id = variable.variable_id
        INNER JOIN source ON signal.source_id = source.source_id
        INNER JOIN source_type ON source.source_type_id = source_type.source_type_id
    WHERE site.name = '{location}'
        AND signal.timestamp >= '{start_date}'
        AND signal.timestamp <= '{end_date}';

    """

    return query(db_file, example_query)


def example_query_2(db_file: str) -> pd.DataFrame:
    """Get all sources that have recordings for variable 'water_temperature'."""

    variable = "water_temperature"

    example_query = f"""

    WITH variable_ids as (
        SELECT variable_id FROM variable WHERE variable.name = '{variable}'
    ), source_ids as (
        SELECT DISTINCT source_id FROM signal
        WHERE signal.variable_id IN (
            SELECT variable_id FROM variable_ids
        )
    )
    SELECT source.name from source
    WHERE source.source_id IN (
        SELECT source_id from source_ids
    )
    
    """

    return query(db_file, example_query)


def example_query_3(db_file: str) -> pd.DataFrame:
    """Get the latest signal of all sources from type 'DS18B20'."""

    type = "DS18B20"

    example_query = f"""

    SELECT
        source.name,
        MAX(signal.timestamp)
    FROM signal
        INNER JOIN source ON signal.source_id = source.source_id
        INNER JOIN source_type ON source.source_type_id = source_type.source_type_id
    WHERE source_type.name = '{type}'
    GROUP BY source.name
    ORDER BY MAX(signal.timestamp) ASC;
        
    """

    return query(db_file, example_query)


def example_query_4(db_file: str) -> pd.DataFrame:
    """Count the entries for all the weekly aggregated data within the database and group by source and variable."""

    example_query = f"""
    
    WITH count_table AS (
        SELECT
            COUNT(variable_id) as count,
            variable_id,
            source_id,
            STRFTIME('%W', timestamp) as date_trunc
        FROM signal
        GROUP BY
            STRFTIME('%W', timestamp),
            variable_id,
            source_id
    )
    SELECT
        count_table.count AS value_count,
        variable.name AS variable_name,
        source.name AS source_name,
        count_table.date_trunc AS date_trunc
    FROM count_table
    INNER JOIN variable ON variable.variable_id = count_table.variable_id
    INNER JOIN source ON source.source_id = count_table.source_id
    ORDER BY date_trunc DESC;
    
    """

    return query(db_file, example_query)


def example_query_5(db_file: pathlib.PosixPath, cl_file: pathlib.PosixPath) -> pd.DataFrame:
    """All data from package A1."""

    content_a1 = pd.read_csv(cl_file, sep=";")

    filter_a1 = content_a1[content_a1["A1"] == 1]
    source_names_a1 = filter_a1["source"].to_list()

    example_query = f"""

    SELECT
        signal.timestamp,
        value,
        unit,
        variable.name,
        source_type.name,
        source.name
    FROM signal
        INNER JOIN site ON signal.site_id = site.site_id
        INNER JOIN variable ON signal.variable_id = variable.variable_id
        INNER JOIN source ON signal.source_id = source.source_id
        INNER JOIN source_type ON source.source_type_id = source_type.source_type_id
    WHERE source.name IN (?{",?" * (len(source_names_a1)-1)})

    """

    return query(db_file, example_query, query_args=source_names_a1)


def example_query_6(db_file: pathlib.PosixPath, cl_file: pathlib.PosixPath) -> pd.DataFrame:
    """All data from package A2."""

    content_a2 = pd.read_csv(cl_file, sep=";")

    filter_a2 = content_a2[content_a2["A2"] == 1]
    source_names_a2 = filter_a2["source"].to_list()

    example_query = f"""

    SELECT
        signal.timestamp,
        value,
        unit,
        variable.name,
        source_type.name,
        source.name
    FROM signal
        INNER JOIN site ON signal.site_id = site.site_id
        INNER JOIN variable ON signal.variable_id = variable.variable_id
        INNER JOIN source ON signal.source_id = source.source_id
        INNER JOIN source_type ON source.source_type_id = source_type.source_type_id
    WHERE source.name IN (?{",?" * (len(source_names_a2)-1)})

    """

    return query(db_file, example_query, query_args=source_names_a2)


def example_query_7(db_file: pathlib.PosixPath, cl_file: pathlib.PosixPath) -> pd.DataFrame:
    """All data from package A3."""

    content_a3 = pd.read_csv(cl_file, sep=";")

    filter_a3 = content_a3[content_a3["A3"] == 1]
    source_names_a3 = filter_a3["source"].to_list()

    example_query = f"""

    SELECT
        signal.timestamp,
        value,
        unit,
        variable.name,
        source_type.name,
        source.name
    FROM signal
        INNER JOIN site ON signal.site_id = site.site_id
        INNER JOIN variable ON signal.variable_id = variable.variable_id
        INNER JOIN source ON signal.source_id = source.source_id
        INNER JOIN source_type ON source.source_type_id = source_type.source_type_id
    WHERE source.name IN (?{",?" * (len(source_names_a3)-1)})

    """

    return query(db_file, example_query, query_args=source_names_a3)


def example_query_8(db_file: pathlib.PosixPath, cl_file: pathlib.PosixPath) -> pd.DataFrame:
    """All data from package A4."""

    content_a4 = pd.read_csv(cl_file, sep=";")

    filter_a4 = content_a4[content_a4["A4"] == 1]
    source_names_a4 = filter_a4["source"].to_list()

    example_query = f"""

    SELECT
        signal.timestamp,
        value,
        unit,
        variable.name,
        source_type.name,
        source.name
    FROM signal
        INNER JOIN site ON signal.site_id = site.site_id
        INNER JOIN variable ON signal.variable_id = variable.variable_id
        INNER JOIN source ON signal.source_id = source.source_id
        INNER JOIN source_type ON source.source_type_id = source_type.source_type_id
    WHERE source.name IN (?{",?" * (len(source_names_a4)-1)})

    """

    return query(db_file, example_query, query_args=source_names_a4)


def main(args: argparse.Namespace) -> None:

    path_to_db = pathlib.Path(args.sourcedirectory)
    filename = args.filename
    content_list = args.contentlist

    db = path_to_db / filename
    cl = path_to_db / content_list

    print(example_query_0(db_file=db))
    # print(example_query_1(db_file=db))
    # print(example_query_2(db_file=db))
    # print(example_query_3(db_file=db))
    # print(example_query_4(db_file=db))
    # print(example_query_5(db_file=db, cl_file=cl))
    # print(example_query_6(db_file=db, cl_file=cl))
    # print(example_query_7(db_file=db, cl_file=cl))
    # print(example_query_8(db_file=db, cl_file=cl))


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-sd", "--sourcedirectory", default="/path/to/db_file")
    parser.add_argument("-fn", "--filename", default="dp.sqlite")
    parser.add_argument("-cl", "--contentlist", default="package_information.csv")

    args = parser.parse_args()

    main(args)
