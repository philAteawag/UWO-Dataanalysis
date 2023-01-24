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


def connect(db_file):
    return sqlite3.connect(db_file)


def _query_plain(conn, sql_query):
    cur = conn.cursor()
    cur.execute(sql_query)
    res = cur.fetchall()
    cur.close()
    return res


def _query_df(conn, sql_query):
    return pd.read_sql_query(
        sql_query,
        conn,
    )


def query(db_file, sql_query, return_dataframe=True):
    with open_sqlite(db_file) as conn:
        if return_dataframe:
            return _query_df(conn, sql_query)
        else:
            return _query_plain(conn, sql_query)


def example_query_1(db_file: str) -> pd.DataFrame:
    """List all datapoints recorded during the last 30 days at location '11e_russikerstr'."""

    end_time = pendulum.now(tz="Europe/Zurich")
    start_time = end_time.subtract(days=30)

    location = "11e_russikerstr"

    example_query = f"""

    SELECT
        signal.timestamp,
        value,
        unit,
        parameter.name,
        source_type.name,
        source.name
    FROM signal
        INNER JOIN site ON signal.site_id = site.site_id
        INNER JOIN parameter ON signal.parameter_id = parameter.parameter_id
        INNER JOIN source ON signal.source_id = source.source_id
        INNER JOIN source_type ON source.source_type_id = source_type.source_type_id
    WHERE site.name = '{location}'
        AND signal.timestamp >= '{start_time}'
        AND signal.timestamp <= '{end_time}';

    """

    return query(db_file, example_query)


def example_query_2(db_file: str) -> pd.DataFrame:
    """Get all sources that have recordings for parameter 'water_temperature'."""

    parameter = "water_temperature"

    example_query = f"""

    WITH parameter_ids as (
        SELECT parameter_id FROM parameter WHERE parameter.name = '{parameter}'
    ), source_ids as (
        SELECT DISTINCT source_id FROM signal
        WHERE signal.parameter_id IN (
            SELECT parameter_id FROM parameter_ids
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


def main(args: argparse.Namespace) -> None:

    path_to_db = pathlib.Path(args.sourcedirectory)

    db = path_to_db / "dp_copy.sqlite"

    print(query(db, "SELECT name FROM source_type"))

    print(example_query_1(db_file=db))
    print(example_query_2(db_file=db))
    print(example_query_3(db_file=db))


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-sd", "--sourcedirectory", default="/path/to/db_file")

    args = parser.parse_args()

    main(args)
