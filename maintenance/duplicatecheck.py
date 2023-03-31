# coding: utf-8


import argparse
import configparser
import contextlib as ctlib
import logging
import os
import pathlib

import pandas as pd
import psycopg2

from dotenv import load_dotenv

load_dotenv()


HOST = os.getenv("DATAPOOL_HOST")
PORT = os.getenv("DATAPOOL_PORT")
DATABASE = os.getenv("DATAPOOL_DATABASE")
USER = os.getenv("DATAPOOL_USER")
PASSWORD = os.getenv("DATAPOOL_PASSWORD")


def load_logger(log_file):
    """Set up a logger to log events to a file.

    Args:
        file_destination: The directory where the log file should be saved.
        filename: The name of the log file.

    """
    logger = logging.getLogger("duplicate_check")
    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    fh.setFormatter(formatter)
    logger.addHandler(fh)


@ctlib.contextmanager
def open_postgres(
    host: str = HOST,
    database: str = DATABASE,
    user: str = USER,
    password: str = PASSWORD,
    port: str = PORT,
) -> psycopg2.extensions.connection:
    """A context manager to create a connection to a PostgreSQL database.

    Returns:
        A connection object to a PostgreSQL database.

    """
    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
        port=port,
    )
    try:
        yield conn
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()


def query(sql_query: str) -> pd.DataFrame:
    """Execute a SQL query on a PostgreSQL database and return the result as a Pandas DataFrame.

    Args:
        sql_query: The SQL query to execute.

    Returns:
        The result of the query as a Pandas DataFrame.

    """
    with open_postgres() as conn:
        cur = conn.cursor()
        cur.execute(sql_query)
        res = cur.fetchall()
        cur.close()
        df = pd.DataFrame(res, columns=["time", "value", "unit", "variable", "source"])
        return df
        # return pd.read_sql_query(sql_query, conn)


def sql_query(source_name: str, variable_name: str) -> str:
    """Create a SQL query that retrieves data from a specific source and variable.

    Args:
        source_name: The name of the source.
        variable_name: The name of the variable.

    Returns:
        A SQL query as a string.

    """
    return f"""
    SELECT
        signal.timestamp,
        value,
        unit,
        variable.name,
        source.name
    FROM signal
        INNER JOIN variable ON signal.variable_id = variable.variable_id
        INNER JOIN source ON signal.source_id = source.source_id
    WHERE source.name = '{source_name}' AND variable.name = '{variable_name}'
    ORDER BY signal.timestamp ASC

    """


def get_data(source_name: str, variable_name: str) -> pd.DataFrame:
    """Retrieve data from a specific source and variable using a SQL query.

    Args:
        source_name: The name of the data source.
        variable_name: The name of the variable.

    Returns:
        The data as a Pandas DataFrame.

    """
    sql_qry = sql_query(source_name, variable_name)
    return query(sql_qry)


def duplicate_check(df: pd.DataFrame) -> pd.DataFrame:
    """Check for duplicate rows in a Pandas DataFrame.

    Args:
        df (pandas.DataFrame): The DataFrame to check for duplicates.

    Returns:
        pandas.DataFrame: A DataFrame containing only the duplicate rows.

    """
    return df[df.duplicated()]


def main(args: argparse.Namespace) -> None:
    input_path = pathlib.Path(args.sourcedirectory)
    ini_filename = "datapool_duplicates_config.ini"
    output_path = pathlib.Path(args.targetdirectory)
    log_filename = "datapool_duplicates.log"

    load_logger(output_path / log_filename)

    config = configparser.ConfigParser()
    config.read(input_path / ini_filename)

    test_candidates = config.items("DEFAULT")

    for source, variable in test_candidates:
        df = get_data(source, variable)
        res = duplicate_check(df)
        if not res.empty:
            logging.info(msg=res)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-sd", "--sourcedirectory", default="/path/to/input")
    parser.add_argument("-td", "--targetdirectory", default="/path/to/output")

    args = parser.parse_args()

    main(args)
