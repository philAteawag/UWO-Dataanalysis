# coding: utf-8


import argparse
import contextlib as ctlib
import os
import pathlib
import json

import psycopg2

from dotenv import load_dotenv

load_dotenv()


HOST = os.getenv("DATAPOOL_HOST")
PORT = os.getenv("DATAPOOL_PORT")
DATABASE = os.getenv("DATAPOOL_DATABASE")
USER = os.getenv("DATAPOOL_USER")
PASSWORD = os.getenv("DATAPOOL_PASSWORD")


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


def query(sql_query: str) -> list:
    """Execute a SQL query on a PostgreSQL database and return the result.

    Args:
        sql_query: The SQL query to execute.

    Returns:
        The result of the query.

    """
    with open_postgres() as conn:
        cur = conn.cursor()
        cur.execute(sql_query)
        res = cur.fetchall()
        cur.close()
        return [i[0] for i in res]


def sql_sources_query() -> str:
    """Create a SQL query that retrieves all source names.

    Returns:
        A SQL query as a string.

    """
    return f"""
    SELECT DISTINCT name
    FROM source;

    """


def sql_variables_query(source_name: str) -> str:
    """Create a SQL query that retrieves all source names.

    Returns:
        A SQL query as a string.

    """
    return f"""
    SELECT DISTINCT 
        variable.name
    FROM signal
        INNER JOIN variable ON signal.variable_id = variable.variable_id
        INNER JOIN source ON signal.source_id = source.source_id
    WHERE source.name = '{source_name}';

    """


def get_sources() -> list:
    """Retrieve data from a specific source and variable using a SQL query.

    Returns:
        The data as a list.

    """
    sql_qry = sql_sources_query()
    return query(sql_qry)


def get_variables(source_name: str) -> list:
    """Retrieve all variables of a specific source using a SQL query.

    Args:
        source_name: source name.
    
    Returns:
        The variable names as a list.    
    
    """
    sql_qry = sql_variables_query(source_name)
    return query(sql_qry)


def main(args: argparse.Namespace) -> None:
    output_path = pathlib.Path(args.targetdirectory)
    json_filename = "datapool_sources_variables.json"

    sources = get_sources()

    source_variables = {}
    for source in sources:

        source_variables.update({source: get_variables(source)})

    json_object = json.dumps(source_variables)

    with open(output_path / json_filename, "w") as outfile:
        outfile.write(json_object)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-td", "--targetdirectory", default="/path/to/output")

    args = parser.parse_args()

    main(args)
