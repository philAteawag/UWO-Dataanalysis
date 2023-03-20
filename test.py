# coding: utf-8


"""Check how to merge files, check if data is consitent (read source data from life system and
compare to sqlite), check if only the data is in there that we want to publish, is there
unnecessary data, export some example images and check them, ...

"""


import argparse
import contextlib
import pathlib
import sqlite3
import random

import pandas as pd

from datapool_client import DataPool


COMBINATIONS = [
    {
        "filename": "data_UWO_2021-01_2022-01.sqlite",
        "source": "bl_ceb5f_58sbw_undermulistr",
        "parameter": "water_level",
        "start": "2019-04-11 08:54:00",
        "end": "2019-06-11 08:56:00",
        "picture_id": "126",
    },
    {
        "filename": "data_UWO_2020-01_2021-01.sqlite",
        "source": "bn_dl803_coop_grundstr",
        "parameter": "rainfall_intensity",
        "start": "2020-04-11 08:54:00",
        "end": "2020-06-11 08:56:00",
        "picture_id": "127",
    },
    {
        "filename": "data_UWO_2021-01_2022-01.sqlite",
        "source": "bt_dl929_164_luppmenweg",
        "parameter": "water_temperature",
        "start": "2021-04-11 08:54:00",
        "end": "2021-06-11 08:56:00",
        "picture_id": "123",
    },
]


@contextlib.contextmanager
def open_sqlite(db_file):
    conn = sqlite3.connect(db_file)
    try:
        yield conn
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()


def query_sqlite(db_file: pathlib.PosixPath, sql_query: str):

    with open_sqlite(db_file) as conn:
        return pd.read_sql_query(
            sql_query,
            conn,
        )


def query_datapool(source, parameter, start, end):

    datapool_instance = DataPool(to_replace={"parameter": "variable"})

    data = datapool_instance.signal.get(
        source_name=source, variable_name=parameter, start=start, end=end
    )

    return data


def compare_data(sqlite_path: pathlib.Path, selection: dict) -> None:

    sql_query = f"""
        SELECT
        signal.timestamp AS timestamp,
        value AS value,
        unit AS unit,
        variable.name AS parameter,
        source.name AS source
    FROM signal
        INNER JOIN variable ON signal.variable_id = variable.variable_id
        INNER JOIN source ON signal.source_id = source.source_id
    WHERE source.name = '{selection.get("source")}'
        AND variable.name = '{selection.get("parameter")}'
        AND signal.timestamp >= '{selection.get("start")}'
        AND signal.timestamp <= '{selection.get("end")}';   
    
    """

    data_datapool = query_datapool(
        selection.get("source"),
        selection.get("parameter"),
        selection.get("start"),
        selection.get("end"),
    )
    data_datapool = data_datapool[["timestamp", "value", "unit", "variable", "source"]]
    data_datapool = data_datapool.sort_values(by="variable")

    data_sqlite = query_sqlite(sqlite_path, sql_query
    )
    data_sqlite = data_sqlite.sort_values(by="parameter")

    print("############################################################################")
    print("")
    print("### Consistency of datapool and SQLite")
    print("")
    print("### Check returns True if consistent, False if not consitent.")
    print("### We apply robust checks that ignore dtype and format.")
    print("")
    print(f"### These results are for the selection {selection}.")
    print("")
    print(f"### Same variable name: {all(data_datapool['variable'] == data_sqlite['parameter'])}")
    print(f"### Same unit: {all(data_datapool['unit'] == data_sqlite['unit'])}")
    print(
        f"### Value column no difference: {sum((data_datapool['value']-data_sqlite['value'])**2) == 0.0}"
    )
    print("")
    print("############################################################################")


def export_image(sqlite_path: pathlib.Path, selection: dict) -> None:

    with open_sqlite(sqlite_path) as conn:
        cursor = conn.cursor()
        query = f"SELECT * FROM picture;"
        # query = f"SELECT * FROM picture WHERE picture_id = '{selection.get('picture_id')}';"
        cursor.execute(query)
        records = cursor.fetchall()
        # for row in records:
        #     image = row[5]
        #     with open(".image.jpg", 'wb') as file:
        #         file.write(image)


def main(args: argparse.Namespace) -> None:

    sd = pathlib.Path(args.sourcedirectory)

    selection = random.choice(COMBINATIONS)

    # compare_data(sd / selection.get("filename"), selection)

    export_image(sd / selection.get("filename"), selection)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-sd", "--sourcedirectory", default="/path/to/db_file")

    args = parser.parse_args()

    main(args)
