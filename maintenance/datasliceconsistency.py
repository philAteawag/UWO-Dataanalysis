# coding: utf-8


import logging
import argparse
import pathlib
import sqlite3
import json
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


def query(
    db_files: list[pathlib.PosixPath],
    sql_query: str,
    to_dataframe: bool=True,
):  
    res = []
    for db in db_files:
        with open_sqlite(db) as conn:
            cur = conn.cursor()
            cur.execute(sql_query)
            qry_res = cur.fetchall()
            cur.close()
            if to_dataframe:
                res.append(_to_df(qry_res))
            else:
                res.append([i[0] for i in qry_res])
    return res


def _to_df(lst: list[tuple]) -> pd.DataFrame:
    if not lst:
        res = pd.DataFrame(lst, columns=["time", "value"])        
    elif len(lst[0]) == 2:
        res = pd.DataFrame(lst, columns=["time", "value"])
    else:
        res = pd.DataFrame(lst, columns=["time", "value", "unit", "variable", "source"])
    return res


def remove_negative_values(lst: list[pd.DataFrame]) -> list[pd.DataFrame]:

    for df in lst:
        df.loc[df["value"]<0, "value"] = 0
    return lst


def all_sources_available(sources, dbs):

    qry = f"""
    SELECT DISTINCT name
    FROM source;

    """
    
    all_sources = query(dbs, qry, to_dataframe=False)

    for ist, year in zip(all_sources, [2019, 2020, 2021]):

        difference = list(set(sources).difference(ist))

        if difference:
            logging.info(f"{len(difference)} sources are not in data slice {year}: {difference}.")
        else:
            logging.info(f"All sources from 'dataslices_content_overview.csv' are in data slice {year}.")


def all_variables_available(sources, overview, dbs):

    def __get_qry(source_name, variable_name):
    
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
    
    for source in sources:
        for variable in overview.get(source):
            qry = __get_qry(source, variable)
            result = query(dbs, qry)
            for res, year in zip(result, [2019, 2020, 2021]):
                if res.empty:
                    logging.info(f"No entry for source {source} and variable {variable} in data slice {year}.")
                else:
                    logging.info(f"{len(res['value'])} data points for source {source} and variable {variable} in data slice {year}.")


def check_rain_sums(sources, dbs):

    def __get_qry(source_name, variable_name="rainfall_intensity"):
    
        return f"""
        SELECT
            signal.timestamp,
            value
        FROM signal
            INNER JOIN variable ON signal.variable_id = variable.variable_id
            INNER JOIN source ON signal.source_id = source.source_id
        WHERE source.name = '{source_name}' AND variable.name = '{variable_name}'
        ORDER BY signal.timestamp ASC

        """
    
    for source in sources:
        qry = __get_qry(source)
        raw = query(dbs, qry)
        raw = remove_negative_values(raw)
        result = [sum(df["value"]) for df in raw]
        for res, year in zip(result, [2019, 2020, 2021]):
            if res / 60 < 1000 or res / 60 > 2000:
                logging.info(f"Rain sum for source {source} conspicuous with {round(res / 60, 2)} mm in data slice {year}.")
            else:
                logging.info(f"Rain sum for source {source} in data slice {year} ok with {round(res / 60, 2)} mm.")


def check_flow_volumes(sources, reference, dbs):

    def __get_qry(source_name, variable_name="flow_rate"):
    
        return f"""
        SELECT
            signal.timestamp,
            value
        FROM signal
            INNER JOIN variable ON signal.variable_id = variable.variable_id
            INNER JOIN source ON signal.source_id = source.source_id
        WHERE source.name = '{source_name}' AND variable.name = '{variable_name}'
        ORDER BY signal.timestamp ASC

        """
    
    qry = __get_qry(reference)
    raw = query(dbs, qry)
    raw = remove_negative_values(raw)
    reference = [sum(df["value"]) for df in raw]
    
    for source in sources:
        qry = __get_qry(source)
        res_raw = query(dbs, qry)
        res_raw = remove_negative_values(res_raw)
        result = [sum(df["value"]) for df in res_raw]
        for res, year, ref in zip(result, [2019, 2020, 2021], reference):
            if res > ref:
                logging.info(f"Flow volume for source {source} too high with {round(res / 1000, 2)} m3 in data slice {year}.")
            else:
                logging.info(f"Flow volume for source {source} in data slice {year} ok with {round(res / 1000, 2)} m3.")


def main(args: argparse.Namespace) -> None:
    path_to_db = pathlib.Path(args.sourcedirectory)
    dbs = []
    for filename in ["data_UWO_2019-01_2020-01.sqlite", "data_UWO_2020-01_2021-01.sqlite", "data_UWO_2021-01_2022-01.sqlite"]:
        dbs.append(path_to_db / filename)

    path_to_overview = pathlib.Path(args.secondsourcedirectory)
    json_file = path_to_overview / "datapool_sources_variables.json"
    source_variable_overview = json.load(json_file.open())

    content_list = "dataslices_content_overview.csv"
    package_content = pd.read_csv(path_to_db / content_list, sep=";")

    output_path = pathlib.Path(args.targetdirectory)
    log_filename = "dataslice_consistency.log"
    logging.basicConfig(filename=output_path / log_filename,
                    filemode="w",
                    format="%(message)s",
                    level=logging.DEBUG)

    # logging.info("Are all source from the file 'dataslices_content_overview.csv' existing in the data slices?")
    # all_sources_available(package_content["source"].tolist(), dbs)

    # logging.info("Are all variables from the datapool export in the data slices?")
    # all_variables_available(package_content["source"].tolist(), source_variable_overview, dbs)

    logging.info("Do the measured rainfall heights make sense?")
    check_rain_sums([
        "bn_dl259_rub_morg",
        "bn_dl797_rub_morg",
        "bn_dl798_electrosuisse_luppmenstr",
        "bn_dl799_ara_flatroof",
        "bn_dl800_pumpwau_rumlikerstr",
        "bn_dl801_pumpwgeeren_geerenstr",
        "bn_dl802_gerber_zurcherstr",
        "bn_dl803_coop_grundstr",
        "bn_r02_school_chatzenrainstr",
        "bn_r03_rub_morg",
        "bn_r04_airport_speck",
        "bn_r05_schutzenhaus_burgweg"
        ], dbs)
    
    # logging.info("Do the measured flow volumina make sense?")
    # check_flow_volumes([
    #     "bf_f02_555_mesikerstr",
    #     "bf_f03_11e_russikerstr",
    #     "bf_f07_23_bahnhofstr",
    #     "bf_f08_166_luppmenweg",
    #     "bf_f10_22a_bahnhofstr",
    #     "bf_f12_47a_zurcherstr",
    # ],
    # "bf_plsZUL1100_inflow_ara", dbs)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-sd", "--sourcedirectory", default="/path/to/db_file_directory")
    parser.add_argument("-ss", "--secondsourcedirectory", default="/path/to/processed_data_directory")
    parser.add_argument("-td", "--targetdirectory", default="/path/to/output")

    args = parser.parse_args()

    main(args)
