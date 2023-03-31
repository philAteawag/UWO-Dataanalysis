# -*- coding: utf-8 -*-


import pathlib
import contextlib
import sqlite3


import pandas as pd


SOURCES = [
    "bl_ceaf0_rub_morg",
    "bl_ceb5f_58sbw_undermulistr",
    "bl_ceb60_138a_venturi",
    "bl_ce769_rubpw80sbw_industry",
    "bl_dl256_rubbasin_ara",
    "bl_dl257_inflow_ara",
    "bl_dl258_rubbasin_ara",
    "bm_dl290_rubbasin_ara",
    "bm_dl291_rubbasin_ara",
    "bl_dl309_sk102_wermatswilstr",
    "bl_dl310_sk_ara",
    "bl_dl311_rubmorg_inflow",
    "bl_dl312_40d_imberg",
    "bl_dl313_124a_chueferi",
    "bl_dl314_50sbw_acherm",
    "bl_dl316_40g_csovoland",
    "bl_dl317_115a_chueferistr",
    "bl_dl318_ra40a_sbwvoland",
    "bl_dl318_597sbw_ara",
    "bl_dl319_wildbach_rumlikonstr",
    "bl_dl320_597sbw_ara",
    "bl_dl321_48sbw_notuberlauf",
    "bl_dl322_rub128inflow_usterstr",
    "bl_dl323_rub128basin_usterstr",
    "bl_dl324_137_schutzengasse",
    "bl_dl325_585sbw_amwildbach",
    "bl_dl9449_15a_russikerstr",
    "bp_dl326_wildbach_kempttalstr",
    "bp_dl327_luppmen_usterstr",
    "bm_dl328_sk_ara",
    "bm_dl329_rub128basin_usterstr",
    "bm_dl330_rub128inflow_usterstr",
    "bm_dl331_58sbw_undermulistr",
    "bm_dl332_rub_morg",
    "bm_dl333_vs22_kempttalstr",
    "bm_dl892_58sbw_undermulistr",
    "bl_dl893_585sbw_amwildbach",
    "bl_dl899_inflow_ara",
    "bl_dl900_156a_geerenstr",
    "bl_ce463_vs22_kempttalstr",
    "bl_ce0bd_15a_russikerstr",
    "bl_ce5ce_450b_usterstr",
    "bf_f02_555_mesikerstr",
    "bf_f03_11e_russikerstr",
    "bf_f04_23_bahnhofstr",
    "bf_f06_11e_russikerstr",
    "bf_f07_23_bahnhofstr",
    "bf_f08_166_luppmenweg",
    "bf_f09_11e_russikerstr",
    "bf_f10_22a_bahnhofstr",
    "bf_f12_47a_zurcherstr",
    "bl_lm064_rubpw80sbwbasin_industry",
    "bl_lm065_7_kempttalstr",
    "bm_lm067_ra40a_sbwvoland",
    "bl_lm069_ra40a_sbwvoland",
    "bl_lm073_607sbw_kempttalerstr",
    "bl_lm076_rohrbach_fehraltorferstr",
    "bm_lm079_rubpw80sbwbasin_industry",
    "bm_lm085_rubpw80sbwbasin_industry",
    "bl_plsRKPI1201_rubpw80sbw_industry",
    "bl_plsRKBU1201_rub128basin_usterstr",
    "bl_plsRKBM1203_rub_morg",
    "bf_plsRKBM1101_3r_rub_morg_overflow",
    "bl_plsRKBA1201_rubbasin_ara",
    "bl_plsRFBA1201_sk_ara",
    "bf_plsRKBU1101_rub128basin_usterstr",
    "bf_plsZUL1100_inflow_ara",
    "bf_plsRKPI1102_rubpw80sbw_overflow",
    "bf_plsRKBU1102_rub128basin_overflow",
    "bf_plsRKBA1101_rubbasin_ara_overflow",
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


def query(db_file: pathlib.PosixPath, sql_query: str):
    with open_sqlite(db_file) as conn:
        return pd.read_sql_query(
            sql_query,
            conn,
        )


def main(source_directory, filename, target_directory):
    db = source_directory / filename

    sql_query = f"""
    SELECT source.name, source.description, source_type.name, source_type.description
    FROM source
    INNER JOIN source_type ON source_type.source_type_id = source.source_type_id 
    """
    res = query(db, sql_query)

    res.to_csv(target_directory / "source_types.csv")


if __name__ == "__main__":
    source_directory = pathlib.Path(
        "Q:/Abteilungsprojekte/eng/SWWData/2015_fehraltorf/3_data/__uwo_data_slices"
    )
    target_directory = pathlib.Path(
        "C:/Users/dischand/VisualStudioProjects/data/processed"
    )

    filename = "data_UWO_2019-01_2020-01.sqlite"

    main(source_directory, filename, target_directory)
