# -*- coding: utf-8 -*-


import pathlib
import contextlib
import sqlite3


import pandas as pd
import plotly.graph_objs as go
import plotly.subplots as subp


HEATMAP_QUERY = f"""
WITH count_table AS (
SELECT
count(variable_id) AS count,
variable_id,
source_id,
strftime('%Y-%m-%d', timestamp, 'weekday 0', '-6 days') AS date_trunc
FROM signal
GROUP BY
strftime('%Y-%m-%d', timestamp, 'weekday 0', '-6 days'),
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


def exclude_data(data, exclude_list):
    to_delete = []
    for idx, name in data.source_name.items():
        if name in exclude_list:
            to_delete.append(idx)
    return data.drop(index=to_delete).reset_index(drop=True)


def get_highest_counts(data):
    return data.sort_values(
        ["source_name", "value_count"], ascending=False
    ).drop_duplicates(subset=["source_name", "date_trunc"], keep="first")


def format_time_to_source(highest_values):
    df = highest_values.pivot_table(
        values=["value_count"], columns=["source_name"], index="date_trunc"
    )
    df.columns = df.columns.droplevel()
    df.index.name = ""
    df.columns.name = ""
    return df


def format_time_to_variable(highest_values):
    df = highest_values.pivot_table(
        values=["value_count"], columns=["variable_name"], index="date_trunc"
    )
    df.columns = df.columns.droplevel()
    df.index.name = ""
    df.columns.name = ""
    return df


def get_variable_matrices(data):
    matrices = {}
    for name, index in data.groupby("source_name").groups.items():
        df = data.loc[index]
        matrices[name] = format_time_to_variable(df)
    return matrices


def get_plot_name_of_single_source(name):
    return "_".join(name.split("_")[:2])


def normalize_matrix(data):
    return data.apply(
        lambda x: x / x.max(),
        axis=0,
    )


def group_main_heatmap(heatmap_main):
    _groups = set([i.split("_")[0] for i in heatmap_main.columns])

    groups = {}
    for group in _groups:
        groups[group] = []
        for source in heatmap_main.columns:
            if source.startswith(group):
                groups[group].append(source)

    return groups


def generate_heatmap(df_a1, df_a2, df_a3, df_a4, target_directory: pathlib.Path):
    tot_col = (
        len(df_a1.columns)
        + len(df_a2.columns)
        + len(df_a3.columns)
        + len(df_a4.columns)
    )
    row_widths = [
        len(df_a1.columns) / tot_col,
        len(df_a2.columns) / tot_col,
        len(df_a3.columns) / tot_col,
        len(df_a4.columns) / tot_col,
    ]
    row_widths.reverse()

    fig = subp.make_subplots(
        rows=4,
        cols=1,
        shared_xaxes=True,
        subplot_titles=("Package A1", "Package A2", "Package A3", "Package A4"),
        vertical_spacing=0.05,
        row_width=row_widths,
    )

    fig.add_trace(
        go.Heatmap(
            x=df_a1.index,
            y=df_a1.columns,
            z=df_a1.values.transpose(),
            coloraxis="coloraxis",
        ),
        1,
        1,
    )

    fig.add_trace(
        go.Heatmap(
            x=df_a2.index,
            y=df_a2.columns,
            z=df_a2.values.transpose(),
            coloraxis="coloraxis",
        ),
        2,
        1,
    )

    fig.add_trace(
        go.Heatmap(
            x=df_a3.index,
            y=df_a3.columns,
            z=df_a3.values.transpose(),
            coloraxis="coloraxis",
        ),
        3,
        1,
    )

    fig.add_trace(
        go.Heatmap(
            x=df_a4.index,
            y=df_a4.columns,
            z=df_a4.values.transpose(),
            coloraxis="coloraxis",
        ),
        4,
        1,
    )

    fig.update_layout(coloraxis={"colorscale": "blues"})

    fig.update_layout(
        autosize=False,
        width=1000,
        height=1500,
    )

    fig.update_annotations(font=dict(family="Helvetica", size=20))

    fig.layout.annotations[0].update(x=-0.05)
    fig.layout.annotations[2].update(x=-0.05)
    fig.layout.annotations[1].update(x=-0.05)
    fig.layout.annotations[3].update(x=-0.05)

    fig.update_coloraxes(showscale=False)

    fig.write_image(target_directory.parent / "figure" / "figure_1.png")

    fig.write_html(target_directory.parent / "figure" / "figure_1.html")


# groups = group_main_heatmap(heatmap_main)
# for group in groups:
#     generate_heatmaps(
#         heatmap_main[groups[group]]
#     )

# variables = get_variable_matrices(data)

# for name in variables.keys():
#     normalized = normalize_matrix(variables[name])
#     plot_name = get_plot_name_of_single_source(name)
#     generate_heatmaps(
#         normalized
#     )


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


def _query_df(conn, sql_query):
    return pd.read_sql_query(
        sql_query,
        conn,
    )


def query(db_file: pathlib.PosixPath, sql_query: str):
    with open_sqlite(db_file) as conn:
        return _query_df(conn, sql_query)


def main(
    source_directory: pathlib.Path,
    filename: str,
    clfile: pathlib.Path,
    target_directory: pathlib.Path,
    reload_data: bool,
) -> None:
    if reload_data:
        db = source_directory / filename
        data = query(db, HEATMAP_QUERY)
        data.to_csv(target_directory / "heatmap_data_1.csv", index=False)
    else:
        data = pd.read_csv(target_directory / "heatmap_data_3.csv")
        data = data.append(pd.read_csv(target_directory / "heatmap_data_2.csv"))
        data = data.append(pd.read_csv(target_directory / "heatmap_data_1.csv"))

    data["date_trunc"] = pd.to_datetime(data["date_trunc"], format="%Y-%m-%d")

    highest = get_highest_counts(data)
    pivot = format_time_to_source(highest)
    heatmap_main = normalize_matrix(pivot)

    heatmap_main = heatmap_main.fillna(0)

    content = pd.read_csv(clfile, sep=";")

    filter_a1 = content[content["A1"] == 1]
    source_names_a1 = filter_a1["source"].to_list()
    df_a1 = heatmap_main[source_names_a1]
    filter_a2 = content[content["A2"] == 1]
    source_names_a2 = filter_a2["source"].to_list()
    source_names_a2_updated = [
        x
        for x in source_names_a2
        if x
        not in [
            "bl_dl318_597sbw_ara",
            "bf_f04_23_bahnhofstr",
            "bf_f06_11e_russikerstr",
            "bf_f09_11e_russikerstr",
        ]
    ]
    df_a2 = heatmap_main[source_names_a2_updated]
    filter_a3 = content[content["A3"] == 1]
    source_names_a3 = filter_a3["source"].to_list()
    source_names_a3_updated = [
        x for x in source_names_a3 if x not in ["bt_dl912_rw137_schutzengasse"]
    ]
    df_a3 = heatmap_main[source_names_a3_updated]
    filter_a4 = content[content["A4"] == 1]
    source_names_a4 = filter_a4["source"].to_list()
    source_names_a4_updated = [
        x
        for x in source_names_a4
        if x not in ["bl_dl318_597sbw_ara", "bt_dl912_rw137_schutzengasse"]
    ]
    df_a4 = heatmap_main[source_names_a4_updated]

    generate_heatmap(df_a1, df_a2, df_a3, df_a4, target_directory)


if __name__ == "__main__":
    source_directory = pathlib.Path(
        "Q:/Abteilungsprojekte/eng/SWWData/2015_fehraltorf/uwo_data_slices"
    )
    target_directory = pathlib.Path(
        "C:/Users/dischand/VisualStudioProjects/data/processed"
    )

    filename = "data_UWO_2019-01_2020-01.sqlite"

    package_directory = pathlib.Path(
        "C:/Users/dischand/switchdrive/UWO/Arbeiten und Artikel/UWO_Data_paper/_dataslice/_upload"
    )
    cl_file = "package_information.csv"
    clfile = package_directory / cl_file

    main(source_directory, filename, clfile, target_directory, reload_data=False)
