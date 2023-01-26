# -*- coding: utf-8 -*-


from pathlib import Path


import pandas as pd
from datapool_client import DataPool
import plotly.graph_objs as go
import plotly.subplots as subp


HEATMAP_QUERY = f"""
WITH count_table AS (
    SELECT 
        count(parameter_id), 
        parameter_id, 
        source_id, 
        date_trunc('week', timestamp)
    FROM signal
    GROUP BY 
        date_trunc('week', timestamp), 
        parameter_id, 
        source_id
)
SELECT 
    count_table.count AS value_count, 
    parameter.name AS parameter_name, 
    source.name AS source_name,
    count_table.date_trunc AS date_trunc
FROM count_table
INNER JOIN parameter ON parameter.parameter_id = count_table.parameter_id
INNER JOIN source ON source.source_id = count_table.source_id
order by date_trunc desc;
"""

EXCLUDE_LIST = []


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


def format_time_to_parameter(highest_values):
    df = highest_values.pivot_table(
        values=["value_count"], columns=["parameter_name"], index="date_trunc"
    )
    df.columns = df.columns.droplevel()
    df.index.name = ""
    df.columns.name = ""
    return df


def get_parameter_matrices(data):
    matrices = {}
    for name, index in data.groupby("source_name").groups.items():
        df = data.loc[index]
        matrices[name] = format_time_to_parameter(df)
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


def generate_heatmap(df, target_directory: Path):

    df = df.dropna(how="all")

    fig = subp.make_subplots(
        rows=4,
        cols=1,
        shared_xaxes=True,
        subplot_titles=("Package A1", "Package A2", "Package A3", "Package A4"),
        vertical_spacing=0.05,
    )

    fig.add_trace(
        go.Heatmap(x=df.index, y=df.columns, z=df.values, coloraxis="coloraxis"), 1, 1
    )

    fig.add_trace(
        go.Heatmap(x=df.index, y=df.columns, z=df.values, coloraxis="coloraxis"), 2, 1
    )

    fig.add_trace(
        go.Heatmap(x=df.index, y=df.columns, z=df.values, coloraxis="coloraxis"), 3, 1
    )

    fig.add_trace(
        go.Heatmap(x=df.index, y=df.columns, z=df.values, coloraxis="coloraxis"), 4, 1
    )

    fig.update_layout(coloraxis={"colorscale": "blues"})

    fig.update_layout(
        autosize=False,
        width=1600,
        height=1200,
    )

    fig.update_annotations(font=dict(family="Helvetica", size=20))

    fig.layout.annotations[0].update(x=-0.05)
    fig.layout.annotations[2].update(x=-0.05)
    fig.layout.annotations[1].update(x=-0.05)
    fig.layout.annotations[3].update(x=-0.05)

    fig.write_image(target_directory.parent / "figure" / "figure_1.png")


# groups = group_main_heatmap(heatmap_main)
# for group in groups:
#     generate_heatmaps(
#         heatmap_main[groups[group]]
#     )

# parameters = get_parameter_matrices(data)

# for name in parameters.keys():
#     normalized = normalize_matrix(parameters[name])
#     plot_name = get_plot_name_of_single_source(name)
#     generate_heatmaps(
#         normalized
#     )


def main(target_directory: Path, reload_data: bool) -> None:

    if reload_data:
        dp = DataPool(
            host="eaw-sdwh3.eawag.wroot.emp-eaw.ch",
            port="5432",
            database="datapool",
            user="datapool",
            password="corona666",
            to_replace={"parameter": "variable"},
        )
        data = dp.query_df(HEATMAP_QUERY)
        data.to_csv(target_directory / "heatmap_data.csv", index=False)
    else:
        data = pd.read_csv(target_directory / "heatmap_data.csv")

    data["date_trunc"] = pd.to_datetime(data["date_trunc"], format="%Y-%m-%d")

    data = data.loc[
        (data["date_trunc"] >= "2018-01-01") & (data["date_trunc"] < "2023-01-01")
    ]

    data = exclude_data(data, EXCLUDE_LIST)

    highest = get_highest_counts(data)
    pivot = format_time_to_source(highest)
    heatmap_main = normalize_matrix(pivot)

    generate_heatmap(heatmap_main, target_directory)


if __name__ == "__main__":

    target_directory = Path("/home/adisch/VisualStudioProjects/data/processed")

    main(target_directory, reload_data=False)
