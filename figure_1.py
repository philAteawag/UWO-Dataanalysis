from datapool_client import DataPool
import plotly.offline as py
import plotly.graph_objs as go


def exclude_data(data, exclude_list):
    to_delete = []
    for idx, name in data.source_name.items():
        if name in exclude_list:
            to_delete.append(idx)
    return data.drop(
        index=to_delete
    ).reset_index(
        drop=True
    )


def get_highest_counts(data):
    return data.sort_values(
        ['source_name', 'value_count'],
        ascending=False
    ).drop_duplicates(
        subset=["source_name", "date_trunc"],
        keep="first"
    )


def format_time_to_source(highest_values):
    df = highest_values.pivot_table(
        values=["value_count"],
        columns=["source_name"],
        index="date_trunc"
    )
    df.columns = df.columns.droplevel()
    df.index.name = ""
    df.columns.name = ""
    return df


def format_time_to_parameter(highest_values):
    df = highest_values.pivot_table(
        values=["value_count"],
        columns=["parameter_name"],
        index="date_trunc"
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


def generate_heatmaps(df, path, auto_open=False):
    df = df.dropna(how="all")
    i_plot = df.iplot(
        title="Datapool Content",
        kind="heatmap",
        colorscale='ylgn',
        asFigure=True
    )
    plot_dict = i_plot.to_dict()
    plot_dict["layout"]["margin"] = {
        "l": 250,
        "r": 50
    }
    i_plot = go.Figure(plot_dict)
    py.plot(
        i_plot,
        filename=path,
        auto_open=auto_open
    )


dp = DataPool()


heat_map_query = """
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

save_path = "/home/heinrich/Downloads/test_1/"
save_path = r"C:\Users\foerstchadm\Programs\simpleplotlyserver\templates"

exclude_list = ["bf_plsABL1101_outflow_ara",
                "bt_plsABL1301_outflow_ara",
                "bq_plsABL3701_outflow_ara",
                "bl_plsZUH1201_inflow_ara",
                "bt_plsZUL1311_inflow_ara",
                "bq_plsZUL3201_inflow_ara",
                "bl_plsRFBA1201_sk_ara",
                "bl_plsRKBA1201_rubbasin_ara",
                "bl_plsRKBM1201_rubmorg_inflow",
                "bl_plsRKBM1203_rub_morg",
                "bf_plsRKBU1101_rub128basin_usterstr",
                "bl_plsRKBU1201_rub128basin_usterstr",
                "bl_plsRKPI1201_rubpw80sbw_industry",
                "bn_plsALG1801E_ara_flatroof"]


data = dp.query_df(heat_map_query)

data.to_csv(f"{save_path}/raw_data.csv", sep=";")

data = exclude_data(data, exclude_list)

highest = get_highest_counts(data)
pivot = format_time_to_source(highest)
heatmap_main = normalize_matrix(pivot)

generate_heatmaps(
    heatmap_main,
    f"{save_path}/main.html"
)

groups = group_main_heatmap(heatmap_main)
for group in groups:
    generate_heatmaps(
        heatmap_main[groups[group]],
        f"{save_path}/{group}.html"
    )

parameters = get_parameter_matrices(data)

for name in parameters.keys():
    normalized = normalize_matrix(parameters[name])
    plot_name = get_plot_name_of_single_source(name)
    generate_heatmaps(
        normalized,
        f"{save_path}/{plot_name}.html"
    )
