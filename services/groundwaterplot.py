# -*- coding: utf-8 -*-


"""
This code reads in data from various files and sources, prepares the data for plotting,
and then generates a choropleth map of the groundwater level for a specific date.

"""

import pathlib
import pickle

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import rasterio
import scipy
from rasterstats import zonal_stats


def read_catchment_data(source_directory: pathlib.Path) -> gpd.GeoDataFrame:
    """
    Reads in catchment area data.

    Args:
        source_directory: pathlib.Path object for the directory containing the data.
    
    Returns:
        catchment areas
    
    """
    faf_catchment = gpd.read_file(source_directory / "subcat_info.shp")
    faf_catchment = faf_catchment.to_crs("EPSG:21781")

    return faf_catchment


def read_wells_data(source_directory: pathlib.Path) -> tuple[pd.DataFrame, gpd.GeoDataFrame]:
    """
    Reads in wells data.

    Args:
        source_directory: pathlib.Path object for the directory containing the data.

    Returns:
        daily well data and well locations

    """
    dict_data_smoothed = pickle.load(
        open(source_directory / "dict_data_smoothed.p", "rb")
    )
    wells = pd.DataFrame(dict_data_smoothed)
    wells["times"] = pd.to_datetime(wells["times"])
    wells.set_index("times", inplace=True)
    wells[wells.columns] = wells[wells.columns].astype("float")
    wells_day = wells.resample("1D").mean()

    wells_coordinates = gpd.read_file(source_directory / "wells_coordinates.shp")
    wells_coordinates["array"] = wells_coordinates.apply(
        lambda x: (x.geometry.x, x.geometry.y), axis=1
    )

    return wells_day, wells_coordinates


def define_base_grid(faf_catchment: gpd.GeoDataFrame) -> tuple[np.meshgrid, np.meshgrid]:
    """
    Defines a base grid for a given catchment area using its bounding box coordinates.

    Args:
        faf_catchment (gpd.GeoDataFrame): A GeoDataFrame containing the catchment area polygon.

    Returns:
        the latitude and longitude coordinates of the base grid points

    """

    latmin, lonmin, latmax, lonmax = faf_catchment.total_bounds
    xx, yy = np.meshgrid(
        np.arange(latmin - 100, latmax + 100, 0.5),
        np.arange(lonmin - 100, lonmax + 100, 0.5),
    )
    
    return xx, yy


def interpolate_grid(
    date: str,
    wells_day: pd.DataFrame,
    wells_coordinates: gpd.GeoDataFrame,
    xx: np.meshgrid,
    yy: np.meshgrid,
) -> np.ndarray:
    """
    Calculates groundwater levels for each catchment area on the given date using well data.

    Args:
        date: A string representing the date in "YYYY-MM-DD" format.
        wells_day: A pandas DataFrame containing daily well data.
        wells_coordinates: A GeoDataFrame containing well locations.
        xx: A numpy meshgrid of x-coordinates.
        yy: A numpy meshgrid of y-coordinates.

    Returns:
        The interpolated groundwater levels for each point in the base grid.

    """

    values = list(wells_day[wells_day.index == date].T[date])
    grid_z2 = scipy.interpolate.griddata(
        list(wells_coordinates.array), values, (xx, yy), method="nearest"
    )

    return grid_z2


def calc_initial_groundwatertable(faf_catchment: gpd.GeoDataFrame, grid_z2: np.ndarray, affine: rasterio.Affine):
    """
    Calculates the initial groundwater table elevation for each polygon in the given FAF catchment.

    Args:
        faf_catchment: A GeoDataFrame representing the FAF catchment polygons.
        grid_z2: A two-dimensional numpy array representing the elevation grid.
        affine: An affine transformation object defining the spatial relationship between the grid and the FAF catchment.

    Returns:
        The initial groundwater table elevation for each polygon in the FAF catchment, in the same order as the input GeoDataFrame.

    """
    
    zs_faf = zonal_stats(
        faf_catchment["geometry"], grid_z2, affine=affine, stats="mean"
    )

    start_gwtable = [x["mean"] for x in zs_faf]

    return start_gwtable


def add_modified_gwtable(
    faf_catchment: gpd.GeoDataFrame,
    groundwater: pd.DataFrame,
    start_gwtable: list[float]
) -> gpd.GeoDataFrame:
    """
    Calculates groundwater levels for each catchment area on the given date.

    Args:
        faf_catchment: geopandas GeoDataFrame containing catchment area polygons.
        groundwater: pandas DataFrame containing groundwater level data.
        start_gwtable: the initial groundwater table elevation.

    Returns:
        Updated version of faf_catchment with the calculated groundwater level for each
        catchment area.

    """

    groundwater["Egw"] = start_gwtable
    groundwater["Egw"] = groundwater["Egw"].round(2)

    for index, row in groundwater.iterrows():
        if row["ESurf"] < row["Egw"]:
            groundwater.loc[index, "Egw"] = row["ESurf"] - 0.01

    groundwater["Egwt"] = "*"
    groundwater.loc[206, "ESurf"] = 533

    faf_catchment["Egw"] = groundwater["Egw"]

    return faf_catchment


def plot_data(faf_catchment: gpd.GeoDataFrame) -> None:
    """
    The plot_data function takes the updated faf_catchment dataframe and generates a
    choropleth map of the groundwater level for the given date using the plot method
    of the geopandas package.

    Args:
        faf_catchment: geopandas GeoDataFrame containing catchment area polygons.    

    """

    faf_catchment.plot(column="Egw", cmap="viridis", legend=True)
    plt.show()


def main(date: str, source_directory: pathlib.Path) -> None:

    faf_catchment = read_catchment_data(source_directory)

    wells_day, wells_coordinates = read_wells_data(source_directory)

    xx, yy = define_base_grid(faf_catchment)

    groundwater = pd.read_csv(source_directory / "GW_start.csv", index_col="index")

    grid_z2 = interpolate_grid(date, wells_day, wells_coordinates, xx, yy)

    affine = rasterio.Affine(0.5, 0.0, 697769.1029279142, 0.0, -0.5, 250377.20599496504)
    initial_gwtable = calc_initial_groundwatertable(faf_catchment, grid_z2, affine)

    faf_catchment = add_modified_gwtable(faf_catchment, groundwater, initial_gwtable)

    plot_data(faf_catchment)


if __name__ == "__main__":

    date = "2019-04-01"
    source_directory = pathlib.Path("C:/Users/dischand/VisualStudioProjects/data/raw")

    main(date, source_directory)
