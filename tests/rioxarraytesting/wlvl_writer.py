# %%
import os
import time
from pathlib import Path

import geopandas as gpd
import hhnk_research_tools as hrt
import numpy as np
import pandas as pd
import rasterio as rio
import rioxarray as rxr
from scipy.interpolate import LinearNDInterpolator
from scipy.spatial import Delaunay
from threedidepth import morton

SETUP = "clip_medium"
PLAYGROUND_DIR = Path(os.environ["3DI_PLAYGROUND_DIR"])
grid_gdf = gpd.read_file(PLAYGROUND_DIR / SETUP / "grid_corr.gpkg")
dem_path = PLAYGROUND_DIR / SETUP / "dem.tif"
dem_raster = hrt.Raster(dem_path)
NO_DATA_VALUE = -9999.0
wlvl_column = "wlvl_max_replaced"

# TODO docstrings zijn erg summier. Bij functies iets meer achtergrond.


def get_boundary_points(grid_gdf, boundary, wlvl_column, no_data_value):
    # select only polygons with data
    gdf = grid_gdf[grid_gdf[wlvl_column] != no_data_value]

    # select only polygons at, but inside boundary
    gdf = gdf[gdf.intersects(boundary)]

    # get grid corner and edge-center points as fast as we can (we think :-))
    bounds_gdf = gdf.bounds  # yields minx, miny, maxx and maxy
    bounds_gdf.loc[:, "x"] = gdf.centroid.x  # centre x
    bounds_gdf.loc[:, "y"] = gdf.centroid.y  # centre y

    # get 4 corners and 4 edge-centers x and y, preserving index
    x = pd.concat(
        [
            bounds_gdf.minx,
            bounds_gdf.minx,
            bounds_gdf.minx,
            bounds_gdf.x,
            bounds_gdf.maxx,
            bounds_gdf.maxx,
            bounds_gdf.maxx,
            bounds_gdf.x,
        ]
    )

    y = pd.concat(
        [
            bounds_gdf.miny,
            bounds_gdf.y,
            bounds_gdf.maxy,
            bounds_gdf.maxy,
            bounds_gdf.maxy,
            bounds_gdf.y,
            bounds_gdf.miny,
            bounds_gdf.miny,
        ]
    )

    # create a point GeoDataFrame
    pnt_gdf = gpd.GeoDataFrame(geometry=gpd.points_from_xy(x, y), index=x.index, crs=grid_gdf.crs)

    # as we preserved index, we can add original waterlevel
    pnt_gdf.loc[:, wlvl_column] = gdf[wlvl_column]

    # drop rows with equal geometry + waterlevel
    pnt_gdf.drop_duplicates(inplace=True)

    # select only points that are on the boundary
    pnt_gdf = pnt_gdf[pnt_gdf.buffer(0.1).intersects(boundary)]

    # take the average water-level of all points at the same location
    gdf = gpd.GeoDataFrame(pnt_gdf.groupby("geometry").mean().reset_index(), crs=grid_gdf.crs)

    return gdf


def get_points_grid(grid_gdf, poly, wlvl_column, no_data_value):
    # select the the polygons adjacent to polygons with data
    gdf = grid_gdf[grid_gdf[wlvl_column] == no_data_value]
    gdf = gdf[gdf.intersects(poly.boundary)]

    # concat with polygons with data
    gdf = pd.concat([gdf, grid_gdf[grid_gdf[wlvl_column] != no_data_value]])

    # take the centroid and change nodata to NaN
    gdf.loc[:, "geometry"] = gdf.centroid
    gdf.loc[gdf[wlvl_column] == no_data_value, wlvl_column] = pd.NA

    return gdf


def get_delaunay_grid(grid_gdf, wlvl_column, no_data_value):
    # we maken een buffer rond de gridcellen met data; dit begrenst de interpolatie
    poly = grid_gdf[grid_gdf[wlvl_column] != no_data_value].unary_union
    boundary = poly.boundary

    gdf = pd.concat(
        [
            get_points_grid(grid_gdf, poly, wlvl_column, no_data_value),
            get_boundary_points(grid_gdf, boundary, wlvl_column, no_data_value),
        ]
    )

    return gdf


def get_interpolator(grid_gdf, wlvl_column, no_data_value, reorder=True):
    """Deze functie maakt een LinearNDInterpolator op basis van Delauny triangulatie"""

    gdf = get_delaunay_grid(grid_gdf, wlvl_column, no_data_value)
    points_grid = gdf.get_coordinates().to_numpy()
    wlvl = gdf[wlvl_column].to_numpy()
    if reorder:
        # taken from the original interpolation.
        points_grid, wlvl = morton.reorder(points_grid, wlvl)

    return LinearNDInterpolator(Delaunay(points_grid), wlvl)


now = time.time()

interpolator = get_interpolator(grid_gdf, wlvl_column, NO_DATA_VALUE)

with rxr.rioxarray.open_rasterio(dem_raster.path) as src:
    shape = src.shape[1:]
    x, y = np.meshgrid(
        src.x.data,
        src.y.data,
    )

with rio.open(dem_raster.path) as src:
    profile = src.profile
    profile.pop("dtype")

level = interpolator(x, y)
level = np.nan_to_num(level, nan=NO_DATA_VALUE)

mask = grid_gdf[wlvl_column] != NO_DATA_VALUE
raster_mask = rio.features.rasterize(
    shapes=grid_gdf[mask].geometry,
    out_shape=shape,
    transform=profile["transform"],
    default_value=True,
    fill=False,
).astype(bool)
level[~raster_mask] = NO_DATA_VALUE

print(time.time() - now)


with rio.open(PLAYGROUND_DIR / SETUP / "wlvl_test.tif", "w", **profile, dtype="float32") as dst:
    dst.write(level, 1)

with rio.open(PLAYGROUND_DIR / SETUP / "mask.tif", "w", **profile, dtype="int32") as dst:
    dst.write(raster_mask, 1)
