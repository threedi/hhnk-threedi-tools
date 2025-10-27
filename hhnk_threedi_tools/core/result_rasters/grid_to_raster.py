# %%
from pathlib import Path
from typing import Union

import geopandas as gpd
import hhnk_research_tools as hrt
import numpy as np
import pandas as pd
import xarray as xr
from scipy.interpolate import LinearNDInterpolator
from scipy.spatial import Delaunay
from shapely.geometry import LineString, Polygon
from threedidepth import morton
from threedigrid.admin.constants import NO_DATA_VALUE

from hhnk_threedi_tools.core.folders import Folders


class BaseCalculatorGPKG:
    """TODO Deprecated, remove in later release."""

    def __init__(self, **kwargs):
        raise DeprecationWarning(
            "The BaseCalculatorGPKG class has been named to \
htt.GridToRaster since v2024.2. Please rewrite your code."
        )


class GridToRaster:
    """TODO Deprecated, remove in later release."""

    def __init__(self, **kwargs):
        raise DeprecationWarning(
            "The GridToRaster class has been named to \
htt.GridToWaterLevel and htt.GridToWaterDepth since v2024.x. Please rewrite your code."
        )


def get_boundary_points(
    grid_gdf: gpd.GeoDataFrame, boundary: LineString, wlvl_column: str, no_data_value: float
) -> gpd.GeoDataFrame:
    """Een extra rij met gridpunten op het middenpunt van de edges aan de rand van het 3Di grid

    Parameters
    ----------
    grid_gdf : gpd.GeoDataFrame
        3Di grid in GeoDataFrame
    boundary : LineString
        Rand van het 3Di data-domein
    wlvl_column : str
        kolom in GeoDataFrame met waterstanden
    no_data_value : float
        no_data_value in 3Di grid

    Returns
    -------
    gpd.GeoDataFrame
        GeoDataFrame met extra grid-punten aan rand van 3Di grid met data
    """
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


def get_points_grid(
    grid_gdf: gpd.GeoDataFrame, poly: Polygon, wlvl_column: str, no_data_value: float
) -> gpd.GeoDataFrame:
    """Selecteren van punten binnen het 3Di grid met data en converteren naar punten

    Parameters
    ----------
    grid_gdf : gpd.GeoDataFrame
        3Di grid in GeoDataFrame
    poly : Polygon
        Polygoon waarmee 3Di gridcellen worden gefilterd
    wlvl_column : str
        kolom in GeoDataFrame met waterstanden
    no_data_value : float
        no_data_value in 3Di grid

    Returns
    -------
    gpd.GeoDataFrame
        GeoDataFrame met grid-punten binnen en aan rand van 3Di grid met data
    """
    # select the the polygons adjacent to polygons with data
    gdf = grid_gdf[grid_gdf[wlvl_column] == no_data_value]
    gdf = gdf[gdf.intersects(poly.boundary)]

    # concat with polygons with data
    gdf = pd.concat([gdf, grid_gdf[grid_gdf[wlvl_column] != no_data_value]])

    # take the centroid and change nodata to NaN
    gdf.loc[:, "geometry"] = gdf.centroid
    gdf.loc[gdf[wlvl_column] == no_data_value, wlvl_column] = pd.NA

    return gdf


def get_delaunay_grid(grid_gdf: gpd.GeoDataFrame, wlvl_column: str, no_data_value: float) -> gpd.GeoDataFrame:
    """Uitlezen van alle punten uit het 3DI grid die handig zijn voor interpolatie

    Parameters
    ----------
    grid_gdf : gpd.GeoDataFrame
        3Di grid in GeoDataFrame
    wlvl_column : str
        kolom in GeoDataFrame met waterstanden
    no_data_value : float
        no_data_value in 3Di grid

    Returns
    -------
    gpd.GeoDataFrame
        GeoDataFrame met alle grid-punten waarmee delaunay triangulatie mee gemaakt wordt
    """
    # we maken een buffer rond de gridcellen met data; dit begrenst de interpolatie
    poly = grid_gdf[grid_gdf[wlvl_column] != no_data_value].union_all()
    boundary = poly.boundary

    # we voegen alle relevante punten samen tot 1 grid
    gdf = pd.concat(
        [
            get_points_grid(grid_gdf, poly, wlvl_column, no_data_value),
            get_boundary_points(grid_gdf, boundary, wlvl_column, no_data_value),
        ]
    )

    return gdf[[wlvl_column, "geometry"]]


def get_interpolator_input(
    grid_gdf: gpd.GeoDataFrame, wlvl_column: str, no_data_value: float, reorder=True
) -> Delaunay:
    """Get the interpolation input as an input to prepare for Delauney interpolation.

    https://docs.scipy.org/doc/scipy/reference/generated/scipy.interpolate.LinearNDInterpolator.html

    Parameters
    ----------
    grid_gdf : gpd.GeoDataFrame
        3Di grid in GeoDataFrame
    wlvl_column : str
        kolom in GeoDataFrame met waterstanden
    no_data_value : float
        no_data_value in 3Di grid
    reorder : bool, optional
        optie om threedidepth.morton.reorder uit te voeren, staat default op True

    Returns
    -------
    Delaunay
        Interpolation input voor Delaunay interpolatie
    """

    # aanmaken delaunay grid
    gdf = get_delaunay_grid(grid_gdf, wlvl_column, no_data_value)

    # converteren naar numpy-array met punt_grid
    points_grid = gdf.get_coordinates().to_numpy()

    # inlezen waterstanden als numpy-array
    wlvl = gdf[wlvl_column].to_numpy()

    # herordenen volgens threedidepth (geen idee wat het doet)
    if reorder:
        points_grid, wlvl = morton.reorder(points_grid, wlvl)

    return Delaunay(points_grid), wlvl


"""Calculate interpolated rasters from a grid. The grid_gdf is
created using the class ThreediGrid. Which converts the NetCDF
into a gpkg.

It is possible to calculate the wlvl and wdepth.
"""


class GridToWaterLevel:
    def __init__(self, dem_path: Path, grid_gdf: gpd.GeoDataFrame, wlvl_column: str):
        """Bereken waterstanden geinterpoleerd naar de resolutie van de DEM, gebruikmakend van Delaunay-interpolatie

        Parameters
        ----------
        dem_path : Path
            Path naar de DEM
        grid_gdf : gpd.GeoDataFrame
            3Di grid in GeoDataFrame
        wlvl_column : str
            kolom in GeoDataFrame met waterstanden
        """
        self.dem_raster = hrt.Raster(dem_path)
        self.grid_gdf = grid_gdf
        self.wlvl_column = wlvl_column
        self.wlvl_raster = None
        self.points_grid = None
        self.wlvl = None
        self._delaunay_grid_gdf = None

    @property
    def delaunay_grid_gdf(self):
        """Grid voor Delaunay interpolator als GeoDataFrame"""
        if self._delaunay_grid_gdf is None:
            self._delaunay_grid_gdf = get_delaunay_grid(
                grid_gdf=self.grid_gdf, wlvl_column=self.wlvl_column, no_data_value=self.wlvl_column
            )
        return self._delaunay_grid_gdf

    def prepare_interpolator_input(self):
        """2D Delaunay interpolator input. We cannot create the interpolator here because
        it doesnt work well with dasks multiprocessing. For each compute block we initialize a
        new interpolator.
        """
        if self.points_grid is None:
            self.points_grid, self.wlvl = get_interpolator_input(
                grid_gdf=self.grid_gdf, wlvl_column=self.wlvl_column, no_data_value=NO_DATA_VALUE
            )

    def run(self, output_file, chunksize: Union[int, None] = None, overwrite: bool = False):
        # level block_calculator
        def calc_level(_, dem_chunk: xr.DataArray):
            # get x and y coordinates from dem_da
            x, y = np.meshgrid(
                dem_chunk.x.data,
                dem_chunk.y.data,
            )

            # interpolate levels to x and y coordinates
            interpolator = LinearNDInterpolator(points=self.points_grid, values=self.wlvl)
            level = interpolator(x, y)
            level = np.expand_dims(level, axis=0)

            # Return as xarray DataArray, using dem_da's coordinates
            wlvl_da = xr.full_like(dem_chunk, dem_chunk.rio.nodata)
            wlvl_da.values = level

            return wlvl_da

        # init result raster
        create = hrt.check_create_new_file(output_file=output_file, overwrite=overwrite)

        if create:
            # get dem as xarray
            dem = self.dem_raster.open_rxr(chunksize=chunksize)
            self.prepare_interpolator_input()

            # create empty result array
            result_template = xr.full_like(dem, dem.rio.nodata)

            wlvl_da = dem.map_blocks(calc_level, args=[dem], template=result_template)

            self.wlvl_raster = hrt.Raster.write(
                output_file, result=wlvl_da, nodata=dem.rio.nodata, chunksize=chunksize
            )
        else:
            self.wlvl_raster = hrt.Raster(output_file)

        return self.wlvl_raster

    def __enter__(self):
        """With GridToWaterLevel(**args) as x. will call this func."""
        self._interpolator = None
        self._delaunay_grid_gdf = None
        return self

    def __exit__(self, *args):
        self._interpolator = None
        self._delaunay_grid_gdf = None


class GridToWaterDepth:
    def __init__(self, dem_path: Path, wlvl_path: Path):
        """Bereken waterdieptes geinterpoleerd naar de resolutie van de DEM, gebruikmakend van Delaunay-interpolatie

        Parameters
        ----------
        dem_path : Path
            Path naar de DEM
        wlvl_path : Path
            Pad naar grid met waterstanden
        """
        self.dem_raster = hrt.Raster(dem_path)
        self.wlvl_raster = hrt.Raster(wlvl_path)
        self.depth_raster = None  # Set on run.

        if not self.wlvl_raster.path.exists():
            raise FileNotFoundError(
                f"{self.wlvl_raster.path} does not exist. Provide valid path or run GridToWaterLevel first."
            )

    def run(self, output_file, chunksize: Union[int, None] = None, overwrite: bool = False):
        # get dem as xarray

        # init result raster
        self.depth_raster = hrt.Raster(output_file, chunksize=chunksize)
        create = hrt.check_create_new_file(output_file=self.depth_raster.path, overwrite=overwrite)

        if create:
            dem = self.dem_raster.open_rxr(chunksize=chunksize)
            level = self.wlvl_raster.open_rxr(chunksize=chunksize)

            # create empty result array
            result = level - dem
            result = xr.where(result < 0, 0, result)

            result.rio.set_crs(dem.rio.crs)
            self.depth_raster = hrt.Raster.write(output_file, result=result, nodata=0, chunksize=chunksize)

        return self.depth_raster

    def __enter__(self):
        """With GridToWaterDepth(**args) as x. will call this func."""
        return self

    def __exit__(self, *args):
        pass


# %%
if __name__ == "__main__":
    from hhnk_threedi_tools import Folders

    OVERWRITE = True

    folder_path = r"E:\02.modellen\23_Katvoed"
    folder = Folders(folder_path)
    output_fd = folder.output.one_d_two_d[0]
    threedi_result = folder.threedi_results.one_d_two_d[0]

    # grid_gdf = gpd.read_file(threedi_result.path/"grid_raw.gpkg", driver="GPKG")
    # grid_gdf = threedi_result.full_path("grid_corr.gpkg").load()
    grid_gdf = gpd.read_file(output_fd.grid_nodes_2d.path)

    calculator_kwargs = {
        "dem_path": folder.model.schema_base.rasters.dem.base,
        "grid_gdf": grid_gdf,
        "wlvl_column": "wlvl_max_orig",
    }

    # Init calculator
    with GridToWaterLevel(**calculator_kwargs) as self:
        self.run(output_file=threedi_result.full_path("wdepth_orig.tif"), overwrite=OVERWRITE)
        print("Done.")

# %%
