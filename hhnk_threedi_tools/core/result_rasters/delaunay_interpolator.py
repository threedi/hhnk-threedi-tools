import geopandas as gpd
import pandas as pd
from scipy.interpolate import LinearNDInterpolator
from scipy.spatial import Delaunay
from shapely.geometry import LineString, Polygon
from threedidepth import morton


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
    poly = grid_gdf[grid_gdf[wlvl_column] != no_data_value].unary_union
    boundary = poly.boundary

    # we voegen alle relevante punten samen tot 1 grid
    gdf = pd.concat(
        [
            get_points_grid(grid_gdf, poly, wlvl_column, no_data_value),
            get_boundary_points(grid_gdf, boundary, wlvl_column, no_data_value),
        ]
    )

    return gdf


def get_interpolator(
    grid_gdf: gpd.GeoDataFrame, wlvl_column: str, no_data_value: float, reorder=True
) -> LinearNDInterpolator:
    """LinearNDInterpolator op basis van Delauny triangulatie.

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
    LinearNDInterpolator
        LinearNDInterpolator
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

    return LinearNDInterpolator(Delaunay(points_grid), wlvl)
