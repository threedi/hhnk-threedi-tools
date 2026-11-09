"""Build and apply DEM-derived cross-sections for drainage ditches.

Run this file with a project folder, greppel GeoPackage, and DEM path to sample cross-sections along
greppel lines. The workflow joins profiles to nearby channels, calculates reference and bank levels,
builds cross-section tables, and updates the model's orifice, connection-node, and cross-section-location
layers. Invalid profiles and intermediate profile samples are saved under the project's source-data
folder. Model layers are overwritten. Plot generation is optional; pass ``--plot-profiles`` to save
profile plots alongside the intermediate data.

The project folder must follow the ``Folders`` layout, with DAMO ``HydroObject`` and ``Waterdeel`` layers
and a model GeoPackage containing the required channel, cross-section-location, orifice, and
connection-node layers. Profile width, DEM sample spacing, greppel spacing, and water-area buffer are
configured near the start of ``main``. Raster values equal to 10 are intentionally excluded as invalid,
and the search helper uses the project's fixed 0.5-unit raster resolution.
"""

# %%
from math import atan2, cos, sin
from pathlib import Path
from typing import Any, Literal, Optional, Tuple, Union

import geopandas as gpd
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import rasterio
from shapely import line_interpolate_point
from shapely.geometry import LineString, MultiPoint, Point
from shapely.geometry.base import BaseGeometry
from shapely.ops import nearest_points
from xarray import DataArray

from hhnk_threedi_tools import Folders


def nearest_intersect(
    boundary_geom: BaseGeometry,
    line_geom: BaseGeometry,
    point_geom: BaseGeometry,
) -> Tuple[Optional[BaseGeometry], Optional[float]]:
    """
    Find the intersection between a boundary and line that lies closest to a reference geometry. Return
    the selected Shapely point together with its planar distance from ``point_geom``. If the input
    geometries do not intersect, return ``(None, None)`` instead of raising an exception. All three
    geometries must use compatible coordinate systems; this helper does not reproject inputs or convert
    coordinate units. Distance is measured in the units of the input coordinates, so use projected
    coordinates when a physical distance is required. When the intersection contains multiple locations,
    Shapely's ``nearest_points`` operation chooses the location nearest to the reference geometry.
    """

    if boundary_geom.intersects(line_geom):
        intersection = nearest_points(point_geom, boundary_geom.intersection(line_geom))[1]
        nearest_dist = intersection.distance(point_geom)
    else:
        intersection = None
        nearest_dist = None

    return intersection, nearest_dist


def nearest_intersect_exclude(
    boundary_geom: Optional[BaseGeometry],
    line_geom: BaseGeometry,
    point_geom: BaseGeometry,
    exclude_point: Optional[BaseGeometry],
) -> Tuple[Optional[BaseGeometry], Optional[float]]:
    """
    Find a boundary-line intersection while applying the exclusion point as a distance reference. If
    ``exclude_point`` is missing, or the boundary does not intersect the line, return ``(None, None)``.
    For a MultiPoint intersection, omit candidates within 0.01 coordinate units of the excluded point
    and return the remaining candidate nearest to it. When the intersection is a single Point, the current
    implementation returns that point without checking whether it equals ``exclude_point``; its reported
    distance is measured from ``point_geom`` instead. Other intersection geometry types produce no result.
    The measurements are planar and use the input coordinate units; inputs are not reprojected or checked
    for a shared coordinate reference system.
    """
    intersection = None
    nearest_dist = None
    valid_points = []
    valid_dists = []

    if exclude_point is None:
        return intersection, nearest_dist

    elif boundary_geom is not None:
        if boundary_geom.intersects(line_geom):
            intersections = boundary_geom.intersection(line_geom)
            if isinstance(intersections, MultiPoint):
                for p in intersections.geoms:
                    dist = p.distance(exclude_point)
                    if dist > 0.01:
                        valid_points.append(p)
                        valid_dists.append(dist)
                if len(valid_dists) > 0:
                    nearest_dist = min(valid_dists)
                    for p in intersections.geoms:
                        dist = p.distance(exclude_point)
                        if dist == nearest_dist:  # kan ook met .index?
                            intersection = p
            elif isinstance(intersections, Point):
                intersection = intersections
                nearest_dist = intersections.distance(point_geom)

    return intersection, nearest_dist


def perpendicular_lines(
    p1: Tuple[float, float],
    p2: Tuple[float, float],
    distance: float,
    side: Literal["left", "right"],
) -> LineString:
    """
    Take two points and calculate points perpendicular  to first point and draw a line between them.

    Parameters
    ----------
    p1 = point 1 (x,y in list)

    p2 = point 2 (x,y in list)

    distance = distance new points from p1

    side = 'left' or 'right'

    Returns
    -------
    Perpendicular line to first point (LineString)
    """

    # # shifted coordinate gives None error
    # if p2 is None:
    #   perp_line = None
    # Delta x
    dx = p2[0] - p1[0]
    # Delta y
    dy = p2[1] - p1[1]
    # Angle between p1 and p2 in rad
    angle = atan2(dy, dx)
    # Displacement of points
    lx = sin(angle) * distance
    ly = cos(angle) * distance

    if side == "right":
        perp_coords = [p1[0] - lx, p1[1] + ly]
    elif side == "left":
        perp_coords = [p1[0] + lx, p1[1] - ly]
    else:
        raise ValueError("side must be 'left' or 'right'")

    perp_line = LineString([p1, perp_coords])

    return perp_line


def get_nearest_non_nodata(
    rioxarray_raster: DataArray,
    point_gdf: Point,
    tolerance: float = 0.5,
    pixelsize: float = 0.5,
    mode: Literal["min", "max", "med", "mean"] = "min",
) -> Union[float, int, np.floating[Any], np.integer[Any]]:
    """
    Sample a non-nodata raster value at ``point_gdf`` or search outward up to ``tolerance``. Coordinates
    are selected on the raster's ``x`` and ``y`` dimensions using nearest-neighbor lookup. If the initial
    sample is nodata, the function expands a square set of candidate coordinates in pixel-sized steps.
    ``mode`` reduces the non-nodata values gathered during that search using minimum, maximum, median
    (``med``), or mean. If no valid value is found before the tolerance limit, the result is NaN. The
    raster and point must already use matching coordinate systems and units. Although ``pixelsize`` is a
    parameter, the implementation resets it to 0.5 before sampling, so a caller-supplied value currently
    has no effect. Larger tolerances can require many repeated raster selections.
    """
    dist = 0.0
    pixelsize = 0.5
    x = point_gdf.x
    y = point_gdf.y
    result = rioxarray_raster.sel(x=x, y=y, method="nearest", tolerance=pixelsize).to_numpy()[0]
    x_set = {x}
    y_set = {y}
    while np.isnan(result) and dist < tolerance:
        dist = dist + pixelsize
        x_set.add(x - dist)
        x_set.add(x + dist)
        y_set.add(y - dist)
        y_set.add(y + dist)

        data = []
        for i in x_set:
            for j in y_set:
                data.append(rioxarray_raster.sel(x=i, y=j, method="nearest", tolerance=pixelsize).to_numpy()[0])

        if np.isnan(data).all():
            result = np.nan
        elif mode == "min":
            result = np.nanmin(data)
        elif mode == "max":
            result = np.nanmax(data)
        elif mode == "med":
            result = np.nanmedian(data)
        elif mode == "mean":
            result = np.nanmean(data)

    return result


def get_min_value_in_polygon(
    rioxarray_raster: DataArray,
    polygon: BaseGeometry,
    mode: Literal["min", "max", "med", "mean"] = "min",
) -> Union[float, int, np.floating[Any], np.integer[Any]]:
    """
    Find non-nodata value within polygon.
    If multiple pixels whotin polygon 'mode' desides.

    Basic zonal statistics without rasterstats module.

    Not as quick or accurate as `get_nearest_non_nodata`
    """
    # Mask the raster with the polygon
    masked_rioxarray_raster = rioxarray_raster.rio.clip([polygon], rioxarray_raster.rio.crs)
    # Get the data array
    data = masked_rioxarray_raster.data
    # Mask the data array to ignore nodata values
    data = np.ma.masked_equal(data, rioxarray_raster.rio.nodata)
    # Return the desired value
    if mode == "min":
        result = np.nanmin(data)
    elif mode == "max":
        result = np.nanmax(data)
    elif mode == "med":
        result = np.nanmedian(data)
    elif mode == "mean":
        result = np.nanmean(data)
    return result


def full_perpendicular_line(p1: Tuple[float, float], p2: Tuple[float, float], width: float) -> LineString:
    """
    Build a three-vertex cross-section centered at ``p1`` and perpendicular to the direction ``p1``-``p2``.
    ``width`` is the total segment length, split evenly between the left and right sides of the center.
    The returned LineString starts at the left endpoint, passes through ``p1``, and ends at the right
    endpoint. Input coordinates and width must use the same planar units; no CRS or unit conversion is
    performed. The function delegates each half-segment to ``perpendicular_lines`` and preserves the
    resulting coordinate order. ``p1`` and ``p2`` must be distinct because coincident coordinates do not
    define a direction; that case raises ``ValueError``. A non-positive width is not rejected explicitly,
    so callers should provide a positive width to obtain the expected cross-section orientation and size.
    """
    if p1[0] == p2[0] and p1[1] == p2[1]:
        raise ValueError("p1 and p2 should be differents")

    half_width = width / 2

    line_a = perpendicular_lines(p1, p2, half_width, "left")
    line_b = perpendicular_lines(p1, p2, half_width, "right")

    return LineString(
        [
            line_a.coords[-1],
            p1,
            line_b.coords[-1],
        ]
    )


def points_along_lines(
    lines: gpd.GeoDataFrame,
    space: float = 10,
    code_column: str = "code",
    include_endpoints: bool = True,
) -> gpd.GeoDataFrame:
    """
    Generate point records at regular distances along every line in ``lines``. ``space`` is measured in
    the line coordinate units, and ``code_column`` names the source attribute copied to each point's
    ``code`` field. For lines shorter than ``space``, create one point at the midpoint regardless of
    ``include_endpoints``. For longer lines, include both ends when the flag is true; otherwise start at
    one interval and omit the end. Consequently, a line whose length equals ``space`` produces no points
    when endpoints are excluded. The result contains sequential ``point_id``, ``code``, along-line
    ``distance``, and point geometry. When the input has a ``point_id`` column, copy it as ``profile_id``.
    The output GeoDataFrame retains the input CRS. Empty inputs produce an empty GeoDataFrame.
    """
    points: list[dict[str, Any]] = []

    for _, line in lines.iterrows():
        length = line.geometry.length

        # create a single point at the midpoint of the line if the line is shorter than the space
        if length < space:
            distances = np.array([length / 2])

        # Create distances along the line, including the end point
        elif include_endpoints:
            distances = np.arange(0, length, space)
            distances = np.append(distances, length)

        # Create distances along the line, without the end point
        else:
            distances = np.arange(space, length, space)

        # Create points at each distance along the line using line_interpolate_point to get the geometry
        for distance in distances:
            point_data = {
                "point_id": len(points),
                "code": line[code_column],
                "distance": distance,
                "geometry": line_interpolate_point(line.geometry, distance),
            }

            if "point_id" in lines.columns:
                point_data["profile_id"] = line["point_id"]

            points.append(point_data)

    return gpd.GeoDataFrame(
        points,
        geometry="geometry",
        crs=lines.crs,
    )


# %%


# greppels = gpd.read_file(r"H:\02.modellen\NZK_leggertool\01_source_data\greppels_nzk.shp")
# points_gdf = points_along_lines(lines=greppels, space=10)
# %%


def draw_perpendicular_lines(
    width: float,
    points_gdf: gpd.GeoDataFrame,
    greppels: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """
    Create one perpendicular cross-section for each point in ``points_gdf`` using its matching line in
    ``greppels``. The line is selected by matching the point's ``code`` to the source ``CODE`` field.
    Its local direction is estimated from a location 0.5 coordinate units ahead, or behind when the point
    is at the line end. ``width`` is passed as the total width of each cross-section. The returned
    GeoDataFrame contains ``code``, ``point_id``, the source-line ``distance``, and line geometry, and
    retains the CRS of ``greppels``. The first matching source line is used; missing codes or empty input
    geometries are not handled specially.
    """

    profiles = []
    codes = points_gdf.groupby("code")
    for code, group in codes:
        line = greppels.loc[greppels["CODE"] == code, "geometry"].iloc[0]
        # print(code)
        # print(group)
        for _, point in group.iterrows():
            p1 = point["geometry"]
            distance = point["distance"]

            if distance < line.length:
                p2_distance = min(distance + 0.5, line.length)
            else:
                p2_distance = max(distance - 0.5, 0)

            p2 = line_interpolate_point(line, p2_distance)

            profile_line = full_perpendicular_line((p1.x, p1.y), (p2.x, p2.y), width)

            profiles.append(
                {
                    "code": code,
                    "point_id": point["point_id"],
                    "distance": distance,
                    "geometry": profile_line,
                }
            )

    profiles_lines = gpd.GeoDataFrame(
        profiles,
        geometry="geometry",
        crs=greppels.crs,
    )

    return profiles_lines


def sample_elevation_per_profile_point(
    width: float,
    points_gdf: gpd.GeoDataFrame,
    greppels: gpd.GeoDataFrame,
    dem_path: Path,
    code_column: str,
    waterdeel_gdf: gpd.GeoDataFrame,
    space: float,
    buffer_waterdeel: float,
) -> Tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """
    Build cross-sections, place sample points along them, and read elevations from ``dem_path``. Candidate
    sections are first restricted to those intersecting ``waterdeel_gdf``; a buffer of
    ``buffer_waterdeel`` is then used to retain nearby sample points and clip the output lines. ``space``
    controls spacing along each section, while ``code_column`` identifies the point attribute copied to
    the generated samples. Raster coordinates are sampled directly, so the input layers and DEM must have
    compatible CRSs and units. Samples whose value equals 10 are treated as invalid and removed together
    with their corresponding profile lines. Return a pair containing the sampled point GeoDataFrame first
    and the matching clipped-line GeoDataFrame second; both retain their spatial reference information.
    """

    coords = []
    elevations = []

    profile_lines = draw_perpendicular_lines(width, points_gdf, greppels)
    profile_lines = gpd.sjoin(profile_lines, waterdeel_gdf[["geometry"]], how="inner", predicate="intersects")

    profile_lines = profile_lines.drop(columns="index_right")

    profile_lines = profile_lines.drop_duplicates(subset="point_id")

    profile_points_gdf = points_along_lines(profile_lines, space=space, code_column=code_column)

    # filter points without change their original or aligned distance
    waterdeel_buffer = waterdeel_gdf.geometry.buffer(buffer_waterdeel).union_all()

    profile_points_gdf = profile_points_gdf.loc[profile_points_gdf.geometry.intersects(waterdeel_buffer)].copy()

    waterdeel_buffer_gdf = gpd.GeoSeries([waterdeel_buffer], crs=waterdeel_gdf.crs)
    profile_lines = gpd.clip(
        profile_lines,
        waterdeel_buffer_gdf,
        keep_geom_type=True,
    )

    profile_lines = profile_lines.loc[profile_lines["point_id"].isin(profile_points_gdf["profile_id"])].copy()

    for point in profile_points_gdf.geometry:
        if not isinstance(point, Point):
            raise TypeError("Profile sample geometries must be Points")
        coords.append((point.x, point.y))

    with rasterio.open(dem_path) as dem:
        for coord in coords:
            samples = dem.sample([coord], indexes=1)
            value = next(samples)
            elevations.append(value[0])

    profile_points_gdf["elevation"] = elevations

    invalid_profile_ids = profile_points_gdf.loc[
        profile_points_gdf["elevation"] == 10,
        "profile_id",
    ].unique()

    profile_points_gdf = profile_points_gdf.loc[~profile_points_gdf["profile_id"].isin(invalid_profile_ids)].copy()
    profile_lines_gdf = profile_lines.loc[~profile_lines["point_id"].isin(invalid_profile_ids)].copy()

    return profile_points_gdf, profile_lines_gdf


def plot_profile(gpkg_path: Path, code: Any, output_path: Path) -> None:
    """
    Read profile samples from ``gpkg_path``, select rows whose ``code`` equals the requested value, and
    save a plot to ``output_path``. Each ``profile_id`` becomes one plotted series; its rows are sorted by
    cross-section distance before distance is placed on the horizontal axis and elevation on the vertical
    axis. The function labels both axes, adds a title and grid, applies tight layout, and closes the figure
    after saving so repeated calls do not retain open Matplotlib figures. ``gpkg_path`` is read using
    GeoPandas' default layer selection. If no matching profile rows exist, raise ``ValueError`` before
    creating the figure. The output format is inferred by Matplotlib from the destination filename, so a
    PNG suffix is conventional but not required.
    """
    matplotlib.use("Agg")

    points = gpd.read_file(gpkg_path)

    greppel_profiles = points.loc[points["code"] == code]

    if greppel_profiles.empty:
        raise ValueError(f"No profiles found for greppel {code}.")

    fig, ax = plt.subplots(figsize=(10, 5))

    for _, profile in greppel_profiles.groupby("profile_id"):
        profile = profile.sort_values("distance")

        ax.plot(
            profile["distance"],
            profile["elevation"],
            alpha=0.5,
        )

    ax.set_xlabel("Distance along cross-section (m)")
    ax.set_ylabel("Elevation (m)")
    ax.set_title(f"Cross-section profiles — {code}")
    ax.grid(True)

    fig.tight_layout()
    fig.savefig(output_path, dpi=150)
    plt.close(fig)

    print(f"Figure saved: {output_path}", flush=True)


# %%
def get_height_and_reference_level(
    greppels_gdf: gpd.GeoDataFrame,
    channel_gdf: gpd.GeoDataFrame,
    profile_lines_gdf: gpd.GeoDataFrame,
    profile_points_gdf: gpd.GeoDataFrame,
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """
    Associate sampled profiles with channels intersecting a one-unit buffer around ``greppels_gdf``. The
    profile-point distances are rounded to three decimals, then each channel/profile group receives a
    reference level equal to its minimum sampled elevation and a relative ``height`` value. A serialized
    cross-section table is calculated for each group with sufficient vertical range and merged back onto
    the point rows. Return the enriched profile-point GeoDataFrame and a second GeoDataFrame identifying
    profiles that could not produce a table. The calculation uses planar geometry operations and assumes
    the input layers share compatible CRSs and contain the expected ID, distance, and elevation columns.
    The current implementation initializes the invalid-profile result only when at least one invalid
    profile is found, so callers should ensure that condition or handle the resulting unbound-local error.
    """
    # buffer and dissolve greppels.
    greppels_buffer = greppels_gdf.buffer(1).union_all()
    greppels_buffer_gdf = gpd.GeoDataFrame(
        geometry=[greppels_buffer],
        crs=greppels_gdf.crs,
    )

    # Select channels that intersects with greppels.
    channel_join = channel_gdf.sjoin(greppels_buffer_gdf, how="inner", predicate="intersects")
    channel_join = channel_join.drop(columns="index_right")
    channel_join = channel_join.drop_duplicates()
    channel_join = channel_join[["id", "geometry"]].rename(columns={"id": "channel_id"})

    # Select the profile lines to the joined channels.
    profile_lines_intersect = profile_lines_gdf.sjoin(channel_join, how="inner", predicate="intersects")
    profile_lines_intersect = profile_lines_intersect.drop_duplicates()

    # asing channel id to the points.
    profile_lines_id_channel = (
        profile_lines_intersect[["point_id", "channel_id"]]
        .drop_duplicates()
        .rename(columns={"point_id": "profile_id"})
    )

    new_profile_points = profile_points_gdf.merge(
        profile_lines_id_channel, how="inner", on="profile_id", validate="many_to_one"
    )
    # Round distances before averaging matching positions.
    new_profile_points["distance"] = new_profile_points["distance"].round(3)

    profile_points_with_heights = new_profile_points.copy()

    # group by channels and keep columns distance and height
    groups = profile_points_with_heights.groupby(["channel_id", "profile_id"])

    # loop over the groups to compute reference level, heigh and crossection table per cross section profile
    cross_section_tables_list = []
    invalid_profiles = []
    for key in groups.groups.keys():
        channel_id, profile_id = key
        profile = groups.get_group(key).copy()

        reference_level = profile["elevation"].min()
        profile["height"] = profile["elevation"] - reference_level
        selection = (profile_points_with_heights["channel_id"] == channel_id) & (
            profile_points_with_heights["profile_id"] == profile_id
        )

        profile_points_with_heights.loc[selection, "reference_level"] = reference_level
        profile_points_with_heights.loc[selection, "height"] = profile["height"]

        cross_section_tables = get_cross_section_table(profile, height_step=0.10)
        if cross_section_tables is None:
            invalid_profiles.append(
                {
                    "channel_id": channel_id,
                    "profile_id": profile_id,
                }
            )

            print(
                f"Warning: Channel {channel_id}, profile {profile_id}: "
                "insuficient vertical range to build t he cross sect ion table"
                "Check alignment of Hydroobject and Waterdeel with the DEM"
            )
            continue
        cross_section_tables["channel_id"] = channel_id
        cross_section_tables["profile_id"] = profile_id

        cross_section_tables_list.append(cross_section_tables)
    # merge results.
    if cross_section_tables_list:
        cross_section_tables = pd.concat(cross_section_tables_list, ignore_index=True)
    else:
        cross_section_tables = pd.DataFrame(columns=["channel_id", "profile_id", "cross_section_table"])
    # print(cross_section_tables[["channel_id", "profile_id", "cross_section_table"]].head())

    profile_points_with_heights = profile_points_with_heights.merge(
        cross_section_tables, on=["channel_id", "profile_id"], how="left", validate="many_to_one"
    )

    invalid_profiles_gdf = profile_lines_gdf.iloc[0:0].copy()
    if invalid_profiles:
        invalid_profiles_gdf = profile_lines_gdf.iloc[0].copy()
        invalid_profiles_df = pd.DataFrame(invalid_profiles)

        invalid_profiles_gdf = profile_lines_gdf.merge(
            invalid_profiles_df, left_on="point_id", right_on="profile_id", how="inner"
        )
    return profile_points_with_heights, invalid_profiles_gdf


def get_cross_section_table(
    profile: pd.DataFrame,
    height_step: float = 0.10,
) -> Optional[pd.DataFrame]:
    """
    Convert a sampled profile into a one-row table containing serialized widths at successive heights.
    The input must provide numeric ``distance`` and ``height`` columns; duplicate pairs are removed and
    rows are ordered by distance before processing. For each level from zero through the lower endpoint
    height, calculate the horizontal width lying below that level, interpolating where a segment crosses
    the level. ``height_step`` controls the vertical increment, and the final endpoint height is included
    even when it is not an exact multiple of the step. The output has one ``cross_section_table`` value
    containing newline-separated height-width pairs formatted to three decimals. Return ``None`` when
    only one or zero levels can be produced; channel identifiers are not added by this helper.
    """
    profile = profile[["distance", "height"]]
    profile = profile.drop_duplicates().sort_values("distance")

    distances = profile["distance"].to_numpy()
    profile_heights = profile["height"].to_numpy()

    # Stop at the lower endpoint to avoid extending beyond the profile. Max hights of the taluds
    max_height = min(profile_heights[0], profile_heights[-1])

    # Vertical interval between levels at which we calculate the cross-section width.
    heights = np.arange(0, max_height, height_step)
    heights = np.append(heights, max_height)

    cross_section_rows = []

    for height in heights:
        width = 0.0

        for index in range(len(distances) - 1):
            x1 = distances[index]
            x2 = distances[index + 1]

            h1 = profile_heights[index]
            h2 = profile_heights[index + 1]

            # Include the entire segment when both ends are below the cut.
            if h1 <= height and h2 <= height:
                width += x2 - x1

            # Include only the portion below the horizontal cut.
            elif min(h1, h2) < height < max(h1, h2):
                # find where the height intersects the segment using linear interpolation.
                crossing = x1 + ((height - h1) / (h2 - h1)) * (x2 - x1)

                if h1 < height:
                    width += crossing - x1
                else:
                    width += x2 - crossing

        cross_section_rows.append(f"{height:.3f},{width:.3f}")

    # Replace a zero bottom width with the width at the next height.
    if len(cross_section_rows) >= 2:
        first_height, first_width = cross_section_rows[0].split(",")
        next_height, next_width = cross_section_rows[1].split(",")

        if float(first_width) == 0:
            cross_section_rows[0] = f"{first_height},{next_width}"
    if len(cross_section_rows) <= 1:
        return None

    cross_section = pd.DataFrame(
        {
            "cross_section_table": ["\n".join(cross_section_rows)],
        }
    )

    return cross_section


def get_bank_level(profile_points_with_heights: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Estimate a bank elevation independently for each ``channel_id`` and ``profile_id`` group. Sort each
    profile by distance, calculate adjacent elevation slopes, and identify locations where absolute slope
    decreases toward either outside bank. The strongest positive flattening candidate on each side is
    collected; when candidates exist, the lower candidate elevation becomes that profile's bank level.
    Groups without a candidate retain a NaN value and are reported to standard output. The function adds
    or replaces the ``bank_level`` column directly on ``profile_points_with_heights`` and returns that same
    GeoDataFrame. Required columns include channel and profile identifiers, distance, and elevation.
    Distances and elevations are assumed to be numeric and consistently measured; no CRS or data-quality
    validation is performed.
    """

    profile_points_with_heights["bank_level"] = float("nan")

    groups = profile_points_with_heights.groupby(["channel_id", "profile_id"])

    for key in groups.groups.keys():
        channel_id, profile_id = key

        profile = groups.get_group(key).sort_values("distance").copy()

        profile["slope"] = profile["elevation"].diff() / profile["distance"].diff()
        slope_abs = profile["slope"].abs()

        # Positive values indicate flattening toward the outside.
        profile["flattening_right"] = slope_abs.shift(-1) - slope_abs
        profile["flattening_left"] = slope_abs - slope_abs.shift(-1)

        bottom_distance = profile.loc[profile["elevation"].idxmin(), "distance"]

        right = profile.loc[profile["distance"] < bottom_distance]
        left = profile.loc[profile["distance"] > bottom_distance]

        right_candidates = right.loc[right["flattening_right"] > 0]
        left_candidates = left.loc[left["flattening_left"] > 0]
        bank_levels = []

        if not right_candidates.empty:
            right_index = right_candidates["flattening_right"].idxmax()
            bank_levels.append(profile.loc[right_index, "elevation"])

        if not left_candidates.empty:
            left_index = left_candidates["flattening_left"].idxmax()
            bank_levels.append(profile.loc[left_index, "elevation"])

        if not bank_levels:
            print(f"No bank candidates: channel {channel_id}")
            continue

        bank_level = min(bank_levels)

        selection = (profile_points_with_heights["channel_id"] == channel_id) & (
            profile_points_with_heights["profile_id"] == profile_id
        )

        profile_points_with_heights.loc[selection, "bank_level"] = round(bank_level, 3)

    return profile_points_with_heights


def update_cross_sections(
    profile_points_with_heights: gpd.GeoDataFrame,
    cross_section_locations: gpd.GeoDataFrame,
    profile_lines_gdf: gpd.GeoDataFrame,
    channel_gdf: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """
    Create replacement cross-section-location rows from profile reference levels, bank levels, and
    serialized tables, then combine them with existing locations. Candidate profiles are deduplicated and
    rows without a section table are discarded. Each generated location is placed at the intersection of
    its profile line and channel geometry; empty intersections are skipped with a warning. Existing rows
    for candidate channels are removed before the generated rows are appended. If no new row is generated,
    return a copy of the original locations. The result preserves the cross-section GeoDataFrame's CRS and
    carries forward its schema using the first existing row as a template. Inputs must contain the IDs,
    geometry, and computed level columns used by the operation; empty templates are not supported.
    """

    channel_cross_sections_banklevel = (
        profile_points_with_heights[
            ["channel_id", "profile_id", "reference_level", "bank_level", "cross_section_table"]
        ]
        .drop_duplicates()
        .dropna(subset=["cross_section_table"])
    )

    new_cross_section = []
    next_id = int(cross_section_locations["id"].max()) + 1
    template = cross_section_locations.iloc[[0]].copy()
    template["cross_section_shape"] = 6
    template["friction_type"] = 2
    template["friction_value"] = 0.050

    for index, channel in channel_cross_sections_banklevel.iterrows():
        # Check if the channel already has a cross-section location.
        cross_section = template.copy()
        cross_section["channel_id"] = channel["channel_id"]
        # locater the cross sect ion on the channel
        profile_line = profile_lines_gdf.loc[
            profile_lines_gdf["point_id"] == channel["profile_id"],
            "geometry",
        ].iloc[0]

        channel_line = channel_gdf.loc[
            channel_gdf["id"] == channel["channel_id"],
            "geometry",
        ].iloc[0]

        location = profile_line.intersection(channel_line)

        if location.is_empty:
            print(f"Warning: No intersection found for channel {channel['channel_id']}. Skipping.")
            continue

        cross_section["geometry"] = location
        cross_section["code"] = f"grepple_{next_id}"
        cross_section["id"] = next_id
        cross_section["updated"] = True
        cross_section["reference_level"] = round(channel["reference_level"], 3)
        cross_section["bank_level"] = channel["bank_level"]
        cross_section["cross_section_table"] = channel["cross_section_table"]

        new_cross_section.append(cross_section)
        next_id += 1

    # print(new_cross_section[0].columns.tolist())
    # print(new_cross_section[0][["id", "channel_id", "updated", "cross_section_table"]])
    if not new_cross_section:
        return cross_section_locations.copy()

    processed_channels = pd.concat(
        new_cross_section,
        ignore_index=True,
    )["channel_id"].unique()

    processed_channels = channel_cross_sections_banklevel["channel_id"]

    cross_section_locations = cross_section_locations.loc[
        ~cross_section_locations["channel_id"].isin(processed_channels)
    ].copy()

    cross_section_concatenated = pd.concat([cross_section_locations] + new_cross_section, ignore_index=True)
    cross_section_locations = gpd.GeoDataFrame(
        cross_section_concatenated, geometry="geometry", crs=cross_section_locations.crs
    )
    return cross_section_locations


# %%
def update_model(
    cross_section_locations_updated: gpd.GeoDataFrame,
    channel_gdf: gpd.GeoDataFrame,
    hydroobject: gpd.GeoDataFrame,
    orifice_gdf: gpd.GeoDataFrame,
    connection_node_gdf: gpd.GeoDataFrame,
    model_path: Path,
) -> None:
    """
    Apply rows marked ``updated`` in ``cross_section_locations_updated`` to the model layers stored at
    ``model_path``. Connected orifices on secondary or tertiary hydroobjects receive a crest level based
    on the highest reference level of their updated neighboring channels. Connection nodes exclusive to
    updated channels receive an initial water level derived from the nearest available bank level, reduced
    by ten units. The function writes the modified ``orifice``, ``connection_node``, and
    ``cross_section_location`` layers to the GeoPackage and returns ``None``. It mutates the supplied
    GeoDataFrames while preparing those writes. Existing layer names and the columns referenced by the
    implementation must be present, and the model path must point to a writable GeoPackage. The routine
    does not create backups, reproject geometries, or provide transactional rollback if a write fails.
    """
    # filter channels id base on the updated filter
    channel_id = cross_section_locations_updated.loc[
        cross_section_locations_updated["updated"] == True
    ].channel_id.to_list()

    # Create a set of connection nodes.
    connection_node_start_id = channel_gdf.loc[channel_gdf["id"].isin(channel_id)].connection_node_start_id.to_list()
    connection_node_end_id = channel_gdf.loc[channel_gdf["id"].isin(channel_id)].connection_node_end_id.to_list()
    connection_nodes_set = set()
    connection_nodes_set.update(connection_node_end_id)
    connection_nodes_set.update(connection_node_start_id)

    # select channels to be modify base on channels id filter
    channels_updated = channel_gdf.loc[channel_gdf["id"].isin(channel_id)]

    # Create a copy and buffer all the hydroobjects that has a different category than 1
    secundary_terciary_buffer = hydroobject.loc[hydroobject["CATEGORIEOPPWATERLICHAAM"] != 1].copy()
    secundary_terciary_buffer["geometry"] = secundary_terciary_buffer.geometry.buffer(1)

    # Join the orifices that are within the secundary and terciary buffer
    orifice_filter = orifice_gdf.sjoin(secundary_terciary_buffer, how="inner", predicate="within")

    # filter them to select only the greppels orifices
    orifice_greppels = orifice_filter.loc[
        orifice_filter["connection_node_start_id"].isin(connection_nodes_set)
        | orifice_filter["connection_node_end_id"].isin(connection_nodes_set)
    ]

    # loop over the orifice to update crest level.
    for idx, orifice in orifice_greppels.iterrows():
        # select connectionn nodes from orifices.
        orifice_start = orifice.connection_node_start_id
        orifice_end = orifice.connection_node_end_id

        # selec channels base on orifices coonnection nodes to get reference levels from orifices
        connected_channel_ids = channels_updated.loc[
            channels_updated["connection_node_start_id"].isin([orifice_start, orifice_end])
            | channels_updated["connection_node_end_id"].isin([orifice_start, orifice_end]),
            "id",
        ]

        reference_levels = (
            cross_section_locations_updated.loc[
                cross_section_locations_updated["channel_id"].isin(connected_channel_ids),
                "reference_level",
            ]
            .dropna()
            .unique()
        )

        # compare crest level against reference. It wil  change if reference is higher than crest level
        target_crest_level = round(max(reference_levels), 3)

        # if orifice["crest_level"] > target_crest_level:
        orifice_gdf.loc[idx, "crest_level"] = target_crest_level

    # update model.
    orifice_gdf.to_file(model_path, layer="orifice", driver="GPKG")

    # Here start the porcess to update initial waterlevels
    # filter cross section updates
    cross_section_updated = cross_section_locations_updated.loc[cross_section_locations_updated["updated"] == True]
    greppel_channel_ids = cross_section_updated["channel_id"].to_list()

    channels_no_greppels = channel_gdf.loc[~channel_gdf["id"].isin(greppel_channel_ids)]

    connection_id_no_greppels = set(channels_no_greppels["connection_node_start_id"])
    connection_id_no_greppels.update(channels_no_greppels["connection_node_end_id"])

    for node_id in connection_nodes_set:
        if node_id in connection_id_no_greppels:
            continue
        node_geometry = connection_node_gdf.loc[
            connection_node_gdf["id"] == node_id,
            "geometry",
        ].iloc[0]

        connected_channels = channels_updated.loc[
            (channels_updated["connection_node_start_id"] == node_id)
            | (channels_updated["connection_node_end_id"] == node_id)
        ]

        nearest_distance = float("inf")
        nearest_bank_level = None

        for _, channel in connected_channels.iterrows():
            sections = cross_section_updated.loc[cross_section_updated["channel_id"] == channel["id"]].dropna(
                subset=["bank_level"]
            )

            node_distance = channel.geometry.project(node_geometry)

            for _, section in sections.sort_values("id").iterrows():
                section_distance = channel.geometry.project(section.geometry)

                distance_to_node = abs(section_distance - node_distance)
                if distance_to_node < nearest_distance:
                    nearest_distance = distance_to_node
                    nearest_bank_level = section["bank_level"]

        if nearest_bank_level is None:
            print(f"Warning: Node {node_id}: No valid bank level")
            continue

        connection_node_gdf.loc[
            connection_node_gdf["id"] == node_id,
            "initalwaterlevel",
        ] = round(nearest_bank_level - 10, 2)
    # update connectio node.
    connection_node_gdf.to_file(model_path, layer="connection_node", driver="GPKG")

    cross_section_locations_updated.to_file(model_path, layer="cross_section_location", driver="GPKG", index=False)


# %%
if __name__ == "__main__":
    # paths
    folder = Folders(r"H:\02.modellen\grootslag_greppels_clip_add_crst")
    damo_path = folder.source_data.damo.path
    model_path = folder.model.schema_base.model_path()
    dem_path = Path(
        r"\\corp.hhnk.nl\data\Hydrologen_data\Data\02.modellen\grootslag_leggertool\02_schematisation\00_basis\rasters\dem_grootslag.tif"
    )
    greppels = r"H:\02.modellen\grootslag_leggertool\01_source_data\greppels_from_geoweb_wss_clipped.gpkg"

    # Inputs(read geodataframes)
    hydroobject = gpd.read_file(damo_path, layer="HydroObject")
    waterdeel_gdf = gpd.read_file(damo_path, layer="Waterdeel")
    greppels_gdf = gpd.read_file(greppels)
    channel_gdf = gpd.read_file(model_path, layer="channel")
    cross_section_locations = gpd.read_file(model_path, layer="cross_section_location")
    orifice_gdf = gpd.read_file(model_path, layer="orifice")
    culver_gdf = gpd.read_file(model_path, layer="culvert")
    connection_node_gdf = gpd.read_file(model_path, layer="connection_node")
    width = 5  # profile line width (meters)
    space = 0.15  # space between points (meters)
    space_points_along_lines = 20  # space between points along the greppels (meters)
    buffer_waterdeel = 0.75
    # fucntions.
    # draw profile lines and points along greppels
    points_gdf = points_along_lines(
        lines=greppels_gdf,
        space=space_points_along_lines,
        code_column="CODE",
        include_endpoints=False,
    )

    # Sample profile points using dem
    profile_points_gdf, profile_lines_gdf = sample_elevation_per_profile_point(
        width,
        points_gdf,
        greppels_gdf,
        dem_path,
        code_column="code",
        waterdeel_gdf=waterdeel_gdf,
        space=space,
        buffer_waterdeel=buffer_waterdeel,
    )
    # built cross section table base on: channel code and point location on the profile lines and save invalid profile
    # Save invalid profiles to check them later if it is needed.
    profile_points_with_heights, invalid_profiles_gdf = get_height_and_reference_level(
        greppels_gdf=greppels_gdf,
        channel_gdf=channel_gdf,
        profile_lines_gdf=profile_lines_gdf,
        profile_points_gdf=profile_points_gdf,
    )

    # save invalid profiles
    profile_points_folder = Path(folder.source_data.path / "profile_data")
    profile_points_folder.mkdir(exist_ok=True)
    profile_points_path = profile_points_folder / "profile_points.gpkg"
    invalid_profiles_path = profile_points_folder / "invalid_profiles.gpkg"
    invalid_profiles_gdf.to_file(invalid_profiles_path, driver="GPKG")
    profile_points_with_heights.to_file(profile_points_path, driver="GPKG")

    # get bank level base on the flattering from the banks. (minimum from both side banks.)
    cross_section_banklevels = get_bank_level(profile_points_with_heights)

    # update cross section
    cross_section_locations_updated = update_cross_sections(
        profile_points_with_heights,
        cross_section_locations.copy(),
        profile_lines_gdf,
        channel_gdf,
    )

    update_model(
        cross_section_locations_updated,
        channel_gdf,
        hydroobject,
        orifice_gdf,
        connection_node_gdf,
        model_path,
    )
# %%
# Plot to check profiles.
codes = profile_points_with_heights["code"].unique().tolist()

for code in codes:
    save_path = profile_points_folder / f"{code}.png"
    plot_profile(gpkg_path=profile_points_path, code=code, output_path=save_path)

# # %%
# profile_points_with_heights.to_file(
#     r"H:\02.modellen\grootslag_leggertool\cross_section_points_with_heights.gpkg",
#     driver="GPKG",
# )
# profile_points_gdf.to_file(
#     r"H:\02.modellen\grootslag_leggertool\cross_section_points_function.gpkg",
#     driver="GPKG",
# )
# profile_lines_gdf.to_file(
#     r"H:\02.modellen\grootslag_leggertool\cross_section_lines_function.gpkg",
#     driver="GPKG",
# )

# # %%
# cross_section_banklevels.to_file(
#     r"H:\02.modellen\grootslag_leggertool\cross_section_lines_banklevel.gpkg",
#     driver="GPKG",
# )

# cross_section_locations_updated.to_file(
#     r"H:\02.modellen\grootslag_leggertool\cross_section_updated.gpkg",
#     driver="GPKG",
# )


# %%
