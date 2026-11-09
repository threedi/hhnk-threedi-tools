"""
Process greppel cross-sections from source geometries and a DEM, then update the 3Di model. When run directly,
it loads HydroObject and Waterdeel, greppel lines, channels, cross-section locations, orifices, and connection nodes.
It places regularly spaced points along greppels, constructs perpendicular profiles, clips them to water features,
and samples DEM elevations; elevation 10 marks invalid samples. Valid profiles are aggregated by channel to derive
reference levels, bank levels, and width-height tables. The script saves profile and invalid-profile GeoPackages,
updates model layers, and writes one plot per greppel code. Input paths and sampling parameters are configured in
the executable block.
"""

# %%
from math import atan2, cos, sin
from pathlib import Path
from typing import Any

import geopandas as gpd
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import rasterio
from shapely import line_interpolate_point
from shapely.geometry import LineString
from shapely.ops import nearest_points

from hhnk_threedi_tools import Folders


def nearest_intersect(boundary_geom: Any, line_geom: Any, point_geom: Any) -> tuple[Any | None, float | None]:
    """
    Find the closest intersection between a line and a boundary segment relative to a target point. The function
      tests whether the boundary and line overlap; when they do, it uses the nearest point on the intersection
      geometry to the provided point and measures that distance. If there is no intersection, both outputs are
      returned as None. This helper is useful during profile analysis, especially when choosing a valid reference
      point while avoiding mismatches across clipped or partial line features.
    """

    if boundary_geom.intersects(line_geom):
        intersection = nearest_points(point_geom, boundary_geom.intersection(line_geom))[1]
        nearest_dist = intersection.distance(point_geom)
    else:
        intersection = None
        nearest_dist = None

    return intersection, nearest_dist


def nearest_intersect_exclude(
    boundary_geom: Any,
    line_geom: Any,
    point_geom: Any,
    exclude_point: Any,
) -> tuple[Any | None, float | None]:
    """
    Find the closest valid intersection between a boundary and a line while excluding a known point from the
    result set. The function first checks whether the exclusion target is present; if so, it filters out nearby
    intersection candidates that fall too close to that excluded point. It then selects the nearest remaining
    option and returns both the geometry and its distance to the supplied reference point. This is useful when a
    profile crosses a boundary at multiple locations and a single point has to be rejected to keep geometry
    consistent during alignment.
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
            if intersections.geom_type == "MultiPoint":
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
            elif intersections.geom_type == "Point":
                intersection = intersections
                nearest_dist = intersections.distance(point_geom)

    return intersection, nearest_dist


def perpendicular_lines(
    p1: tuple[float, float],
    p2: tuple[float, float],
    distance: float,
    side: str,
) -> LineString:
    """
    Construct a perpendicular segment from a base point using a direction vector derived from the line between
    two coordinates. The function computes the local angle, offsets the endpoint by the requested distance, and
    chooses a left or right orientation based on the provided side argument. This helper is used to build profile
    cross-section lines around a channel axis while preserving geometric orientation. The result is a Shapely
    LineString that begins at the reference point and ends at the offset endpoint, making it easy to transform
    later into a full profile geometry.
    """

    dx = p2[0] - p1[0]
    dy = p2[1] - p1[1]
    angle = atan2(dy, dx)
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
    rioxarray_raster: Any,
    point_gdf: gpd.GeoDataFrame,
    tolerance: float = 0.5,
    pixelsize: float = 0.5,
    mode: str = "min",
) -> float:
    """
    Find the nearest valid raster value near a point while stepping outward until a non-NA value is found or
    tolerance is reached. The function starts by sampling the exact coordinate and then expands a search square
    outward in fixed pixel increments. It supports several aggregation modes, including minimum, maximum, median,
    and mean, which makes it useful for filling small gaps in raster information. The method is intentionally
    simple for local interpolation and is most effective in small tolerances where the search space stays compact
    and computational overhead remains low.
    """
    dist = 0
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


def get_min_value_in_polygon(rioxarray_raster: Any, polygon: Any, mode: str = "min") -> float:
    """
    Return a representative non-NA raster value found inside a polygon by clipping the raster to that geometry
    and summarizing the masked cells. The function uses a simple zonal-statistics approach without external raster
    libraries, making it lightweight and easy to reuse across local work. It can return the minimum, maximum,
    median, or mean value depending on the selected mode. This is helpful when a polygon-based summary is needed
    quickly, even though it is less exact than more specialized processing tools for larger or more complex rasters.
    """
    masked_rioxarray_raster = rioxarray_raster.rio.clip([polygon], rioxarray_raster.rio.crs)
    data = masked_rioxarray_raster.data
    data = np.ma.masked_equal(data, rioxarray_raster.rio.nodata)
    if mode == "min":
        result = np.nanmin(data)
    elif mode == "max":
        result = np.nanmax(data)
    elif mode == "med":
        result = np.nanmedian(data)
    elif mode == "mean":
        result = np.nanmean(data)
    return result


def full_perpendicular_line(p1: tuple[float, float], p2: tuple[float, float], width: float) -> LineString:
    """
    Build a full cross-section geometry centered on the base point and perpendicular to the segment defined by
    the two input coordinates. The function halves the requested width and creates a line on both sides of the
    reference point using the left and right perpendicular offsets. It then returns a three-point LineString that
    includes the left endpoint, the original point, and the right endpoint. This geometry is useful for cross-section
    sampling because it provides a simple, oriented profile line across a channel without requiring broader spatial
    interpolation or extra transformation steps.
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
    align_to_axis: bool = True,
) -> gpd.GeoDataFrame:
    """
    Generate a point record at regular distances along each line in a GeoDataFrame. The function supports both
    standard linear sampling and alignment along a local axis when distance metadata is available. It produces a
    GeoDataFrame with point identifiers, profile metadata, distance values, and geometry, while optionally carrying
    aligned offsets for profile reconstruction. This is especially useful in channel and greppel workflows where
    repeated cross-sections must be placed at fixed intervals and later compared against a reference axis or original
    line direction.
    """
    points: list[dict[str, Any]] = []

    for _, line in lines.iterrows():
        length = line.geometry.length
        axis_distance = None
        direction_sign = None
        aligned_distances = None

        if align_to_axis:
            axis_distance = line["axis_distance"]
            direction_sign = line["direction_sign"]

            aligned_start = -axis_distance * direction_sign
            aligned_end = (length - axis_distance) * direction_sign

            minimum_distance = min(aligned_start, aligned_end)
            maximum_distance = max(aligned_start, aligned_end)

            first_step = int(np.ceil(minimum_distance / space))
            last_step = int(np.floor(maximum_distance / space))

            aligned_distances = np.arange(first_step, last_step + 1) * space
            distances = axis_distance + aligned_distances * direction_sign

        elif length < space:
            distances = np.array([length / 2])

        elif include_endpoints:
            distances = np.arange(0, length, space)
            distances = np.append(distances, length)

        else:
            distances = np.arange(space, length, space)

        for position, distance in enumerate(distances):
            point_data = {
                "point_id": len(points),
                "code": line[code_column],
                "distance": distance,
                "geometry": line_interpolate_point(line.geometry, distance),
            }

            if "point_id" in lines.columns:
                point_data["profile_id"] = line["point_id"]

            if align_to_axis:
                point_data["axis_distance"] = axis_distance
                point_data["direction_sign"] = direction_sign
                point_data["distance_aligned"] = aligned_distances[position]
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
    waterdeel_gdf: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """
    Construct a perpendicular profile line for each sampled point along a greppel or channel geometry. The
    function groups points by code, retrieves the matching source line, and computes a local offset at a short
    distance ahead of the original point to determine the profile direction. Each line is then transformed into
    a cross-section with the requested width and stored in a GeoDataFrame with the relevant metadata. This is
    the geometric bridge between sampled points and later DEM-based elevation extraction, allowing repeated
    profile extraction without forcing extra manual geometry operations.
    """

    profiles: list[dict[str, Any]] = []
    codes = points_gdf.groupby("code")
    for code, group in codes:
        line = greppels.loc[greppels["CODE"] == code, "geometry"].iloc[0]
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

    return gpd.GeoDataFrame(
        profiles,
        geometry="geometry",
        crs=greppels.crs,
    )


def sample_elevation_per_profile_point(
    width: float,
    points_gdf: gpd.GeoDataFrame,
    greppels_gdf: gpd.GeoDataFrame,
    dem_path: Path,
    code_column: str,
    waterdeel_gdf: gpd.GeoDataFrame,
    space: float,
    buffer_waterdeel: float,
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """
    Sample DEM elevations along each perpendicular profile line and return both the extracted points and the
    cleaned profile geometries. The function builds profile lines, filters them to the relevant waterbody area,
    aligns them to the original greppel axis, and then samples the DEM at each generated point. It removes invalid
    points and associated profiles before returning the two GeoDataFrames, which are later used for reference
    elevation and cross-section calculations. This workflow keeps the geometry and elevation data synchronized and
    prevents profile reconstruction from being distorted by invalid or clipped features.
    """

    coords: list[tuple[float, float]] = []
    elevations: list[float] = []

    profile_lines = draw_perpendicular_lines(
        width=width,
        points_gdf=points_gdf,
        greppels=greppels_gdf,
        waterdeel_gdf=waterdeel_gdf,
    )
    profile_lines = gpd.sjoin(profile_lines, waterdeel_gdf[["geometry"]], how="inner", predicate="intersects")

    profile_lines = profile_lines.drop(columns="index_right")
    profile_lines = profile_lines.drop_duplicates(subset="point_id")

    profile_lines = align_profile_lines(
        profile_lines_gdf=profile_lines,
        points_gdf=points_gdf,
        greppels_gdf=greppels_gdf,
    )

    profile_points_gdf = points_along_lines(
        profile_lines,
        space=space,
        code_column=code_column,
        align_to_axis=True,
    )

    waterdeel_buffer = waterdeel_gdf.geometry.buffer(buffer_waterdeel).union_all()

    profile_points_gdf = profile_points_gdf.loc[profile_points_gdf.geometry.intersects(waterdeel_buffer)].copy()
    profile_lines = gpd.clip(
        profile_lines,
        waterdeel_buffer,
        keep_geom_type=True,
    )

    for point in profile_points_gdf.geometry:
        point_x = point.x
        point_y = point.y
        coords.append((point_x, point_y))

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
    Plot the extracted elevations for one greppel code and save them as a profile image for review. The function
    reads the geometry data from a GeoPackage, filters the rows for the selected code, and raises a clear ValueError
    if no profiles are present. It then draws all sampled cross-sections together, overlays the mean profile, and
    marks the bank level per channel when sufficient data exists. The result is a clean, publication-friendly plot
    with the axis at zero distance, making it easier to validate profile alignment and bank extraction before updating
    the model.
    """
    matplotlib.use("Agg")

    points = gpd.read_file(gpkg_path)

    greppel_profiles = points.loc[points["code"] == code]

    if greppel_profiles.empty:
        raise ValueError(f"No profiles found for greppel {code}.")

    fig, ax = plt.subplots(figsize=(10, 5))

    for profile_id, profile in greppel_profiles.groupby("profile_id"):
        profile = profile.sort_values("distance")

        ax.plot(
            profile["distance"],
            profile["elevation"],
            color="gray",
            alpha=0.4,
            linewidth=1,
        )
    for channel_id, channel in greppel_profiles.groupby("channel_id"):
        mean_profile = channel[["distance", "mean_elevation"]].drop_duplicates().sort_values("distance")

        (mean_line,) = ax.plot(
            mean_profile["distance"],
            mean_profile["mean_elevation"],
            linewidth=2.5,
            label=f"Mean Profile - Channel {channel_id}",
        )

        bank = channel[["bank_distance", "bank_level"]].dropna().drop_duplicates()

        if not bank.empty:
            bank_distance = bank["bank_distance"].iloc[0]
            bank_level = bank["bank_level"].iloc[0]

            ax.scatter(
                bank_distance,
                bank_level,
                color=mean_line.get_color(),
                edgecolors="black",
                s=80,
                zorder=5,
                label=f"Bank Level - Channel {channel_id}: {bank_level:.3f} m",
            )

    ax.axvline(
        0,
        color="black",
        linestyle="--",
        linewidth=1,
        label="Greppel Axis",
    )

    ax.set_xlabel("Distance along cross-section (m)")
    ax.set_ylabel("Elevation (m NAP)")
    ax.set_title(f"Cross-section profiles — {code}")
    ax.grid(True)
    ax.legend()

    fig.tight_layout()
    fig.savefig(output_path, dpi=150)
    plt.close(fig)

    print(f"Figure saved: {output_path}", flush=True)


def validate_profile(
    new_profile_points: gpd.GeoDataFrame,
    profile_lines_gdf: gpd.GeoDataFrame,
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """
    Remove invalid profile groups that do not contain enough vertical range to support a meaningful cross-section
    table. The function groups points by channel and profile, computes each profile's relative height from its minimum
    elevation, and rejects groups whose endpoint height range collapses to zero. This safeguards subsequent interpolation
    and table generation, since profiles with insufficient contrast cannot provide a reliable width-depth relationship.
    In practice, the returned GeoDataFrame contains the cleaned point set and a reduced set of invalid profile lines
    so they can be logged or inspected separately.
    """

    valid_points = new_profile_points.copy()
    invalid_profiles: list[dict[str, Any]] = []

    groups = valid_points.groupby(["channel_id", "profile_id"])
    for key in groups.groups.keys():
        channel_id, profile_id = key
        profile = groups.get_group(key).copy()
        profile = profile.sort_values("distance")

        reference_level = profile["elevation"].min()
        profile["height"] = profile["elevation"] - reference_level
        max_height = min(
            profile["height"].iloc[0],
            profile["height"].iloc[-1],
        )

        if max_height == 0:
            invalid_profiles.append(
                {
                    "channel_id": channel_id,
                    "profile_id": profile_id,
                }
            )

            print(
                f"Warning: Channel {channel_id}, profile {profile_id}: "
                "insuficient vertical range to build the cross section table"
                "Check alignment of Hydroobject and Waterdeel with the DEM"
            )

            selection = (valid_points["channel_id"] == channel_id) & (valid_points["profile_id"] == profile_id)
            valid_points = valid_points.loc[~selection].copy()

    invalid_profiles_df = pd.DataFrame(invalid_profiles, columns=["channel_id", "profile_id"])

    if invalid_profiles_df.empty:
        invalid_profiles_gdf = profile_lines_gdf.iloc[:0].copy()
    else:
        invalid_profiles_gdf = profile_lines_gdf.merge(
            invalid_profiles_df, left_on="point_id", right_on="profile_id", how="inner"
        )
    return valid_points, invalid_profiles_gdf


def align_profile_lines(
    profile_lines_gdf: gpd.GeoDataFrame,
    points_gdf: gpd.GeoDataFrame,
    greppels_gdf: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """
    Align each clipped profile line with the original greppel geometry and determine the local orientation sign.
    The function projects the sampled origin point onto the current profile line to get its axis distance, then
    compares the local direction of the profile against the greppel direction to decide whether distances increase
    to the left or right. When the cross product is zero, the profile orientation cannot be determined safely and a
    ValueError is raised. This step is crucial because the subsequent profile sampling and distance calculations
    depend on a consistent relative axis direction for each channel segment.
    """
    profile_lines_gdf = profile_lines_gdf.copy()

    for index, profile_line in profile_lines_gdf.iterrows():
        origin_point = points_gdf.loc[
            points_gdf["point_id"] == profile_line["point_id"],
            "geometry",
        ].iloc[0]

        axis_distance = profile_line.geometry.project(origin_point)

        greppels_line = greppels_gdf.loc[
            greppels_gdf["CODE"] == profile_line["code"],
            "geometry",
        ].iloc[0]

        greppel_distance = profile_line["distance"]

        before = greppels_line.interpolate(max(greppel_distance - 0.5, 0))
        after = greppels_line.interpolate(min(greppel_distance + 0.5, greppels_line.length))

        greppel_dx = after.x - before.x
        greppel_dy = after.y - before.y

        profile_start = profile_line.geometry.coords[0]
        profile_end = profile_line.geometry.coords[-1]

        profile_dx = profile_end[0] - profile_start[0]
        profile_dy = profile_end[1] - profile_start[1]

        cross_product = greppel_dx * profile_dy - greppel_dy * profile_dx

        if cross_product == 0:
            raise ValueError(
                f"Profile {profile_line['point_id']}: cannot determine orientation relative to the channel."
            )

        direction_sign = 1 if cross_product > 0 else -1

        profile_lines_gdf.loc[index, "axis_distance"] = axis_distance
        profile_lines_gdf.loc[index, "direction_sign"] = direction_sign

    return profile_lines_gdf


def get_height_and_reference_level(
    greppels_gdf: gpd.GeoDataFrame,
    channel_gdf: gpd.GeoDataFrame,
    profile_lines_gdf: gpd.GeoDataFrame,
    profile_points_gdf: gpd.GeoDataFrame,
    height_step: float,
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """
    Compute channel-wise elevation summaries, reference levels, and cross-section tables from sampled profile
    points. The function joins profiles to the relevant channel geometry, recalculates aligned distances, removes
    invalid profiles, and aggregates mean elevations for each distance step. It then derives a reference level per
    channel and uses that base to build relative height values and width tables for each profile. This pipeline is
    the analytical core of the profile workflow, turning a collection of sampled cross-sections into comparable
    channel metrics that can update model layers and bank definitions in a consistent way.
    """
    greppels_buffer = greppels_gdf.buffer(1).union_all()
    greppels_buffer_gdf = gpd.GeoDataFrame(
        geometry=[greppels_buffer],
        crs=greppels_gdf.crs,
    )

    channel_join = channel_gdf.sjoin(greppels_buffer_gdf, how="inner", predicate="intersects")
    channel_join = channel_join.drop(columns="index_right")
    channel_join = channel_join.drop_duplicates()
    channel_join = channel_join[["id", "geometry"]].rename(columns={"id": "channel_id"})

    profile_lines_intersect = profile_lines_gdf.sjoin(channel_join, how="inner", predicate="intersects")
    profile_lines_intersect = profile_lines_intersect.drop_duplicates()

    profile_lines_id_channel = (
        profile_lines_intersect[["point_id", "channel_id"]]
        .drop_duplicates()
        .rename(columns={"point_id": "profile_id"})
    )

    new_profile_points = profile_points_gdf.merge(
        profile_lines_id_channel, how="inner", on="profile_id", validate="many_to_one"
    )

    new_profile_points["distance_from_start"] = new_profile_points["distance"]
    new_profile_points["distance"] = new_profile_points["distance_aligned"].round(3)

    new_profile_points, invalid_profiles_gdf = validate_profile(new_profile_points, profile_lines_gdf)

    profile_point_mean_elevation = (
        new_profile_points.groupby(
            ["channel_id", "distance"],
            as_index=False,
        )["elevation"]
        .mean()
        .round(3)
    )
    reference_level = profile_point_mean_elevation.groupby(
        ["channel_id"],
        as_index=False,
    )["elevation"].min()

    mean_profiles = profile_point_mean_elevation.merge(
        reference_level.rename(columns={"elevation": "reference_level"}), how="inner", on="channel_id"
    )

    mean_profiles["height"] = mean_profiles["elevation"] - mean_profiles["reference_level"]
    mean_profiles = mean_profiles.rename(columns={"elevation": "mean_elevation"})

    profile_points_with_heights = new_profile_points.merge(
        mean_profiles,
        on=["channel_id", "distance"],
        how="left",
        validate="many_to_one",
    )

    groups = profile_points_with_heights.groupby("channel_id")[["distance", "height"]]
    cross_section_table_list = []
    for channel_id in groups.groups.keys():
        profile = groups.get_group(channel_id).copy()

        cross_section_tables = get_cross_section_table(profile, height_step=height_step)

        if cross_section_tables is None:
            print(f"Warning: Channel f{channel_id}: invalid averaged profile")
            print(profile[["distance", "height"]].drop_duplicates().sort_values("distance").to_string(index=False))
            continue

        cross_section_tables["channel_id"] = channel_id
        cross_section_table_list.append(cross_section_tables)
    cross_section_tables = pd.concat(cross_section_table_list, ignore_index=True)
    profile_points_with_heights = profile_points_with_heights.merge(
        cross_section_tables,
        on="channel_id",
        how="left",
        validate="many_to_one",
    )

    return profile_points_with_heights, invalid_profiles_gdf


def get_cross_section_table(profile: pd.DataFrame, height_step: float = 0.10) -> pd.DataFrame | None:
    """
    Build a width-versus-height table for a profile by slicing the cross-section at regular vertical intervals.
    The function sorts the profile points by distance, computes the lower of the two endpoint heights as the maximum
    cut level, and then integrates the width between neighbouring points for each height step. This yields a compact
    text representation suitable for later use in model cross-section definitions. The logic is designed to handle
    monotonic profile shapes, while a final correction prevents a zero width at the lowest point from propagating into
    the table when the next interval is valid.
    """
    profile = profile[["distance", "height"]]
    profile = profile.drop_duplicates().sort_values("distance")

    distances = profile["distance"].to_numpy()
    profile_heights = profile["height"].to_numpy()

    max_height = min(profile_heights[0], profile_heights[-1])

    heights = np.arange(0, max_height, height_step)
    heights = np.append(heights, max_height)

    cross_section_rows: list[str] = []

    for height in heights:
        width = 0.0

        for index in range(len(distances) - 1):
            x1 = distances[index]
            x2 = distances[index + 1]

            h1 = profile_heights[index]
            h2 = profile_heights[index + 1]

            if h1 <= height and h2 <= height:
                width += x2 - x1
            elif min(h1, h2) < height < max(h1, h2):
                crossing = x1 + ((height - h1) / (h2 - h1)) * (x2 - x1)

                if h1 < height:
                    width += crossing - x1
                else:
                    width += x2 - crossing

        cross_section_rows.append(f"{height:.3f},{width:.3f}")

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
    Estimate the bank level for each channel by examining flattening patterns on both sides of the lowest point
    in the mean profile. The function computes slope changes across the profile, identifies the strongest flattening
    on the left and right, and picks the minimum bank elevation as the representative bank level. This makes it easier
    to determine the natural channel edge before updating the model with new reference levels and cross-section tables.
    The result is the original GeoDataFrame enriched with bank information where these features are detected, enabling
    later comparison against the final channel geometry.
    """
    mean_profiles = (
        profile_points_with_heights[["channel_id", "distance", "mean_elevation"]]
        .drop_duplicates()
        .sort_values(["channel_id", "distance"])
    )

    profile_points_with_heights["bank_level"] = float("nan")

    groups = mean_profiles.groupby("channel_id")

    for channel_id, profile in groups:
        profile = profile.reset_index(drop=True).copy()

        profile["slope"] = profile["mean_elevation"].diff() / profile["distance"].diff()
        slope_abs = profile["slope"].abs()

        profile["flattening_right"] = slope_abs.shift(-1) - slope_abs
        profile["flattening_left"] = slope_abs - slope_abs.shift(-1)

        bottom_distance = profile.loc[profile["mean_elevation"].idxmin(), "distance"]

        right = profile.loc[profile["distance"] < bottom_distance]
        left = profile.loc[profile["distance"] > bottom_distance]

        right_candidates = right.loc[right["flattening_right"] > 0]
        left_candidates = left.loc[left["flattening_left"] > 0]

        bank_levels = []
        bank_distances = []

        if not right_candidates.empty:
            right_index = right_candidates["flattening_right"].idxmax()
            bank_levels.append(profile.loc[right_index, "mean_elevation"])
            bank_distances.append(profile.loc[right_index, "distance"])

        if not left_candidates.empty:
            left_index = left_candidates["flattening_left"].idxmax()
            bank_levels.append(profile.loc[left_index, "mean_elevation"])
            bank_distances.append(profile.loc[left_index, "distance"])

        if not bank_levels:
            print(f"No bank candidates: channel {channel_id}")
            profile_points_with_heights.loc[
                profile_points_with_heights["channel_id"] == channel_id,
                "bank_level",
            ] = float("nan")
            continue

        bank_level = min(bank_levels)
        selected_bank = bank_levels.index(bank_level)
        bank_distance = bank_distances[selected_bank]

        profile_points_with_heights.loc[
            profile_points_with_heights["channel_id"] == channel_id,
            "bank_level",
        ] = round(bank_level, 3)

        profile_points_with_heights.loc[
            profile_points_with_heights["channel_id"] == channel_id,
            "bank_distance",
        ] = bank_distance

    return profile_points_with_heights


def update_cross_sections(
    profile_points_with_heights: gpd.GeoDataFrame,
    cross_section_locations: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """
    Update channel cross-section rows with the calculated reference level, bank level, and cross-section table
    values. The function groups the derived channel summaries by unique channel identifier, applies the new
    attributes to matching rows, and renames the code field to keep the greppel naming scheme consistent. This makes
    the model-ready data structure reflect the reprocessed profile results without losing original location metadata.
    The returned GeoDataFrame is ready to be written back to the source model, where it can be consumed by later update
    routines and quality checks.
    """
    channel_cross_sections_banklelvel = profile_points_with_heights[
        ["channel_id", "reference_level", "bank_level", "cross_section_table"]
    ].drop_duplicates()
    for _, channel in channel_cross_sections_banklelvel.iterrows():
        selection = cross_section_locations["channel_id"] == channel["channel_id"]
        cross_section_locations.loc[selection, "updated"] = True
        cross_section_locations.loc[selection, "reference_level"] = round(channel["reference_level"], 3)
        cross_section_locations.loc[selection, "bank_level"] = channel["bank_level"]
        cross_section_locations.loc[selection, "cross_section_table"] = channel["cross_section_table"]

        cross_section_locations.loc[selection, "code"] = "greppel_" + cross_section_locations.loc[
            selection, "code"
        ].astype(str)

    return cross_section_locations


# %%
def update_model(
    cross_section_locations_updated: gpd.GeoDataFrame,
    channel_gdf: gpd.GeoDataFrame,
    hydroobject: gpd.GeoDataFrame,
    orifice_gdf: gpd.GeoDataFrame,
    connection_node_gdf: gpd.GeoDataFrame,
    model_path: Path,
    cross_section_locations: gpd.GeoDataFrame,
) -> None:
    """Apply updated cross-section properties to the model layers for orifices, connection nodes, and channel locations. The function selects affected channels, updates crest levels for matching greppel orifices, and writes the modified orifice layer back to the GeoPackage. It then adjusts initial water levels for channel connection nodes that are part of the updated set and finalizes the cross-section location attributes with the newly computed reference and bank values. This completes the model update loop by ensuring the stored geometry and metadata remain consistent with the recalculated profile results."""
    channel_id = cross_section_locations_updated.loc[
        cross_section_locations_updated["updated"] == True
    ].channel_id.to_list()

    connection_node_start_id = channel_gdf.loc[channel_gdf["id"].isin(channel_id)].connection_node_start_id.to_list()
    connection_node_end_id = channel_gdf.loc[channel_gdf["id"].isin(channel_id)].connection_node_end_id.to_list()
    connection_nodes_set = set()
    connection_nodes_set.update(connection_node_end_id)
    connection_nodes_set.update(connection_node_start_id)

    channels_updated = channel_gdf.loc[channel_gdf["id"].isin(channel_id)]

    secundary_terciary_buffer = hydroobject.loc[hydroobject["CATEGORIEOPPWATERLICHAAM"] != 1].copy()
    secundary_terciary_buffer["geometry"] = secundary_terciary_buffer.geometry.buffer(1)

    orifice_filter = orifice_gdf.sjoin(secundary_terciary_buffer, how="inner", predicate="within")

    orifice_greppels = orifice_filter.loc[
        orifice_filter["connection_node_start_id"].isin(connection_nodes_set)
        | orifice_filter["connection_node_end_id"].isin(connection_nodes_set)
    ]

    for idx, orifice in orifice_greppels.iterrows():
        orifice_start = orifice.connection_node_start_id
        orifice_end = orifice.connection_node_end_id

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

        target_crest_level = round(max(reference_levels), 3)
        orifice_gdf.loc[idx, "crest_level"] = target_crest_level

    orifice_gdf.to_file(model_path, layer="orifice", driver="GPKG")

    cross_section_updated = cross_section_locations_updated.loc[cross_section_locations_updated["updated"] == True]
    greppel_channel_ids = cross_section_updated["channel_id"].to_list()

    channels_no_greppels = channel_gdf.loc[~channel_gdf["id"].isin(greppel_channel_ids)]

    connection_id_no_greppels = set(channels_no_greppels["connection_node_start_id"])
    connection_id_no_greppels.update(channels_no_greppels["connection_node_end_id"])

    for _, connection_node in cross_section_updated.iterrows():
        channel_id = connection_node.channel_id
        bank_level = connection_node.bank_level

        connection_node_start_id = channel_gdf.loc[
            channel_gdf["id"] == channel_id, "connection_node_start_id"
        ].values.tolist()[0]
        connection_node_end_id = channel_gdf.loc[
            channel_gdf["id"] == channel_id, "connection_node_end_id"
        ].values.tolist()[0]

        if connection_node_start_id not in connection_id_no_greppels:
            connection_node_gdf.loc[
                connection_node_gdf["id"] == connection_node_start_id,
                "initial_waterlevel",
            ] = round(bank_level - 0.10, 2)

        if connection_node_end_id not in connection_id_no_greppels:
            connection_node_gdf.loc[
                connection_node_gdf["id"] == connection_node_end_id,
                "initial_waterlevel",
            ] = round(bank_level - 0.10, 2)

    connection_node_gdf.to_file(model_path, layer="connection_node", driver="GPKG")

    cross_section_ids = cross_section_locations_updated["id"]
    for section_id in cross_section_ids:
        bank_level = cross_section_locations_updated.loc[
            cross_section_locations_updated["id"] == section_id, "bank_level"
        ].values.tolist()[0]
        cross_section_locations.loc[cross_section_locations["id"] == section_id, "bank_level"] = bank_level

        cross_section_table = cross_section_locations_updated.loc[
            cross_section_locations_updated["id"] == section_id, "cross_section_table"
        ].values.tolist()[0]
        cross_section_locations.loc[cross_section_locations["id"] == section_id, "cross_section_table"] = (
            cross_section_table
        )

        reference_level = cross_section_locations_updated.loc[
            cross_section_locations_updated["id"] == section_id, "reference_level"
        ].values.tolist()[0]
        cross_section_locations.loc[cross_section_locations["id"] == section_id, "reference_level"] = reference_level

    cross_section_locations.to_file(model_path, layer="cross_section_location", driver="GPKG")


# %%
if __name__ == "__main__":
    # paths
    folder = Folders(Path(r"H:\02.modellen\grootslag_greppels_clip"))
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
    width = 7  # profile line width (meters)
    space = 0.20  # space between points (meters)
    space_points_along_lines = 10  # space between points along the greppels (meters)
    height_step = 0.1  # meters. Vertical interval between the hights at which cross section widths are calculated
    buffer_waterdeel = 0.75
    # fucntions.
    # draw points along the grepples every points_along_lines distance. OVer those points, perpendicular lines will be drawn
    points_gdf = points_along_lines(
        lines=greppels_gdf,
        space=space_points_along_lines,
        code_column="CODE",
        include_endpoints=False,
        align_to_axis=False,
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
    # built cross section table base on: channel code and point location on the profile lines
    profile_points_with_heights, invalid_profiles_gdf = get_height_and_reference_level(
        greppels_gdf=greppels_gdf,
        channel_gdf=channel_gdf,
        profile_lines_gdf=profile_lines_gdf,
        profile_points_gdf=profile_points_gdf,
        height_step=height_step,
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

    cross_section_locations_updated = update_cross_sections(
        profile_points_with_heights,
        cross_section_locations.copy(),
    )

    update_model(
        cross_section_locations_updated,
        channel_gdf,
        hydroobject,
        orifice_gdf,
        connection_node_gdf,
        model_path,
        cross_section_locations,
    )
# %%
codes = profile_points_with_heights["code"].unique().tolist()

for code in codes:
    save_path = profile_points_folder / f"{code}.png"
    plot_profile(gpkg_path=profile_points_path, code=code, output_path=save_path)
# %%
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
