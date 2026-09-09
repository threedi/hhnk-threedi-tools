# %%
from pathlib import Path

import geopandas as gpd
import pandas as pd
import rasterio
from shapely import line_interpolate_point
from shapely.ops import nearest_points

from hhnk_threedi_tools import Folders


def nearest_intersect(boundary_geom, line_geom, point_geom):
    """
    Find intersection between line and boundary that is closest
    to given point.
    """

    if boundary_geom.intersects(line_geom):
        intersection = nearest_points(point_geom, boundary_geom.intersection(line_geom))[1]
        nearest_dist = intersection.distance(point_geom)
    else:
        intersection = None
        nearest_dist = None

    return intersection, nearest_dist


def nearest_intersect_exclude(boundary_geom, line_geom, point_geom, exclude_point):
    """
    Find intersection between line and boundary that is closest
    to given point, but not the exclude point.
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


from math import atan2, cos, sin

from shapely.geometry import LineString


def perpendicular_lines(p1, p2, distance, side):
    """
    Takes two points and calculates points perpendicular  to first point and draws line between them.

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
    if side == "left":
        perp_coords = [p1[0] + lx, p1[1] - ly]

    perp_line = LineString([p1, perp_coords])

    return perp_line


import numpy as np


def get_nearest_non_nodata(rioxarray_raster, point_gdf, tolerance=0.5, pixelsize=0.5, mode="min"):
    """
    Finds nearest (pixel interval) non-nodata pixel within tolerance.
    Tries at exact location first (within pixelsize), then searches further.
    If multiple pixels at same distance 'mode' desides.

    With high tolerance a lot of overhead as .sel is perfomed on all valid pixels
    whitin each while loop. Could be improved by somehow storing known values in
    dataframe, but for small tolerance this is quicker.

    Output is converted to numpy.float
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


import numpy as np


def get_min_value_in_polygon(rioxarray_raster, polygon, mode="min"):
    """
    Finds non-nodata value within polygon.
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


def full_perpendicular_line(p1, p2, width):
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


def points_along_lines(lines, space=10, code_column="code", include_endpoints=True):
    points = []

    for idx, line in lines.iterrows():
        length = line.geometry.length
        if include_endpoints:
            distances = np.arange(0, length, space)
            distances = np.append(distances, length)
        else:
            distances = np.arange(space, length, space)

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
import geopandas as gpd

# greppels = gpd.read_file(r"H:\02.modellen\NZK_leggertool\01_source_data\greppels_nzk.shp")
# points_gdf = points_along_lines(lines=greppels, space=10)
# %%


def draw_perpendicular_lines(width, points_gdf, greppels):
    profiles = []
    codes = points_gdf.groupby("code")
    for code, group in codes:
        line = greppels.loc[greppels["CODE"] == code, "geometry"].iloc[0]
        # print(code)
        # print(group)
        for index, point in group.iterrows():
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

    profiles_gdf = gpd.GeoDataFrame(
        profiles,
        geometry="geometry",
        crs=greppels.crs,
    )
    return profiles_gdf


def sample_elevation_per_profile_point(width, points_gdf, greppels, dem_path, code_column, waterdeel_gdf):
    space = 0.30
    coords = []
    elevations = []

    profile_lines = draw_perpendicular_lines(width, points_gdf, greppels)
    profile_lines = gpd.sjoin(profile_lines, waterdeel_gdf[["geometry"]], how="inner", predicate="intersects")

    profile_lines = profile_lines.drop(columns="index_right")

    profile_lines = profile_lines.drop_duplicates(subset="point_id")

    profile_points_gdf = points_along_lines(profile_lines, space=space, code_column=code_column)

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


import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt


def plot_profile(gpkg_path, code, output_path):
    import geopandas as gpd
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

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
def get_height_and_reference_level(greppels_gdf, channel_gdf, profile_lines_gdf, profile_points_gdf):
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

    # group by channels and keep columns distance and height
    groups = profile_points_with_heights.groupby("channel_id")[["distance", "height"]]
    # apply function to get cross_section_tables
    cross_section_tables = groups.apply(get_cross_section_table, height_step=0.10)
    cross_section_tables = cross_section_tables.reset_index(drop=True)

    # merge results.
    profile_points_with_heights = profile_points_with_heights.merge(
        cross_section_tables,
        on="channel_id",
        how="left",
        validate="many_to_one",
    )
    return profile_points_with_heights


def get_cross_section_table(
    profile,
    height_step=0.10,
):
    channel_id = profile.name

    profile = profile[["distance", "height"]]
    profile = profile.drop_duplicates().sort_values("distance")

    distances = profile["distance"].to_numpy()
    profile_heights = profile["height"].to_numpy()

    # Stop at the lower endpoint to avoid extending beyond the profile.
    max_height = min(profile_heights[0], profile_heights[-1])

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

    cross_section = pd.DataFrame(
        {
            "channel_id": [channel_id],
            "cross_section_table": ["\n".join(cross_section_rows)],
        }
    )

    return cross_section


def get_bank_level(profile_points_with_heights):
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

        # Positive values indicate flattening toward the outside.
        profile["flattening_right"] = slope_abs.shift(-1) - slope_abs
        profile["flattening_left"] = slope_abs - slope_abs.shift(-1)

        bottom_distance = profile.loc[profile["mean_elevation"].idxmin(), "distance"]

        right = profile.loc[profile["distance"] < bottom_distance]
        left = profile.loc[profile["distance"] > bottom_distance]

        right_candidates = right.loc[right["flattening_right"] > 0]
        left_candidates = left.loc[left["flattening_left"] > 0]
        bank_levels = []

        if not right_candidates.empty:
            right_index = right_candidates["flattening_right"].idxmax()
            bank_levels.append(profile.loc[right_index, "mean_elevation"])

        if not left_candidates.empty:
            left_index = left_candidates["flattening_left"].idxmax()
            bank_levels.append(profile.loc[left_index, "mean_elevation"])

        if not bank_levels:
            print(f"No bank candidates: channel {channel_id}")
            profile_points_with_heights.loc[
                profile_points_with_heights["channel_id"] == channel_id,
                "bank_level",
            ] = float("nan")
            continue

        bank_level = min(bank_levels)

        profile_points_with_heights.loc[
            profile_points_with_heights["channel_id"] == channel_id,
            "bank_level",
        ] = round(bank_level, 3)

    return profile_points_with_heights


def update_cross_sections(profile_points_with_heights, cross_section_locations):
    channel_cross_sections_banklelvel = profile_points_with_heights[
        ["channel_id", "reference_level", "bank_level", "cross_section_table"]
    ].drop_duplicates()
    for index, channel in channel_cross_sections_banklelvel.iterrows():
        selection = cross_section_locations["channel_id"] == channel["channel_id"]
        cross_section_locations.loc[selection, "updated"] = True
        cross_section_locations.loc[selection, "reference_level"] = round(channel["reference_level"], 3)
        cross_section_locations.loc[selection, "bank_level"] = channel["bank_level"]
        cross_section_locations.loc[selection, "cross_section_table"] = channel["cross_section_table"]
    return cross_section_locations


# %%


def update_orifice_connection_nodes(
    cross_section_locations_updated, channel_gdf, hydroobject, orifice_gdf, connection_node_gdf
):
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
    orifice_greppels = orifice_filter.loc[orifice_filter["connection_node_end_id"].isin(list(connection_nodes_set))]
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
        target_crest_level = round(min(reference_levels) + 0.10, 3)

        if orifice["crest_level"] > target_crest_level:
            orifice_gdf.loc[idx, "crest_level"] = target_crest_level
        else:
            continue
    orifice_gdf.to_file(model_path, layer="orifice", driver="GPKG")

    cross_section_updated = cross_section_locations_updated.loc[cross_section_locations_updated["updated"] == True]
    for idx, connection_node in cross_section_updated.iterrows():
        channel_id = connection_node.channel_id
        bank_level = connection_node.bank_level

        connection_node_start_id = channel_gdf.loc[
            channel_gdf["id"] == channel_id, "connection_node_start_id"
        ].values.tolist()[0]
        connection_node_end_id = channel_gdf.loc[
            channel_gdf["id"] == channel_id, "connection_node_end_id"
        ].values.tolist()[0]

        connection_node_gdf.loc[connection_node_gdf["id"] == connection_node_start_id, "initial_water_level"] = (
            bank_level - 0.10
        )
        connection_node_gdf.loc[connection_node_gdf["id"] == connection_node_end_id, "initial_water_level"] = (
            bank_level - 0.10
        )

        # print(channel_id, bank_level)
    connection_node_gdf.to_file(model_path, layer="connection_node", driver="GPKG")


# %%
# result.to_file(
#     r"H:\02.modellen\grootslag_leggertool\new_cross_section_points_function_v2.gpkg",
#     driver="GPKG",
# )
# path
model = Path(r"H:\02.modellen\grootslag_leggertool\02_schematisation\greppels")
model_path = model / "bwn_grootslag.gpkg"
folder = Folders(Path(r"H:\02.modellen\grootslag_leggertool"))
dem_path = (model) / "rasters" / "dem_grootslag.tif"
greppels = r"H:\02.modellen\grootslag_leggertool\01_source_data\greppels_from_geoweb_wss_clipped.gpkg"

damo_path = r"H:\02.modellen\grootslag_leggertool\01_source_data\version_1_DCMB\DAMO.gpkg"
# read geodataframes
hydroobject = gpd.read_file(damo_path, layer="HydroObject")
waterdeel_gdf = gpd.read_file(r"H:\02.modellen\grootslag_leggertool\01_source_data\DAMO_waterdeel_backup.gpkg")
greppels_gdf = gpd.read_file(greppels)
channel_gdf = gpd.read_file(model_path, layer="channel")
cross_section_locations = gpd.read_file(model_path, layer="cross_section_location")
orifice_gdf = gpd.read_file(model_path, layer="orifice")
culver_gdf = gpd.read_file(model_path, layer="culvert")
connection_node_gdf = gpd.read_file(model_path, layer="connection_node")
# draw points along  greppels
points_gdf = points_along_lines(lines=greppels_gdf, space=10, code_column="CODE", include_endpoints=False)

width = 5
profile_points_gdf, profile_lines_gdf = sample_elevation_per_profile_point(
    width, points_gdf, greppels_gdf, dem_path, code_column="code", waterdeel_gdf=waterdeel_gdf
)

# %%
profile_points_with_heights = get_height_and_reference_level(
    greppels_gdf=greppels_gdf,
    channel_gdf=channel_gdf,
    profile_lines_gdf=profile_lines_gdf,
    profile_points_gdf=profile_points_gdf,
)
# %%
cross_section_banklevels = get_bank_level(profile_points_with_heights)

cross_section_locations_updated = update_cross_sections(
    profile_points_with_heights,
    cross_section_locations.copy(),
)
# %%
profile_points_with_heights.to_file(
    r"H:\02.modellen\grootslag_leggertool\cross_section_points_with_heights.gpkg",
    driver="GPKG",
)
profile_points_gdf.to_file(
    r"H:\02.modellen\grootslag_leggertool\cross_section_points_function.gpkg",
    driver="GPKG",
)
profile_lines_gdf.to_file(
    r"H:\02.modellen\grootslag_leggertool\cross_section_lines_function.gpkg",
    driver="GPKG",
)

# %%
cross_section_banklevels.to_file(
    r"H:\02.modellen\grootslag_leggertool\cross_section_lines_banklevel.gpkg",
    driver="GPKG",
)

cross_section_locations_updated.to_file(
    r"H:\02.modellen\grootslag_leggertool\cross_section_updated.gpkg",
    driver="GPKG",
)
# %%
