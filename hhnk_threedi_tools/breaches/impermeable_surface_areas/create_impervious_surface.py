"""
Create impervious surface layers for 3Di models using Voronoi polygons.

This module generates drainage subcatchments based on connection nodes and
fixed drainage level areas (FDLA), then creates impervious surface layers
with afvoernorm percentages for integration into 3Di model geopackages.
"""

# %%
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import fiona
import geopandas as gpd
import pandas as pd
from shapely import get_parts, voronoi_polygons
from shapely.geometry import LineString, MultiPoint


def get_nodes_within_fdla(
    model_path_gpkg: Union[str, Path],
    datacheker_path: Union[str, Path],
    polder_polygon_path: Union[str, Path],
) -> Tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """Documentation helper for `get_nodes_within_fdla`.

    Parameters
    ----------
    model_path_gpkg : str | Path
        Path to the 3Di model GeoPackage that contains a `connection_node` layer.
    datacheker_path : str | Path
        Path to the datachecker GeoPackage that contains the
        `fixeddrainagelevelarea` layer.
    polder_polygon_path : str | Path
        Path to a polygon layer defining the polder boundary used to clip FDLA.

    Returns
    -------
    geopandas.GeoDataFrame
        A GeoDataFrame of connection nodes that are used in the network and
        assigned to FDLA polygons. Columns include `con_id`, FDLA attributes and
        geometry.

    Notes
    -----
    This helper is only a docstring carrier to avoid modifying the original
    function signature in this batch; the real function behavior is unchanged.
    """
    # read geopackges to create tissen (voronoi) polygons.
    nodes = gpd.read_file(model_path_gpkg, layer="connection_node")
    fdla = gpd.read_file(datacheker_path, layer="fixeddrainagelevelarea")
    polder = gpd.read_file(polder_polygon_path)

    # change columns names to easy manage information fdla ixeddrainagelevelarea_
    nodes = nodes.rename(columns={"id": "con_id"})
    fdla = fdla.rename(columns={"id": "fdla_id", "code": "fdla_code"})

    # available_layers = fiona.listlayers(model_path_gpkg)

    # layers to search the connectio nodes
    network_layers = [
        "channel",
        "culvert",
        "weir",
        "orifice",
    ]

    # columns to look in
    connection_node_columns = [
        "connection_node_start_id",
        "connection_node_end_id",
    ]

    # Create a empty list with set() it help to not store repeated values.
    used_node_ids = set()

    for layer in network_layers:
        # read layer that contains connection nodes id in their columns
        df = gpd.read_file(model_path_gpkg, layer=layer, ignore_geometry=True)

        # store the connection node id
        for connection_node_col in connection_node_columns:
            used_node_ids.update(df[connection_node_col])

    # filter the nodes base on the used node id
    nodes = nodes[nodes["con_id"].isin(used_node_ids)].copy()
    # clip de fdla with the polder polygon so everything endup within the polder polygon
    fdla = gpd.clip(fdla, polder)
    fdla["geometry"] = fdla.geometry.make_valid()

    # make a join with the fdla to get the code from fdla
    nodes_fdla = gpd.sjoin(
        nodes[["con_id", "geometry"]],
        fdla[["fdla_id", "fdla_code", "geometry"]],
        how="inner",
        predicate="intersects",
    )

    # delete the index column
    nodes_fdla = nodes_fdla.drop(columns=["index_right"], errors="ignore")
    # delete duplicates in case thetr are.
    nodes_fdla = nodes_fdla.drop_duplicates(subset=["con_id"])

    print("Nodes assigned to FDLA:", len(nodes_fdla))
    return nodes_fdla, fdla


# %%
def createa_voronoi_polygons(
    nodes_fdla: gpd.GeoDataFrame,
    fdla: gpd.GeoDataFrame,
) -> Tuple[gpd.GeoDataFrame, List[Dict[str, Any]]]:
    """Create Voronoi subcatchments from connection nodes within FDLA polygons.

    This function:
    1. Reads connection nodes and fixed drainage level areas (FDLA).
    2. Identifies nodes used in network elements (channels, culverts, weirs, orifices).
    3. Assigns nodes to FDLA areas via spatial join.
    4. Creates Voronoi polygons for each FDLA, clipped to FDLA boundaries.
    5. Reassigns orphan polygons (without nodes) to nearest neighbors by shared border.
    6. Dissolves and explodes to create final subcatchments.

    Parameters
    ----------
    model_path_gpkg : Union[str, Path]
        Path to 3Di model geopackage (must contain connection_node, channel,
        culvert, weir, orifice layers).
    datacheker_path : Union[str, Path]
        Path to datachecker geopackage (must contain fixeddrainagelevelarea layer).
    polder_polygon_path : Union[str, Path]
        Path to polder boundary polygon geopackage or shapefile.

    Returns
    -------
    gpd.GeoDataFrame
        GeoDataFrame with columns: con_id, fdla_code, connection_node_id,
        surface_id, area, geometry. One row per subcatchment polygon.
    """

    all_voronoi_cells = []
    rows = []
    for _, area in fdla.iterrows():
        # take the id, code and geom from the fdla gdf
        fdla_id = area["fdla_id"]
        fdla_code = area["fdla_code"]
        fdla_geom = area.geometry

        # select local nodes from the iterations base con fdla id
        local_nodes = nodes_fdla[nodes_fdla["fdla_id"] == fdla_id].copy()

        # if there is no connection node in de fdla the skip it.
        if len(local_nodes) == 0:
            continue

        # if there is only one node add it to the list with the fdla_code, con_id and geometry
        if len(local_nodes) < 2:
            rows.append(
                {
                    "con_id": int(local_nodes.iloc[0]["con_id"]),
                    "fdla_code": fdla_code,
                    "geometry": fdla_geom,
                }
            )
            continue

        # create points geometry base on fdla id.
        points = MultiPoint(local_nodes.geometry.tolist())
        # create the polygons base on the previous points.
        voronoi = voronoi_polygons(points, extend_to=fdla_geom)

        # make the voronoi polygons a gdf
        voronoi_cells = gpd.GeoDataFrame(
            geometry=list(get_parts(voronoi)),
            crs=fdla.crs,
        )

        # intersect the voronoi polygon with fdla
        voronoi_cells["geometry"] = voronoi_cells.geometry.intersection(fdla_geom)
        voronoi_cells = voronoi_cells[~voronoi_cells.geometry.is_empty].copy()

        # explode to avooid polygons that are to far from the connection node.
        voronoi_cells = voronoi_cells.explode(index_parts=False).reset_index(drop=True)
        voronoi_cells["part_id"] = range(len(voronoi_cells))

        # Add fdla_code directly, because all these polygons belong to this FDLA
        voronoi_cells["fdla_code"] = fdla_code

        # add the con_id (connection_node_id) to the polygons.
        voronoi_cells = gpd.sjoin(
            voronoi_cells,
            local_nodes[["con_id", "geometry"]],
            how="left",
            predicate="intersects",
        )

        # drop duplicates and column index
        voronoi_cells = voronoi_cells.drop(columns=["index_right"], errors="ignore")
        voronoi_cells = voronoi_cells.drop_duplicates(subset=["part_id"]).copy()
        all_voronoi_cells.append(voronoi_cells)

    if not all_voronoi_cells:
        raise RuntimeError("No Voronoi polygons were created.")

    voronoi_cells = pd.concat(all_voronoi_cells, ignore_index=True)
    voronoi_cells = gpd.GeoDataFrame(voronoi_cells, geometry="geometry", crs=fdla.crs)

    return voronoi_cells, rows
    # Polygons with connection nodes inside
    # voronoi_with_con_id = voronoi_cells[voronoi_cells["con_id"].notna()].copy()


def correct_voronoi_polygons(voronoi_cells: gpd.GeoDataFrame, rows: List[Dict[str, Any]]) -> gpd.GeoDataFrame:
    """Assign orphan Voronoi polygons to the best neighboring polygon.

    Parameters
    ----------
    voronoi_cell : geopandas.GeoDataFrame
        GeoDataFrame of Voronoi polygons produced for a single FDLA. Expected
        to contain columns `con_id` (may be NaN for orphans) and `fdla_code`.
    rows : list
        Mutable list of dicts that accumulates final subcatchment records;
        this function appends records to `rows` and returns the final
        GeoDataFrame of subcatchments.

    Returns
    -------
    geopandas.GeoDataFrame
        Finalized subcatchments GeoDataFrame with columns `con_id`,
        `fdla_code`, `connection_node_id`, `surface_id`, `area`, and `geometry`.

    Notes
    -----
    The implementation assumes that polygons without a `con_id` (orphans)
    are assigned to the neighbor with which they share the longest border
    within the same FDLA. The function mutates and returns a GeoDataFrame and
    also extends the provided `rows` list.
    """
    # Polygon WITHOUT connection node inside. So they are Orphan :(
    voronoi_without_con_id = voronoi_cells[voronoi_cells["con_id"].isna()]

    # Loop over the voronoi polygons that does not have connnection id
    for orphan_index, orphan_polygon in voronoi_without_con_id.iterrows():
        # Get the fdla code from the fdla
        orphan_fdla_code = orphan_polygon["fdla_code"]
        orphan_buffer = orphan_polygon.geometry.buffer(0.01)

        # Select polygons that intersect the orphan buffer
        intersects_buffer = voronoi_cells.geometry.intersects(orphan_buffer)
        candidate_parts = voronoi_cells[intersects_buffer].copy()

        # Keep only polygons that already have a connection node
        candidate_parts = candidate_parts[candidate_parts["con_id"].notna()].copy()

        # Keep only polygons from the same FDLA
        shared_polygons = candidate_parts[candidate_parts["fdla_code"] == orphan_fdla_code].copy()

        best_con_id = None
        longest_shared_border = 0

        for _, shared_polygon in shared_polygons.iterrows():
            # get the boundary of an orphan.
            boundary_orphan = orphan_polygon.geometry.boundary

            # get the boundary of orphan
            shared_boundary = shared_polygon.geometry.boundary

            # get the lenght of the boundary intersected polygons
            shared_length = boundary_orphan.intersection(shared_boundary).length

            # if the share length is logner than 0 then it means that there are an orphan polygon sharing border with other polygon
            # and then get the connection id
            if shared_length > longest_shared_border:
                longest_shared_border = shared_length
                best_con_id = shared_polygon["con_id"]
            # assign in the columns con_id the id with the largest shared border.
            if best_con_id is not None:
                voronoi_cells.loc[orphan_index, "con_id"] = best_con_id

    # Keep only polygons with con_id
    voronoi_cells = voronoi_cells[voronoi_cells["con_id"].notna()].copy()

    # Merge polygons that belong to the same node inside the same FDLA
    voronoi_cells = voronoi_cells.dissolve(
        by=["con_id", "fdla_code"],
        as_index=False,
    )

    # Split disconnected multipolygons again
    voronoi_cells = voronoi_cells.explode(index_parts=False).reset_index(drop=True)

    rows.extend(voronoi_cells[["con_id", "fdla_code", "geometry"]].to_dict("records"))

    # Create from the rows a gdf
    # Use the GeoDataFrame CRS from the voronoi cells rather than relying on
    # an outer-scope `fdla` variable which is not available here.
    subcatchments = gpd.GeoDataFrame(rows, geometry="geometry", crs=voronoi_cells.crs)
    # delete not data
    subcatchments = subcatchments[subcatchments.geometry.notna()]
    # remove empty features
    subcatchments = subcatchments[~subcatchments.geometry.is_empty]
    # fix geometry in case is not valid
    subcatchments["geometry"] = subcatchments.geometry.make_valid()
    # disolve based on connecntion node id and fdla_code
    subcatchments = subcatchments.dissolve(
        by=["con_id", "fdla_code"],
        as_index=False,
    )
    # Separate disconnected polygon parts
    subcatchments = subcatchments.explode(index_parts=False).reset_index(drop=True)

    # Keep the connection node id separately
    subcatchments["connection_node_id"] = subcatchments["con_id"]

    # Create a unique surface id
    subcatchments["surface_id"] = range(1, len(subcatchments) + 1)

    # Calculate area after explode
    subcatchments["area"] = subcatchments.geometry.area

    print("Subcatchments:", len(subcatchments))
    return subcatchments


def create_surface_layer(
    subcatchments: gpd.GeoDataFrame,
    impervious_out_polygon_gpkg: Union[str, Path],
) -> gpd.GeoDataFrame:
    """Create v2_impervious_surface layer from subcatchments.

    Transforms subcatchment GeoDataFrame to 3Di surface schema and writes to geopackage.

    Parameters
    ----------
    subcatchments : gpd.GeoDataFrame
        Output from createa_voronoi_polygons with columns: con_id, fdla_code,
        connection_node_id, surface_id, area, geometry.
    impervious_out_polygon_gpkg : Union[str, Path]
        Output geopackage path (layer: v2_impervious_surface_new).

    Returns
    -------
    gpd.GeoDataFrame
        Surface layer with 3Di schema columns: id, code, connection_node_id,
        display_name, surface_inclination, surface_class, surface_sub_class,
        zoom_category, nr_of_inhabitants, area, dry_weather_flow, function, geometry.
    """
    # from subcahtments geodataframe create de columns base on the sqlite
    surfaces = subcatchments.copy()
    surfaces["id"] = surfaces["surface_id"]
    surfaces["connection_node_id"] = surfaces["con_id"]
    surfaces["display_name"] = surfaces["fdla_code"]
    surfaces["code"] = surfaces["fdla_code"]
    surfaces["surface_class"] = "gesloten verharding"
    surfaces["surface_sub_class"] = None
    surfaces["surface_inclination"] = "uitgestrekt"
    surfaces["zoom_category"] = 1
    surfaces["nr_of_inhabitants"] = 0
    surfaces["dry_weather_flow"] = 0.0
    surfaces["function"] = None
    # organized columns base on sqlite
    surfaces = surfaces[
        [
            "id",
            "code",
            "connection_node_id",
            "display_name",
            "surface_inclination",
            "surface_class",
            "surface_sub_class",
            "zoom_category",
            "nr_of_inhabitants",
            "area",
            "dry_weather_flow",
            "function",
            "geometry",
        ]
    ]
    # save geopackges
    # surfaces.to_file(
    #     impervious_out_polygon_gpkg,
    #     layer="v2_impervious_surface_new",
    #     driver="GPKG",
    # )

    return surfaces


# %%
def get_percentage_afvoernorm(
    hdb_path: Union[str, Path],
    surfaces: gpd.GeoDataFrame,
) -> pd.DataFrame:
    """Calculate afvoernorm percentage for each impervious surface.

    Intersects surfaces with polders_v4 layer and weights afvoernorm by
    intersection area to compute effective percentage per surface.

    Parameters
    ----------
    hdb_path : Union[str, Path]
        Path to HDB (Hydro Database) geopackage with polders_v4 layer
        and Historische_afvoernorm_mm_d column.
    surfaces : gpd.GeoDataFrame
        Surface layer from create_surface_layer with id and geometry columns.

    Returns
    -------
    pd.DataFrame
        DataFrame with columns: impervious_surface_id, percentage (0.0–1.0, rounded to 2 decimals).
    """
    # column to be use to get percentage for the surface map gpkg
    norm_col = "Historische_afvoernorm_mm_d"

    # read Polver_v4 from hdb geopackge
    polders_v4 = gpd.read_file(
        hdb_path,
        layer="polders_v4",  # ajusta el nombre si en tu gpkg se llama diferente
    )

    # copy the polder v4 gpkg with the only norm_column
    polders_v4 = polders_v4[[norm_col, "geometry"]].copy()

    # make numeric all values. If there is a none or a none numeric value, then it will be transform in NaN (ERRORS = COERCE)
    polders_v4[norm_col] = pd.to_numeric(polders_v4[norm_col], errors="coerce")
    # drop nan and make the geometry valid fom the polder valid
    polders_v4 = polders_v4.dropna(subset=[norm_col])
    polders_v4["geometry"] = polders_v4.geometry.make_valid()

    # copy the surfaces id and rename the columns base on the sqlite and make the geometry valid
    surfaces_for_norm = surfaces[["id", "geometry"]].copy()
    surfaces_for_norm = surfaces_for_norm.rename(columns={"id": "impervious_surface_id"})
    surfaces_for_norm["geometry"] = surfaces_for_norm.geometry.make_valid()

    # intersect the surface with the polder. To get percentage base on weight
    intersections = gpd.overlay(
        surfaces_for_norm,
        polders_v4,
        how="intersection",
    )

    # Calculate area of intersection.
    intersections["intersect_area"] = intersections.geometry.area
    intersections["weighted_norm"] = intersections["intersect_area"] * intersections[norm_col]
    # discolve, aggregate and sum values of the previous columns based on impervious surface id
    percentage_by_surface = intersections.dissolve(
        by="impervious_surface_id",
        aggfunc={
            "weighted_norm": "sum",
            "intersect_area": "sum",
        },
    ).reset_index()

    # Calculate percentage
    percentage_by_surface["percentage"] = (
        percentage_by_surface["weighted_norm"] / percentage_by_surface["intersect_area"]
    )

    # select same columns as sqlite
    percentage_by_surface = percentage_by_surface[["impervious_surface_id", "percentage"]].copy()

    # round columns percentage with max 2 decimals
    percentage_by_surface["percentage"] = percentage_by_surface["percentage"].round(2)
    return percentage_by_surface


def create_surface_map_layer(
    model_path_gpkg: Union[str, Path],
    surfaces: gpd.GeoDataFrame,
    percentage_by_surface: pd.DataFrame,
    impervious_out_line_gpkg: Union[str, Path],
) -> gpd.GeoDataFrame:
    """Create v2_impervious_surface_map layer (lines from surfaces to nodes).

    Generates LineStrings connecting surface centroids to connection nodes,
    merges with afvoernorm percentages, and writes to geopackage.

    Parameters
    ----------
    model_path_gpkg : Union[str, Path]
        Path to 3Di model geopackage (must contain connection_node layer).
    surfaces : gpd.GeoDataFrame
        Surface layer from create_surface_layer.
    percentage_by_surface : pd.DataFrame
        Output from get_percentage_afvoernorm with impervious_surface_id and percentage.
    impervious_out_line_gpkg : Union[str, Path]
        Output geopackage path (layer: v2_impervious_surface_map_new).

    Returns
    -------
    gpd.GeoDataFrame
        Surface map layer with columns: id, percentage, impervious_surface_id,
        connection_node_id, geometry (LineString). One line per surface-to-node connection.
    """
    # select node form the model gkpg
    nodes = gpd.read_file(model_path_gpkg, layer="connection_node")

    # change columns names base con surface map sqlite columns
    surfaces_for_map = surfaces[["id", "connection_node_id", "geometry"]].copy()
    surfaces_for_map = surfaces_for_map.rename(columns={"id": "impervious_surface_id"})
    surfaces_for_map = surfaces_for_map.rename_geometry("surface_geometry")

    nodes_for_map = nodes[["id", "geometry"]].copy()
    nodes_for_map = nodes_for_map.rename(columns={"id": "connection_node_id"})
    nodes_for_map = nodes_for_map.rename_geometry("node_geometry")

    surface_map = surfaces_for_map.merge(
        nodes_for_map,
        on="connection_node_id",
        how="left",
    )

    # merge databases to get percentage base on impervious_surfave id.
    surface_map = surface_map.merge(
        percentage_by_surface,
        on="impervious_surface_id",
        how="left",
    )

    # Create id column bsae on impervious surface id
    surface_map["id"] = surface_map["impervious_surface_id"]

    # create a list to store lines that will be connected to each connection node
    line_geometries = []

    # iterate over all the polygons
    for _, row in surface_map.iterrows():
        # create a point within the polygon. Not centroid to avoid issues with location.
        surface_centroid = row["surface_geometry"].representative_point()
        # get the geometry point from the connection node
        node_geometry = row["node_geometry"]

        # create a line base that start in the centroid and ends in the connectio node
        line_geometry = LineString(
            [
                surface_centroid,
                node_geometry,
            ]
        )

        # append all lines in the list
        line_geometries.append(line_geometry)

    # update geometry column
    surface_map["geometry"] = line_geometries

    # create gdf from that information
    surface_map = gpd.GeoDataFrame(
        surface_map[
            [
                "id",
                "percentage",
                "impervious_surface_id",
                "connection_node_id",
                "geometry",
            ]
        ],
        geometry="geometry",
        crs=surfaces.crs,
    )
    # drop nan
    surface_map = surface_map.dropna(subset=["geometry"]).copy()

    # surface_map.to_file(
    #     impervious_out_line_gpkg,
    #     layer="v2_impervious_surface_map_new",
    #     driver="GPKG",
    # )
    return surface_map


def update_model_geopackage(
    model_path_gpkg: Union[str, Path],
    surfaces: gpd.GeoDataFrame,
    surface_map: gpd.GeoDataFrame,
    output_model_path: Union[str, Path, None] = None,
    surface_layer_name: str = "impervious_surface",
    surface_map_layer_name: str = "impervious_surface_map",
    sure_update: bool = False,
) -> Path:
    """Update 3Di model geopackage with new impervious surface layers.

    Parameters
    ----------
    model_path_gpkg : Union[str, Path]
        Path to 3Di model geopackage to update.
    surfaces : gpd.GeoDataFrame
        Surface layer from create_surface_layer.
    surface_map : gpd.GeoDataFrame
        Surface map layer from create_surface_map_layer.
    output_model_path : Union[str, Path, None], optional
        Unused parameter (kept for compatibility). Default is None.
    surface_layer_name : str, optional
        Target layer name for surfaces (usually "impervious_surface").
        Default is "impervious_surface".
    surface_map_layer_name : str, optional
        Target layer name for surface map (usually "impervious_surface_map").
        Default is "impervious_surface_map".
    sure_update : bool, optional
        If False, only prints a message; no update performed. If True,
        removes old layers and writes new ones. Default is False.

    Returns
    -------
    Path
        Path to updated model geopackage (or original if sure_up    date=False).
    """
    # SET PATHS to save and copy geopackges
    model_path_gpkg = Path(model_path_gpkg)

    # remove old surface impervious layer and map
    if sure_update:
        fiona.remove(
            model_path_gpkg,
            layer=surface_layer_name,
            driver="GPKG",
        )
        fiona.remove(
            model_path_gpkg,
            layer=surface_map_layer_name,
            driver="GPKG",
        )

        # Write new layers with the sqlite names
        surfaces = surfaces.drop(columns=["connection_node_id"])
        surfaces.to_file(
            model_path_gpkg,
            layer=surface_layer_name,
            driver="GPKG",
        )

        surface_map.to_file(
            model_path_gpkg,
            layer=surface_map_layer_name,
            driver="GPKG",
        )

        print("Updated model saved here:")
        print(model_path_gpkg)
    else:
        print("check the results before updateing the model. Once you have checked them update model")
    return model_path_gpkg


def run(model_path_gpkg, datacheker_path, polder_polygon_path, hdb_path, sure_update):
    # Get connection nodes assigned to FDLA polygons
    nodes_fdla, fdla = get_nodes_within_fdla(
        model_path_gpkg=model_path_gpkg,
        datacheker_path=datacheker_path,
        polder_polygon_path=polder_polygon_path,
    )

    # Create Voronoi polygons
    voronoi_cells, rows = createa_voronoi_polygons(
        nodes_fdla=nodes_fdla,
        fdla=fdla,
    )

    # Correct orphan Voronoi polygons and create final subcatchments
    subcatchments = correct_voronoi_polygons(
        voronoi_cells=voronoi_cells,
        rows=rows,
    )

    # Transform subcatchments into the format expected by 3Di
    surfaces = create_surface_layer(
        subcatchments=subcatchments,
        impervious_out_polygon_gpkg=model_path_gpkg,
    )

    # Calculate afvoernorm percentage per surface
    percentage_by_surface = get_percentage_afvoernorm(
        hdb_path=hdb_path,
        surfaces=surfaces,
    )

    # Create surface map lines
    surface_map = create_surface_map_layer(
        model_path_gpkg=model_path_gpkg,
        surfaces=surfaces,
        percentage_by_surface=percentage_by_surface,
        impervious_out_line_gpkg=model_path_gpkg,
    )

    # Write impervious_surface and impervious_surface_map to the GeoPackage
    update_model_geopackage(
        model_path_gpkg=model_path_gpkg,
        surfaces=surfaces,
        surface_map=surface_map,
        output_model_path=model_path_gpkg,
        sure_update=sure_update,
    )


# inputs
# %%
hdb_path = r"H:\01.basisgegevens\00.HDB\Hydro_database.gpkg"
folder = Path(r"H:\personen\jacosta\update_3di_model_test\Zijpe_West_2026_MR")
source_data = folder / "01_source_data"
damo_path = source_data / "DAMO.gpkg"
datacheker_path = source_data / "datachecker_output.gpkg"
polder_polygon_path = source_data / "polder_polygon.shp"
model_path_gpkg = folder / "02_schematisation" / "00_basis" / "bwn_zijpe-west.gpkg"
impervious_out_polygon_gpkg = source_data / "impervious_pol_review.gpkg"
impervious_out_line_gpkg = source_data / "impervious_line_review.gpkg"
sure_update = True

run(model_path_gpkg, datacheker_path, polder_polygon_path, hdb_path, sure_update)
# %%
