# %%
import shutil
from pathlib import Path

import fiona
import geopandas as gpd
import pandas as pd
from shapely import get_parts, voronoi_polygons
from shapely.geometry import LineString, MultiPoint

from hhnk_threedi_tools import Folders


# %%
def createa_voronoi_polygons(model_path_gpkg, datacheker_path, polder_polygon_path):
    # read geopackges to create tissen polygons.
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

    # print("Nodes assigned to FDLA:", len(nodes_fdla))

    # create list to store voronoi polygons
    rows = []
    for _, area in fdla.iterrows():
        # take the id, code and geom from the fdla gdf
        fdla_id = area["fdla_id"]
        fdla_code = area["fdla_code"]
        fdla_geom = area.geometry

        # select local nodes from the iterations base con fdla id
        local_nodes = nodes_fdla[nodes_fdla["fdla_id"] == fdla_id].copy()

        # if there is only one node add it to the list  with  the fdla_code, con_id and geometry
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

        # Clip Voronoi cells with the current FDLA
        voronoi_cells = gpd.clip(voronoi_cells, gpd.GeoDataFrame(geometry=[fdla_geom], crs=fdla.crs))

        # filter empty geometries
        voronoi_cells = voronoi_cells[~voronoi_cells.geometry.is_empty].copy()

        # Spatial join: assign each Voronoi polygon to the node inside it base on the current
        # fdla (local_nodes). Keep columns con_id
        voronoi_cells = gpd.sjoin(
            voronoi_cells,
            local_nodes[["con_id", "geometry"]],
            how="inner",
        )
        # add code columns to the voronoi polygons
        voronoi_cells["fdla_code"] = fdla_code

        # add or extend the list using the voronoi_cells
        rows.extend(voronoi_cells[["con_id", "fdla_code", "geometry"]].to_dict("records"))

    # Create from the rows a gdf
    subcatchments = gpd.GeoDataFrame(rows, geometry="geometry", crs=fdla.crs)
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
    # Get the area from subchatchments
    subcatchments["area"] = subcatchments.geometry.area

    print("Subcatchments:", len(subcatchments))
    return subcatchments


def create_surface_layer(subcatchments, impervious_out_polygon_gpkg):
    # from subcahtments geodataframe create de columns base on the sqlite
    surfaces = subcatchments.copy()
    surfaces["id"] = surfaces["con_id"]
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
    surfaces.to_file(
        impervious_out_polygon_gpkg,
        layer="v2_impervious_surface_new",
        driver="GPKG",
    )

    return surfaces


# %%
def get_percentage_afvoernorm(hdb_path, surfaces):
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


def create_surface_map_layer(model_path_gpkg, surfaces, percentage_by_surface, impervious_out_line_gpkg):
    # select node form the model gkpg
    nodes = gpd.read_file(model_path_gpkg, layer="connection_node")

    # change columns names base con surface map sqlite columns
    surfaces_for_map = surfaces[["id", "geometry"]].copy()
    surfaces_for_map = surfaces_for_map.rename(columns={"id": "impervious_surface_id"})
    surfaces_for_map = surfaces_for_map.rename_geometry("surface_geometry")

    # change columns names base con surface map sqlite columns
    nodes_for_map = nodes[["id", "geometry"]].copy()
    nodes_for_map = nodes_for_map.rename(columns={"id": "connection_node_id"})
    nodes_for_map = nodes_for_map.rename_geometry("node_geometry")

    # marge both databases to get ids in the table
    surface_map = surfaces_for_map.merge(
        nodes_for_map,
        left_on="impervious_surface_id",
        right_on="connection_node_id",
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
        # create a centroid in the voronoi polygon
        surface_centroid = row["surface_geometry"].centroid
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

    surface_map.to_file(
        impervious_out_line_gpkg,
        layer="v2_impervious_surface_map_new",
        driver="GPKG",
    )
    return surface_map


def update_model_geopackage(
    model_path_gpkg,
    surfaces,
    surface_map,
    output_model_path=None,
    surface_layer_name="impervious_surface",
    surface_map_layer_name="impervious_surface_map",
    sure_update=None,
):
    # SET PATHS to save and copy geopackges
    model_path_gpkg = Path(model_path_gpkg)
    backup_folder = model_path_gpkg.parent / "_backup"
    backup_path = backup_folder / f"{model_path_gpkg.stem}.gpkg"

    # copy backup
    shutil.copy2(model_path_gpkg, backup_path)
    print("Backup created:")
    print(backup_path)
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


# %%
# inputs

hdb_path = r"H:\01.basisgegevens\00.HDB\Hydro_database.gpkg"
folder = Folders(r"H:\02.modellen\grootslag_leggertool")
damo_path = folder.source_data.damo.path
datacheker_path = folder.source_data.datachecker.path
polder_polygon_path = folder.source_data.polder_polygon.path
model_path_gpkg = folder.model.schema_base.path / "bwn_grootslag.gpkg"
impervious_out_polygon_gpkg = model_path_gpkg.with_name(model_path_gpkg.stem + "_impervious_pol_review.gpkg")
impervious_out_line_gpkg = model_path_gpkg.with_name(model_path_gpkg.stem + "_impervious_line_review.gpkg")
out_csv = model_path_gpkg.with_name("v2_impervious_surface_map_new.csv")

subcatchments = createa_voronoi_polygons(model_path_gpkg, datacheker_path, polder_polygon_path)
surfaces = create_surface_layer(subcatchments, impervious_out_polygon_gpkg)
percentage_by_surface = get_percentage_afvoernorm(hdb_path, surfaces)
surface_map = create_surface_map_layer(model_path_gpkg, surfaces, percentage_by_surface, impervious_out_line_gpkg)
update_model_geopackage(
    model_path_gpkg,
    surfaces,
    surface_map,
    output_model_path=None,
    surface_layer_name="impervious_surface",
    surface_map_layer_name="impervious_surface_map",
    sure_update=False,
)

# %%
