from pathlib import Path

import fiona
import geopandas as gpd
import pandas as pd
from shapely import get_parts, voronoi_polygons
from shapely.geometry import MultiPoint

# ------------------------------------------------------------
# INPUTS
# ------------------------------------------------------------

gpkg_path = Path(r"D:\path\to\your_model.gpkg")

out_gpkg = gpkg_path.with_name(gpkg_path.stem + "_impervious_review.gpkg")
out_csv = gpkg_path.with_name("v2_impervious_surface_map_new.csv")


# ------------------------------------------------------------
# READ LAYERS
# ------------------------------------------------------------

print(fiona.listlayers(gpkg_path))

nodes = gpd.read_file(gpkg_path, layer="v2_connection_nodes")
fdla = gpd.read_file(gpkg_path, layer="fixeddrainagelevelarea")
polder = gpd.read_file(gpkg_path, layer="polder")

nodes = nodes.rename(columns={"id": "con_id"})
fdla = fdla.rename(columns={"id": "fdla_id", "code": "fdla_code"})

nodes = nodes.to_crs(fdla.crs)
polder = polder.to_crs(fdla.crs)


# ------------------------------------------------------------
# GET USED CONNECTION NODES
# ------------------------------------------------------------

available_layers = fiona.listlayers(gpkg_path)

network_layers = [
    "v2_channel",
    "v2_culvert",
    "v2_pipe",
    "v2_weir",
    "v2_orifice",
]
connection_node_columns = [
    "connection_node_start_id",
    "connection_node_end_id",
]
# Create a list of node with no repetition set()
used_node_ids = set()

for layer in network_layers:
    if layer not in available_layers:
        continue

    df = gpd.read_file(gpkg_path, layer=layer, ignore_geometry=True)

    for connection_node_col in connection_node_columns:
        if connection_node_col in df.columns:
            # append the valid connection node
            used_node_ids.update(df[connection_node_col].dropna().astype(int).tolist())


# ------------------------------------------------------------
# REMOVE BOUNDARY NODES
# ------------------------------------------------------------

boundary_node_ids = set()

if "v2_1d_boundary_conditions" in available_layers:
    bc = gpd.read_file(
        gpkg_path,
        layer="v2_1d_boundary_conditions",
        ignore_geometry=True,
    )

    if "connection_node_id" in bc.columns:
        boundary_node_ids = set(bc["connection_node_id"].dropna().astype(int).tolist())

nodes = nodes[nodes["con_id"].isin(used_node_ids)].copy()
nodes = nodes[~nodes["con_id"].isin(boundary_node_ids)].copy()

print("Valid nodes:", len(nodes))


# ------------------------------------------------------------
# CLIP FDLA WITH POLDER
# ------------------------------------------------------------

fdla = gpd.clip(fdla, polder)
fdla["geometry"] = fdla.geometry.make_valid()


# ------------------------------------------------------------
# ASSIGN NODES TO FDLA
# ------------------------------------------------------------

nodes_fdla = gpd.sjoin(
    nodes[["con_id", "geometry"]],
    fdla[["fdla_id", "fdla_code", "geometry"]],
    how="inner",
    predicate="intersects",
)

nodes_fdla = nodes_fdla.drop(columns=["index_right"], errors="ignore")
nodes_fdla = nodes_fdla.drop_duplicates(subset=["con_id"])

print("Nodes assigned to FDLA:", len(nodes_fdla))


# ------------------------------------------------------------
# CREATE VORONOI POLYGONS PER FDLA
# ------------------------------------------------------------

rows = []

for _, area in fdla.iterrows():
    fdla_id = area["fdla_id"]
    fdla_code = area["fdla_code"]
    fdla_geom = area.geometry

    local_nodes = nodes_fdla[nodes_fdla["fdla_id"] == fdla_id].copy()

    points = MultiPoint(local_nodes.geometry.tolist())
    voronoi = voronoi_polygons(points, extend_to=fdla_geom.envelope)

    voronoi_cells = gpd.GeoDataFrame(
        geometry=list(get_parts(voronoi)),
        crs=fdla.crs,
    )
    # Clip Voronoi cells with the current FDLA
    voronoi_cells = gpd.clip(voronoi_cells, gpd.GeoDataFrame(geometry=[fdla_geom], crs=fdla.crs))

    # Remove empty geometries
    voronoi_cells = voronoi_cells[~voronoi_cells.geometry.is_empty].copy()

    # Spatial join: assign each Voronoi polygon to the node inside it
    voronoi_cells = gpd.sjoin(
        voronoi_cells,
        local_nodes[["con_id", "geometry"]],
        how="inner",
        predicate="intersects",
    )
    voronoi_cells["fdla_code"] = fdla_code

    rows.append(voronoi_cells[["con_id", "fdla_code", "geometry"]])

subcatchments = gpd.GeoDataFrame(rows, crs=fdla.crs)

subcatchments = subcatchments[subcatchments.geometry.notna()]
subcatchments = subcatchments[~subcatchments.geometry.is_empty]
subcatchments["geometry"] = subcatchments.geometry.make_valid()


# ------------------------------------------------------------
# DISSOLVE BY CONNECTION NODE
# ------------------------------------------------------------

subcatchments = subcatchments.dissolve(
    by=["con_id", "fdla_code"],
    as_index=False,
)

subcatchments["area"] = subcatchments.geometry.area

print("Subcatchments:", len(subcatchments))


# ------------------------------------------------------------
# MAKE v2_impervious_surface_new
# ------------------------------------------------------------

surfaces = subcatchments.copy()

surfaces["id"] = surfaces["con_id"]
surfaces["display_name"] = surfaces["fdla_code"]
surfaces["code"] = surfaces["fdla_code"].astype(str) + "_node_" + surfaces["con_id"].astype(str)

surfaces["surface_class"] = "gesloten verharding"
surfaces["surface_sub_class"] = None
surfaces["surface_inclination"] = "uitgestrekt"
surfaces["zoom_category"] = 1
surfaces["nr_of_inhabitants"] = 0
surfaces["dry_weather_flow"] = 0.0

surfaces = surfaces[
    [
        "id",
        "display_name",
        "code",
        "surface_class",
        "surface_sub_class",
        "surface_inclination",
        "zoom_category",
        "nr_of_inhabitants",
        "dry_weather_flow",
        "area",
        "geometry",
    ]
]


# ------------------------------------------------------------
# MAKE v2_impervious_surface_map_new
# ------------------------------------------------------------

surface_map = pd.DataFrame(
    {
        "id": subcatchments["con_id"].astype(int),
        "impervious_surface_id": subcatchments["con_id"].astype(int),
        "connection_node_id": subcatchments["con_id"].astype(int),
        "percentage": 100.0,
    }
)


# ------------------------------------------------------------
# WRITE OUTPUTS
# ------------------------------------------------------------

if out_gpkg.exists():
    out_gpkg.unlink()

surfaces.to_file(
    out_gpkg,
    layer="v2_impervious_surface_new",
    driver="GPKG",
)

subcatchments.to_file(
    out_gpkg,
    layer="subcatchments_by_connection_node",
    driver="GPKG",
)

surface_map.to_csv(out_csv, index=False)

print("Done")
print(out_gpkg)
print(out_csv)
