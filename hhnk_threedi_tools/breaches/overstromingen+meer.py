# %%
import math
import os

import geopandas as gpd
from shapely import LineString, Point

bressen = r"Y:\02.modellen\RegionalFloodModel\work in progress\schematisation\breach_selection_meer.gpkg"
model_path = r"Y:\02.modellen\RegionalFloodModel\work in progress\schematisation\RegionalFloodModel.gpkg"
connection_nodes = r"Y:\02.modellen\RegionalFloodModel\work in progress\schematisation\connection_node_meer.gpkg"
channels = r"Y:\02.modellen\RegionalFloodModel\work in progress\schematisation\channel_meer.gpkg"
orifice = r"Y:\02.modellen\RegionalFloodModel\work in progress\schematisation\orifice_layout.gpkg"

boundary_condition_gdf = gpd.read_file(model_path, layer="1d_boundary_condition", driver="GPKG")
bressen_gdf = gpd.read_file(bressen, driver="GPKG")
potential_breach = gpd.read_file(model_path, layer="potential_breach", driver="GPKG")
connection_nodes_gdf = gpd.read_file(connection_nodes, driver="GPKG")
channels_gdf = gpd.read_file(channels, driver="GPKG")
orifice_gdf = gpd.read_file(orifice, driver="GPKG")

codes = bressen_gdf["code"].to_list()

breach_selection = potential_breach[potential_breach["code"].isin(codes)].copy()
breach_selection["base"] = breach_selection["code"].str.split("-", n=1).str[0]


for base_name, breach_set in breach_selection.groupby("base"):
    if base_name.split("_")[0] == "MARKEN":
        continue
    else:
        base_breach = breach_set[breach_set["code"] == (base_name + "-1000")]
        geometry_breach_1000 = base_breach.iloc[0].geometry
        initial_coordiante = Point(geometry_breach_1000.coords[0])
        final_coordinate = Point(geometry_breach_1000.coords[-1])

        for idx, final_points in breach_set.geometry.items():
            coords = list(final_points.coords)
            coords[-1] = final_coordinate
            potential_breach.loc[idx, "geometry"] = LineString(coords)

potential_breach.to_file(model_path, layer="potential_breach", driver="GPKG")

node_id_col = "id"
code_col = "code"
start_col = "connection_node_start_id"
end_col = "connection_node_end_id"

channels["base"] = channels[code_col].str.rsplit("-", n=1).str[0]

L_MAX = 3.0  # metros
last_connection_node_id = 20885
end_id_cursor = 20885

ORIF_COLS = orifice_gdf.columns
# %%
breach_selection = breach_selection[["base", "geometry"]]
# channels -> base (spatial join)
channels_join_breach = gpd.sjoin(channels_gdf, breach_selection, how="inner", predicate="intersects")

# nodes -> base (spatial join)
nodes_base = gpd.sjoin(
    connection_nodes_gdf[["id", "geometry"]],
    channels_join_breach[["base", "geometry"]],
    how="inner",
    predicate="intersects",
)
nodes_base = nodes_base[["id", "base", "geometry"]]

# filter node with no orifice
existing_node_ids = orifice_gdf["connection_node_start_id"].values
nodes_base_missing = nodes_base[~nodes_base["id"].isin(existing_node_ids)].copy()


orifice_join = gpd.sjoin(orifice_gdf, nodes_base[["base", "geometry"]], how="inner", predicate="intersects")
orifice_join = orifice_join[~orifice_join.index.duplicated(keep="first")]


orifs_clean = orifice_gdf[ORIF_COLS].copy()
orifs_clean.loc[orifice_join.index, "code"] = orifice_join["base"].values

nodes_i = connection_nodes_gdf.set_index("id")


next_orif_id = int(orifs_clean["id"].max()) + 1

new_orif_rows = []
new_end_node_rows = []
# %%
for base, df_nodes in nodes_base_missing.groupby("base"):
    # plantilla del set: code == base
    base_orifice = orifs_clean[orifs_clean["code"] == base]
    if base_orifice.empty:
        continue
    orifice_set = base_orifice.iloc[0]

    g = orifice_set.geometry
    x0, y0 = g.coords[0]
    x1, y1 = g.coords[-1]
    dx, dy = (x1 - x0, y1 - y0)

    L = math.hypot(dx, dy)
    if L == 0:
        continue
    if L > L_MAX:
        s = L_MAX / L
        dx, dy = dx * s, dy * s

    for nid in df_nodes["id"].values:
        if nid not in nodes_i.index:
            continue

        # start = node existente
        p = nodes_i.loc[nid].geometry
        print("p")
        # end point geométrico
        end_pt = Point(p.x + dx, p.y + dy)

        # end node NUEVO (solo este se crea)
        new_end_id = int(end_id_cursor)
        end_id_cursor += 1

        # nuevo orificio (copia template + cambia lo necesario)
        row = orifice_set[ORIF_COLS].to_dict()
        row["id"] = next_orif_id
        next_orif_id += 1

        row["code"] = base
        row["display_name"] = base

        row["connection_node_start_id"] = int(nid)  # EXISTE
        row["connection_node_end_id"] = int(new_end_id)  # NUEVO

        row["geometry"] = LineString([(p.x, p.y), (end_pt.x, end_pt.y)])
        new_orif_rows.append(row)

        # crear solo el END node en nodes
        end_node = dict.fromkeys(connection_nodes_gdf.columns, None)
        end_node["id"] = int(new_end_id)
        end_node["geometry"] = Point(end_pt.x, end_pt.y)
        new_end_node_rows.append(end_node)

# outputs
import pandas as pd

test_location = r"Y:\02.modellen\RegionalFloodModel\work in progress\schematisation"
new_orifice_path = os.path.join(test_location, "new_orifice.gpkg")
new_connection_nodes_path = os.path.join(test_location, "new_connection_nodes.gpkg")

new_orifs = gpd.GeoDataFrame(new_orif_rows, crs=orifs_clean.crs)
new_end_nodes = gpd.GeoDataFrame(new_end_node_rows, crs=connection_nodes_gdf.crs)

out_orifs = gpd.GeoDataFrame(new_orifs, crs=orifs_clean.crs)
out_orifs.to_file(new_orifice_path, driver="GPKG")

out_nodes = gpd.GeoDataFrame(new_end_nodes, crs=connection_nodes_gdf.crs)
out_nodes.to_file(new_connection_nodes_path, driver="GPKG")


# %%
orifice = out_orifs = gpd.GeoDataFrame(pd.concat([orifs_clean, new_orifs], ignore_index=True), crs=orifs_clean.crs)
nodes = gpd.GeoDataFrame(
    pd.concat([connection_nodes_gdf, new_end_nodes], ignore_index=True), crs=connection_nodes_gdf.crs
)
boundary_condition_gdf

for idx, bc_row in boundary_condition_gdf.iterrows():
    start_id = bc_row["connection_node_id"]

    match = orifice[orifice["connection_node_start_id"] == start_id]
    if match.empty:
        continue

    end_id = match.iloc[0]["connection_node_end_id"]

    # 1) actualizar el connection_node_id del BC al END
    boundary_condition_gdf.loc[idx, "connection_node_id"] = end_id

    # 2) mover la geometría del BC a la geometría del node END
    node_match = nodes[nodes["id"] == end_id]
    if node_match.empty:
        continue

    boundary_condition_gdf.loc[idx, "geometry"] = node_match.iloc[0].geometry

boundary_condition_gdf.to_file(model_path, layer="1d_boundary_condition", driver="GPKG")


# %%
