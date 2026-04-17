# %%
# importing external dependencies
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import geopandas as gpd
import numpy as np
import pandas as pd

import hhnk_threedi_tools.core.vergelijkingstool.config as config
from hhnk_threedi_tools.core.vergelijkingstool import styling, utils
from hhnk_threedi_tools.core.vergelijkingstool.config import *
from hhnk_threedi_tools.core.vergelijkingstool.Dataset import DataSet
from hhnk_threedi_tools.core.vergelijkingstool.qml_styling_files import Threedi as Threedi_styling_path
from hhnk_threedi_tools.core.vergelijkingstool.styling import *
from hhnk_threedi_tools.core.vergelijkingstool.utils import ModelInfo

threedi_model = Threedimodel(fn_threedimodel, model_info=model_info)
damo_new = DAMO(
    model_info,
    fn_damo_new,
    fn_hdb_new,
    clip_shape=selection_shape,
    layer_selection=False,
    layers_input_hdb_selection=layers_input_hdb_selection,
    layers_input_damo_selection=layers_input_damo_selection,
)
table_struc_model: Dict[str, gpd.GeoDataFrame] = {}
table_struc_DAMO: Dict[str, gpd.GeoDataFrame] = {}

for struc in config.STRUCTURE_CODES:
    # collect per-structure GeoDataFrames from model and DAMO
    table_struc_model[struc] = threedi_model.get_structure_by_code(struc, config.THREEDI_STRUCTURE_LAYERS)
    table_struc_DAMO[struc] = damo_new.get_structure_by_code(struc, config.DAMO_HDB_STRUCTURE_LAYERS)

table_C: Dict[str, gpd.GeoDataFrame] = {}
model_name = model_info.model_name
# for layer in table_struc_model.keys():
for layer in ["KST"]:
    # check and align CRS between model and DAMO
    if table_struc_model[layer].empty:
        continue
    else:
        if table_struc_DAMO[layer].crs == table_struc_model[layer].crs:
            print(f"CRS of DAMO and model data {layer} is equal")
        else:
            print(f"CRS of DAMO and model data {layer} is not equal")

            table_struc_model[layer] = (
                table_struc_model[layer].set_crs(epsg=4326).to_crs(crs=table_struc_DAMO[layer].crs)
            )
    crs = table_struc_DAMO[layer].crs

    # Add geometry information (length/area) to dataframe
    table_struc_model[layer] = threedi_model.add_geometry_info(table_struc_model[layer])
    table_struc_DAMO[layer] = threedi_model.add_geometry_info(table_struc_DAMO[layer])

    # mark presence in dataset for later 'in_both' column
    table_struc_model[layer]["dataset"] = True
    table_struc_DAMO[layer]["dataset"] = True

    # outer merge on the two tables with suffixes
    table_struc_model[layer] = table_struc_model[layer].add_suffix("_model").rename(columns={"code_model": "code"})
    table_struc_DAMO[layer] = table_struc_DAMO[layer].add_suffix("_damo").rename(columns={"code_damo": "code"})
    table_merged = table_struc_model[layer].merge(table_struc_DAMO[layer], how="outer", on="code")
    print(table_merged)
    table_merged["geometry"] = None
    table_merged = gpd.GeoDataFrame(table_merged, geometry="geometry")
    # fillna values of the two columns by False
    table_merged[["dataset_model", "dataset_damo"]] = table_merged[["dataset_model", "dataset_damo"]].fillna(
        value=False
    )

    # add column with values model, damo or both, depending on code
    inboth = []

    for i in range(len(table_merged)):
        if table_merged["dataset_model"][i] & table_merged["dataset_damo"][i]:
            inboth.append(f"{model_name} both")
        elif table_merged["dataset_model"][i] and not table_merged["dataset_damo"][i]:
            inboth.append(f"{model_name} sqlite")
        else:
            inboth.append(f"{model_name} damo")
    table_merged["in_both"] = inboth

    # use geometry of model when feature exists in model or in both model and damo. Use geometry of damo when feature only exists in damo
    # Inicializar geometría
    table_merged["geometry"] = None

    # Usar geometría del modelo (si existe en modelo o en ambos)
    mask_model = (table_merged["in_both"] == f"{model_name} sqlite") | (
        table_merged["in_both"] == f"{model_name} both"
    )

    table_merged.loc[mask_model, "geometry"] = table_merged.loc[mask_model, "geometry_model"]

    # Usar geometría de DAMO (solo si existe solo en DAMO)
    mask_damo = table_merged["in_both"] == f"{model_name} damo"

    table_merged.loc[mask_damo, "geometry"] = table_merged.loc[mask_damo, "geometry_damo"]

    print(table_merged)
    table_merged = threedi_model.drop_unused_geoseries(table_merged, keep="geometry")
    if table_merged.columns.__contains__("the_geom_model"):
        table_merged.drop(columns=["the_geom_model"], inplace=True)

    table_C[layer] = gpd.GeoDataFrame(
        threedi_model.drop_unused_geoseries(table_merged, keep="geometry"), geometry="geometry"
    )


###########################


# %%
# %%
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import geopandas as gpd
import pandas as pd

from hhnk_threedi_tools.core.vergelijkingstool import json_files as json_files_path
from hhnk_threedi_tools.core.vergelijkingstool import styling, utils
from hhnk_threedi_tools.core.vergelijkingstool.config import *
from hhnk_threedi_tools.core.vergelijkingstool.Dataset import DataSet
from hhnk_threedi_tools.core.vergelijkingstool.qml_styling_files import DAMO as DAMO_styling_path
from hhnk_threedi_tools.core.vergelijkingstool.utils import ModelInfo

# %%
# %%
damo_old = DAMO(
    model_info,
    fn_damo_old,
    fn_hdb_old,
    clip_shape=selection_shape,
    layer_selection=layer_selection,
    layers_input_hdb_selection=layers_input_hdb_selection,
    layers_input_damo_selection=layers_input_damo_selection,
)
damo_new = DAMO(
    model_info,
    fn_damo_new,
    fn_hdb_new,
    clip_shape=selection_shape,
    layer_selection=layer_selection,
    layers_input_hdb_selection=layers_input_hdb_selection,
    layers_input_damo_selection=layers_input_damo_selection,
)
table_A = damo_new.data.copy()
table_B = damo_old.data.copy()
table_C = {}
attribute_comparison = damo_new.json_files_path / "model_attribute_comparison.json"
for layer in table_A.keys():
    # skip missing layers in the other dataset
    if not table_B.keys().__contains__(layer):
        damo_new.logger.warning(f"Layer {layer} does not exist in the other dataset, skipping layer")
        continue

    damo_new.logger.debug(f"Merging {layer} of A and B datasets")

    # Reproject crs B to A if crs are different
    if table_A[layer].crs == table_B[layer].crs:
        damo_new.logger.debug(f"CRS of layer {layer} is equal")
    else:
        table_B[layer].to_crs(crs=table_A[layer].crs)
    crs = table_A[layer].crs

    try:
        # Add geometry information (length/area) to dataframe
        damo_new.logger.debug(f"Adding geometry to table_A[{layer}]")
        table_A[layer] = damo_new.add_geometry_info(table_A[layer])
        damo_new.logger.debug(f"Adding geometry to table_B[{layer}]")
        table_B[layer] = damo_old.add_geometry_info(table_B[layer])

        # Add 'dataset' column, after merging this will become 'dataset_A' and 'dataset_B'
        table_A[layer]["dataset"] = True
        table_B[layer]["dataset"] = True

        # Add layer of origin to dataset
        table_A[layer]["origin"] = layer
        table_B[layer]["origin"] = layer

        if "code" not in table_A[layer].columns:
            if layer == "waterdeel":
                table_A[layer]["code"] = table_A[layer].index.astype("string")

        if "code" not in table_B[layer].columns:
            if layer == "waterdeel":
                table_B[layer]["code"] = table_B[layer].index.astype("string")

        # outer merge on the two tables with suffixes
        table_A[layer] = table_A[layer].add_suffix("_New").rename(columns={"code_New": "code"})
        table_B[layer] = table_B[layer].add_suffix("_Old").rename(columns={"code_Old": "code"})

        # make sure the code column is of type string for proper merging
        table_A[layer]["code"] = table_A[layer]["code"].astype("string")
        table_B[layer]["code"] = table_B[layer]["code"].astype("string")

        table_merged = table_A[layer].merge(table_B[layer], how="outer", on="code")  # Futurewarning
        table_merged["geometry"] = None
        table_merged = gpd.GeoDataFrame(table_merged, geometry="geometry")

        # fillna values of the two columns by False
        table_merged[["dataset_New", "dataset_Old"]] = table_merged[["dataset_New", "dataset_Old"]].fillna(value=False)

        # add column with values A, B or AB, depending on code
        inboth: List[str] = []
        for i in range(len(table_merged)):
            if table_merged["dataset_New"][i] & table_merged["dataset_Old"][i]:
                inboth.append(f"{damo_new.model_name} both")
                # geometry_adjusted.append(table_merged['geometry_A'][i] != table_merged['geometry_B'][i])
            elif table_merged["dataset_New"][i] and not table_merged["dataset_Old"][i]:
                inboth.append(f"{damo_new.model_name} new")
                # geometry_adjusted.append(None)
            else:
                inboth.append(f"{damo_new.model_name} old")
                # geometry_adjusted.append(None)
        table_merged["in_both"] = inboth
        table_merged["geometry_adjusted"] = table_merged["geometry_New"] != table_merged["geometry_Old"]

        # handle geometrical comparison layers separately (polygons/complex ops)
        if layer in [x.lower() for x in GEOMETRICAL_COMPARISON_LAYERS]:
            if layer == "waterdeel":
                # union/intersect/difference approach for 'waterdeel'
                union_A = table_merged.geometry_New.unary_union
                union_B = table_merged.geometry_Old.unary_union
                intersection = union_A.intersection(union_B)
                diff_A = union_A.difference(union_B)
                diff_B = union_B.difference(union_A)

                df_intersections = (
                    gpd.GeoDataFrame(gpd.GeoSeries([intersection, diff_A, diff_B]))
                    .rename(columns={0: "geometry"})
                    .set_geometry("geometry")
                )
                df_intersections["in_both"] = pd.Series(
                    [f"{damo_new.model_name} both", f"{damo_new.model_name} new", f"{damo_new.model_name} old"]
                )
                df_intersections = df_intersections.explode(column="geometry", index_parts=True)
                table_merged = df_intersections[df_intersections.geometry.geom_type == "Polygon"].copy()

                # set geometry area columns appropriately
                table_merged.loc[
                    table_merged["in_both"].isin([f"{damo_new.model_name} new", f"{damo_new.model_name} both"]),
                    "geom_area_New",
                ] = table_merged["geometry"].area
                table_merged.loc[
                    ~table_merged["in_both"].isin([f"{damo_new.model_name} new", f"{damo_new.model_name} both"]),
                    "geom_area_New",
                ] = None

                table_merged.loc[
                    table_merged["in_both"].isin([f"{damo_new.model_name} old", f"{damo_new.model_name} both"]),
                    "geom_area_Old",
                ] = table_merged["geometry"].area
                table_merged.loc[
                    ~table_merged["in_both"].isin([f"{damo_new.model_name} old", f"{damo_new.model_name} both"]),
                    "geom_area_Old",
                ] = None

                table_merged["geom_length_New"] = 0
                table_merged["geom_length_Old"] = 0
            else:
                # compute intersection/difference geometries per code
                intersection = gpd.GeoDataFrame(
                    pd.concat(
                        [
                            table_merged.code,
                            table_merged["geometry_New"].intersection(table_merged["geometry_Old"]),
                        ],
                        axis=1,
                    )
                ).rename(columns={0: "geometry_diff"})
                intersection["origin"] = "intersection"
                diff_A = gpd.GeoDataFrame(
                    pd.concat(
                        [
                            table_merged.code,
                            table_merged["geometry_New"].difference(table_merged["geometry_Old"]),
                        ],
                        axis=1,
                    )
                ).rename(columns={0: "geometry_diff"})
                diff_A["origin"] = "diff_New"
                diff_B = gpd.GeoDataFrame(
                    pd.concat(
                        [
                            table_merged.code,
                            table_merged["geometry_Old"].difference(table_merged["geometry_New"]),
                        ],
                        axis=1,
                    )
                ).rename(columns={0: "geometry_diff"})
                diff_B["origin"] = "diff_Old"

                df_intersections = pd.concat([intersection, diff_A, diff_B])
                df_intersections = df_intersections[df_intersections["geometry_diff"].notna()]
                table_merged = table_merged.merge(df_intersections, how="outer", on="code")

                # choose significant geometry when geometry_diff missing
                table_merged["geometry_diff"] = table_merged.apply(
                    lambda x: damo_new.get_significant_geometry(x["in_both"], x["geometry_New"], x["geometry_Old"])
                    if (x["geometry_diff"] is None)
                    else x["geometry_diff"],
                    axis=1,
                )
                table_merged["origin"].fillna(table_merged["in_both"], inplace=True)
                table_merged["geometry"] = table_merged["geometry_diff"]

                table_merged = table_merged.explode(column="geometry", index_parts=True)
                table_merged = table_merged[table_merged.geometry.geom_type == "Polygon"]
        else:
            # prefer model (New) geometry when present
            table_merged["geometry"] = table_merged.apply(
                lambda x: damo_new.get_significant_geometry(x["in_both"], x["geometry_New"], x["geometry_Old"]),
                axis=1,
            )
            # remove temporary geo-series columns and store resulting GeoDataFrame
        table_merged = damo_new.drop_unused_geoseries(table_merged, keep="geometry")
        table_C[layer] = gpd.GeoDataFrame(table_merged, geometry="geometry", crs=crs)

    except KeyError as err:
        # missing expected column -> skip layer
        damo_new.logger.warning(f"Column {err.args[0]} not found in layer {layer}, skipping layer")
# %%


for i, layer_name in enumerate(table_C):
    damo_new.logger.debug(f"Layer name: {layer_name}")
    count_A = len(
        table_C[layer_name].loc[
            (table_C[layer_name]["in_both"] == f"{damo_new.model_name} new")
            | (table_C[layer_name]["in_both"] == f"{damo_new.model_name} both")
        ]
    )
    count_B = len(
        table_C[layer_name].loc[
            (table_C[layer_name]["in_both"] == f"{damo_new.model_name} old")
            | (table_C[layer_name]["in_both"] == f"{damo_new.model_name} both")
        ]
    )
    count_diff = count_B - count_A

    # sum geometry-based metrics
    length_A = sum(
        table_C[layer_name]
        .loc[
            (table_C[layer_name]["in_both"] == f"{damo_new.model_name} new")
            | (table_C[layer_name]["in_both"] == f"{damo_new.model_name} both")
        ]
        .geom_length_New
    )
    length_B = sum(
        table_C[layer_name]
        .loc[
            (table_C[layer_name]["in_both"] == f"{damo_new.model_name} old")
            | (table_C[layer_name]["in_both"] == f"{damo_new.model_name} both")
        ]
        .geom_length_Old
    )
    length_diff = length_B - length_A
    area_A = sum(
        table_C[layer_name]
        .loc[
            (table_C[layer_name]["in_both"] == f"{damo_new.model_name} new")
            | (table_C[layer_name]["in_both"] == f"{damo_new.model_name} both")
        ]
        .geom_area_New
    )
    area_B = sum(
        table_C[layer_name]
        .loc[
            (table_C[layer_name]["in_both"] == f"{damo_new.model_name} old")
            | (table_C[layer_name]["in_both"] == f"{damo_new.model_name} both")
        ]
        .geom_area_Old
    )
    number_of_critical = sum(table_C[layer_name]["number_of_critical"].sum())

    area_diff = area_B - area_A
    statistics.loc[layer_name, :] = [
        count_A,
        count_B,
        count_diff,
        length_A,
        length_B,
        length_diff,
        area_A,
        area_B,
        area_diff,
        number_of_critical,
    ]
statistics = statistics.fillna(0).astype("int64")


########################33
# %%
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

threedi_damo_gdf = gpd.read_file(
    r"D:\01.modelrepos\Martine\zijpe_west\01_source_data\vergelijkingstool\output\3DI_DAMO.gpkg", layer="KGM"
)
# 1. Convertir capacity_model de l/s a m3/min
threedi_damo_gdf["capacity_model_m3min"] = threedi_damo_gdf["capacity_model"] * 60 / 1000

# 2. Calcular cambio porcentual: (damo - model) / model * 100
threedi_damo_gdf["change_pct_damo_vs_model"] = (
    (threedi_damo_gdf["maximalecapaciteit_damo"] - threedi_damo_gdf["capacity_model_m3min"])
    / threedi_damo_gdf["capacity_model_m3min"]
) * 100

# 3. Eliminar registros sin cambio (cambio_pct == 0)
mask = threedi_damo_gdf["change_pct_damo_vs_model"] != 0

# 4. Seleccionar solo las columnas requeridas
columnas = [
    "code",
    "ws_categorie_damo",
    "capacity_model",  # l/s
    "capacity_model_m3min",  # m3/min
    "maximalecapaciteit_damo",  # m3/min
    "change_pct_damo_vs_model",
]

resultado = threedi_damo_gdf.loc[mask, columnas].copy()
resultado = resultado[~(resultado["capacity_model"].isna() & (resultado["maximalecapaciteit_damo"].isna()))].copy()
resultado = resultado[resultado["change_pct_damo_vs_model"].abs() >= 0.1].copy()
resultado = resultado.merge(threedi_damo_gdf[["code", "functiegemaal_damo"]], on="code", how="left").rename(
    columns={"functiegemaal_damo": "functie gemaal"}
)
resultado = resultado.merge(threedi_damo_gdf[["code", "geometry"]], on="code", how="left")
resultado["change_pct_display"] = resultado["change_pct_damo_vs_model"].map(
    lambda x: f"{x:.1f}%" if pd.notna(x) else ""
)
cols = [
    "code",
    "functie gemaal",
    "ws_categorie_damo",
    "capacity_model",
    "capacity_model_m3min",
    "maximalecapaciteit_damo",
    "change_pct_damo_vs_model",
    "change_pct_display",
    "geometry",
]

resultado = resultado[cols]

resultado = gpd.GeoDataFrame(resultado, geometry="geometry", crs=threedi_damo_gdf.crs)
output_path = Path(
    r"D:\01.modelrepos\Martine\zijpe_west\01_source_data\vergelijkingstool\output\gemaal_capaciteit_damo_vs_3di.gpkg"
)

resultado.to_file(output_path, driver="GPKG")

output_path = Path(
    r"D:\01.modelrepos\Martine\zijpe_west\01_source_data\vergelijkingstool\output\gemaal_capaciteit_damo_vs_3di.xlsx"
)

resultado.drop(columns="geometry", errors="ignore").to_excel(output_path, index=False)

resultado
# %%
import geopandas as gpd
import numpy as np

grid_cell_culverts = (
    r"\\corp.hhnk.nl\data\Hydrologen_data\Data\02.modellen\Regionaal Overstromingsmodel\cell_obstacles_culverts.gpkg"
)
calculation_grid = r"Y:\02.modellen\RegionalFloodModel  - deelmodel amstelmeerboezem_isolated\work in progress\schematisation\AB_calculation_grid.gpkg"

grid_cell_gdf = gpd.read_file(grid_cell_culverts)
flow_lines_gdf = gpd.read_file(calculation_grid, layer="flowline")


# set id per cell
grid_cell_gdf = grid_cell_gdf.reset_index(drop=True).copy()
grid_cell_gdf["grid_id"] = grid_cell_gdf.index

# count all flow lines that falls inside each cell
join_all = gpd.sjoin(
    grid_cell_gdf[["grid_id", "geometry"]],
    flow_lines_gdf[["line_type", "geometry"]],
    how="left",
    predicate="intersects",
)

count_all = join_all.groupby("grid_id")["index_right"].count().rename("n_flowlines_total").reset_index()

# count lines obstacle type 101
join_101 = gpd.sjoin(
    grid_cell_gdf[["grid_id", "geometry"]],
    flow_lines_gdf.loc[flow_lines_gdf["line_type"] == 101, ["line_type", "geometry"]],
    how="left",
    predicate="intersects",
)

count_101 = join_101.groupby("grid_id")["index_right"].count().rename("n_flowlines_101").reset_index()

# merge the count
result = grid_cell_gdf.merge(count_all, on="grid_id", how="left")
result = resultado.merge(count_101, on="grid_id", how="left")

############################
# %%
import geopandas as gpd
import rasterio
from rasterio.features import shapes
from shapely.geometry import shape

raster_path = r"\\corp.hhnk.nl\data\Hydrologen_data\Data\02.modellen\RegionalFloodModel - deelmodel Schermer Laag Zuid\flood_aread.tif"
output_gpkg = r"\\corp.hhnk.nl\data\Hydrologen_data\Data\02.modellen\RegionalFloodModel - deelmodel Schermer Laag Zuid\polygons.gpkg"

with rasterio.open(raster_path) as src:
    image = src.read(1)  # first band
    mask = image == 1
    transform = src.transform
    crs = src.crs

    records = [
        {"geometry": shape(geom), "value": value} for geom, value in shapes(image, mask=mask, transform=transform)
    ]


#  Create GeoDataFrame WITHOUT crs
gdf = gpd.GeoDataFrame(records)

# Assign CRS separately
gdf.set_crs(crs, inplace=True)

# fix invalid geometries
gdf["geometry"] = gdf.geometry.buffer(1)

# Save
gdf.to_file(output_gpkg, driver="GPKG")
# %%
