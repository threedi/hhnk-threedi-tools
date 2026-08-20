from pathlib import Path

import fiona
import geopandas as gpd
import pandas as pd
from osgeo import ogr

from hhnk_threedi_tools.breaches.clip_models.submodel_constants import COLUMNS_NAMES, SchematisationType

list_layers = [
    # "potential_breach",
    "connection_node",
    "1d_boundary_condition",
    "orifice",
    "cross_section_location",
    "channel",
]


def read_geopackage_layers(
    model_path_gpkg: Path,
    schematisation_type: SchematisationType,
    selected_layers: bool = False,
    list_layers: list | None = None,
) -> dict[str, gpd.GeoDataFrame]:

    layers_dict: dict[str, gpd.GeoDataFrame] = {}

    if selected_layers:
        if list_layers is None:
            raise ValueError("list_layers must be provided when selected_layers=True")
        layer_names = list_layers
    else:
        layer_names = fiona.listlayers(model_path_gpkg)

    for layer_name in layer_names:
        with fiona.open(model_path_gpkg, layer=layer_name) as src:
            records = list(src)
            crs = src.crs

        if records:
            gdf = gpd.GeoDataFrame.from_features(records, crs=crs)

            if schematisation_type == SchematisationType.RANA:
                # RANA: use Fiona feature ID
                gdf["id"] = [int(feat["id"]) for feat in records]

            elif schematisation_type == SchematisationType.THREEDI:
                # THREEDI: from_features() already reads properties["id"]
                # Only use Fiona ID if the layer has no 'id' property
                if "id" not in gdf.columns:
                    gdf["id"] = [int(feat["id"]) for feat in records]

        else:
            gdf = gpd.read_file(
                model_path_gpkg,
                layer=layer_name,
                engine="fiona",
            )

            if "id" not in gdf.columns:
                gdf["id"] = pd.array([], dtype="int64")

        layers_dict[layer_name] = gdf

    return layers_dict


# %%


def clean_geopackage(model_path_gpkg, polygon_path, schematisation_type):

    cn = COLUMNS_NAMES[schematisation_type]

    polygon_gdf = gpd.read_file(polygon_path)

    layers_dict = read_geopackage_layers(
        model_path_gpkg=model_path_gpkg,
        schematisation_type=schematisation_type,
        selected_layers=True,
        list_layers=list_layers,
    )

    boundary_condition = layers_dict["1d_boundary_condition"]
    connection_node = layers_dict["connection_node"]
    orifice = layers_dict["orifice"]
    cross_section_location = layers_dict["cross_section_location"]
    channel = layers_dict["channel"]
    # potential_breach = layers_dict["potential_breach"]

    # Boundary conditions over the  polygon
    boundary_condition_overlay = gpd.overlay(
        boundary_condition,
        polygon_gdf[["geometry"]],
        how="intersection",
    )
    # Boundary conditions out of the  polygon
    bc_out_of_intersection = boundary_condition.loc[
        ~boundary_condition["id"].isin(boundary_condition_overlay["id"])
    ].copy()

    bc_condition_connection_node_id = bc_out_of_intersection["connection_node_id"].tolist()

    # Orifices connected to those BC connection nodes
    orifice_out_of_intersection = orifice.loc[
        orifice[cn["connection_node_id_end"]].isin(bc_condition_connection_node_id)
    ].copy()

    orifice_connection_node_start = orifice_out_of_intersection[cn["connection_node_id_start"]].tolist()

    # Connection nodes Start of those orifices
    connection_node_selected = connection_node.loc[connection_node["id"].isin(orifice_connection_node_start)].copy()

    # Buffer around them
    connection_node_buffer = connection_node_selected.copy()
    connection_node_buffer["geometry"] = connection_node_buffer.geometry.buffer(0.1)

    bc_out_of_intersection_buffer = bc_out_of_intersection.copy()
    bc_out_of_intersection_buffer["geometry"] = bc_out_of_intersection_buffer.geometry.buffer(0.1)

    channel_orifice_overlay = gpd.overlay(
        channel,
        connection_node_buffer[["geometry"]],
        how="intersection",
    )

    channel_bc_overlay = gpd.overlay(
        channel,
        bc_out_of_intersection_buffer[["geometry"]],
        how="intersection",
    )

    # Recover original channel records
    channel_selected = channel.loc[
        channel["id"].isin(channel_orifice_overlay["id"]) | channel["id"].isin(channel_bc_overlay["id"])
    ].copy()

    # All connection nodes related to those channels
    connection_node_channel_start = channel_selected[cn["connection_node_id_start"]].to_list()
    connection_node_channel_end = channel_selected[cn["connection_node_id_end"]].to_list()
    connection_node_bc = bc_out_of_intersection["connection_node_id"].to_list()
    connection_node_selected = connection_node.loc[
        connection_node["id"].isin(connection_node_channel_start)
        | connection_node["id"].isin(connection_node_channel_end)
        | connection_node["id"].isin(connection_node_bc)
    ].copy()

    # Cross sections
    channel_buffer = channel_selected.copy()
    channel_buffer = channel_buffer.set_geometry(channel_buffer.geometry.buffer(0.1))
    crosssection_selection = cross_section_location.overlay(channel_buffer[["geometry"]])

    crosssection_overlay = gpd.overlay(
        cross_section_location,
        channel_buffer[["geometry"]],
        how="intersection",
    )
    # Recover original cross-section records
    crosssection_selection = cross_section_location.loc[
        cross_section_location["id"].isin(crosssection_overlay["id"])
    ].copy()

    # potential_breach_selected = potential_breach.loc[
    #     potential_breach["channel_id"].isin(channel_selected["id"].tolist())
    # ]
    return {
        # "potential_breach": potential_breach_selected["id"].tolist(),
        "1d_boundary_condition": bc_out_of_intersection["id"].tolist(),
        "orifice": orifice_out_of_intersection["id"].tolist(),
        "connection_node": connection_node_selected["id"].tolist(),
        "channel": channel_selected["id"].tolist(),
        "cross_section_location": crosssection_selection["id"].tolist(),
    }


# %%
def remove_selection(
    model_path_gpkg,
    layer_name,
    ids_to_remove,
    schematisation_type,
):
    ds = ogr.Open(str(model_path_gpkg), update=1)
    layer = ds.GetLayerByName(layer_name)

    fids_to_remove = []
    print(f"Removing from layer {layer_name}.Total feature to remove: {len(ids_to_remove)}")
    for feature in layer:
        if schematisation_type == SchematisationType.RANA:
            feature_id = feature.GetFID()
        else:
            feature_id = feature.GetField("id")

        if feature_id in ids_to_remove:
            fids_to_remove.append(feature.GetFID())

    # Close/reset the active read cursor before deleting
    layer.ResetReading()

    layer.StartTransaction()
    for fid in fids_to_remove:
        layer.DeleteFeature(fid)

    layer.CommitTransaction()
    layer = None
    ds = None


def run(
    model_gpkg_path,
    polygon_path,
    field_name,
    schematisation_type,
) -> None:
    polygon_gdf = gpd.read_file(polygon_path)

    model_name = Path(model_gpkg_path).stem

    polygon_gdf = polygon_gdf.loc[polygon_gdf[field_name] == model_name].copy()

    remove_dict = clean_geopackage(
        gpkg_path=model_gpkg_path,
        polygon_path=polygon_path,
        schematisation_type=schematisation_type,
    )

    for layer_name, ids_to_remove in remove_dict.items():
        remove_selection(
            gpkg_path=model_gpkg_path,
            layer_name=layer_name,
            ids_to_remove=ids_to_remove,
            schematisation_type=schematisation_type,
        )


# %%

# model_gpkg_path = r"H:\02.modellen\RegionalFloodModel\work in progress\schematisation\ROR PRI - dijktrajecten 13-8 en 13-9 - Stroom_NO\RegionalFloodModel_ROR PRI - dijktrajecten 13-8 en 13-9 - Stroom_NO.gpkg"
# polygon_path = r"H:\03.resultaten\Overstromingsberekeningenprimairedoorbraken2024\deelgebieden\ROR PRI - dijktrajecten 13-8 en 13-9 - Stroom_NO.gpkg"
