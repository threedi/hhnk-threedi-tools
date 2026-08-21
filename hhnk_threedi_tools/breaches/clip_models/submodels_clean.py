from pathlib import Path
from typing import Dict, List, Union

import fiona
import geopandas as gpd
import pandas as pd
from osgeo import ogr

from hhnk_threedi_tools.breaches.clip_models.submodel_constants import COLUMNS_NAMES, LAYER_NAMES, SchematisationType

list_layers = [
    # "potential_breach",
    "connection_node",
    "1d_boundary_condition",
    "orifice",
    "cross_section_location",
    "channel",
    "exchange_line",
]


def read_geopackage_layers(
    model_path_gpkg: Path,
    schematisation_type: SchematisationType,
    selected_layers: bool = False,
    list_layers: list | None = None,
) -> dict[str, gpd.GeoDataFrame]:
    """Read layers from a GeoPackage and return a dict of GeoDataFrames.

    Parameters: model_path_gpkg (Path), schematisation_type (SchematisationType).
    Returns: mapping layer name -> GeoDataFrame with an `id` column present.
    """

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


def clean_submodel_boundary(
    model_path_gpkg: Union[str, Path],
    polygon_path: Union[str, Path],
    schematisation_type: SchematisationType,
) -> Dict[str, List[int]]:
    """Identify features to remove outside a submodel polygon.

    Returns a dict mapping layer names to lists of feature ids to remove.
    """

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
    exchange_line = layers_dict["exchange_line"]
    # potential_breach = layers_dict["potential_breach"]

    # select channels crosses polygons boundaries
    channel_crossing_boundary = gpd.sjoin(
        channel,
        gpd.GeoDataFrame(geometry=polygon_gdf.boundary, crs=polygon_gdf.crs),
        how="inner",
        predicate="intersects",
    )

    # Select exchange lines from channels that are in the border.
    exchange_line_selected = exchange_line[exchange_line["channel_id"].isin(channel_crossing_boundary["id"])]

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
        "exchange_line": exchange_line_selected["id"].tolist(),
    }


# %%
def remove_selection(
    model_path_gpkg: Union[str, Path],
    layer_name: str,
    ids_to_remove: List[int],
    schematisation_type: SchematisationType,
) -> None:
    """Remove features with given ids from a layer inside a GeoPackage.

    Uses OGR to delete features by FID or `id` field depending on schematisation.
    """
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


def set_isolated_1d(
    model_gpkg_path: str | Path,
    layer_name: str,
    ids_to_isolate: list[int],
    schematisation_type: SchematisationType,
    isolated_value: int,
) -> None:
    """Set the calculation type to isolated for selected 1D elements.

    The value used for the isolated calculation type depends on the layer:
    - channel, pipe and culvert: calculation_type = 101
    - manhole: calculation_type = 1
    """

    # Open the GeoPackage in update mode.
    ds = ogr.Open(str(model_gpkg_path), update=1)
    layer = ds.GetLayerByName(layer_name)

    print(f"Isolating {len(ids_to_isolate)} features in layer {layer_name}")

    # Use a transaction to make updating many features considerably faster.
    layer.StartTransaction()

    for feature in layer:
        # RANA uses the GeoPackage feature ID (FID) as identifier.
        # 3Di uses the value stored in the 'id' field.
        if schematisation_type == SchematisationType.RANA:
            feature_id = feature.GetFID()
        else:
            feature_id = feature.GetField("id")

        # Only update features selected for isolation.
        if feature_id in ids_to_isolate:
            feature.SetField("calculation_type", isolated_value)
            layer.SetFeature(feature)

    layer.CommitTransaction()

    # Close the layer and GeoPackage.
    layer = None
    ds = None


def isolate_1d_elements(
    model_gpkg_path: str | Path,
    polygon_gdf: gpd.GeoDataFrame,
    schematisation_type: SchematisationType,
) -> None:
    """Isolate 1D elements that are not fully within the submodel area.

    Channels, pipes and culverts are isolated when their geometry is not
    completely within the submodel polygon.

    Manholes are also isolated for 3Di schematisations. RANA does not
    contain a manhole layer.

    The isolated calculation type is:
    - 101 for channels, pipes and culverts
    - 1 for manholes
    """

    # Get the layer-name mapping for the selected schematisation type.
    ln = LAYER_NAMES[schematisation_type]

    # These 1D layers are available in both RANA and 3Di.
    list_layers = [
        ln["channel"],
        ln["pipe"],
        ln["culvert"],
    ]

    # Manholes only exist in the 3Di schematisation.
    if schematisation_type == SchematisationType.THREEDI:
        list_layers.append(ln["manhole"])

    # Read only the layers needed for the isolation step.
    layers = read_geopackage_layers(
        model_path_gpkg=Path(model_gpkg_path),
        schematisation_type=schematisation_type,
        selected_layers=True,
        list_layers=list_layers,
    )

    # Process every 1D layer using the same spatial selection logic.
    for layer_name in layers:
        layer = layers[layer_name]

        # Select features that are completely within the submodel polygon.
        #
        # 'within' is intentionally used instead of 'intersects':
        # a line that crosses or partially lies outside the polygon should
        # also be isolated.
        layer_inside = gpd.sjoin(
            layer,
            polygon_gdf[["geometry"]],
            how="inner",
            predicate="within",
        )

        # Everything that is not fully within the polygon must be isolated.
        layer_to_isolate = layer.loc[~layer["id"].isin(layer_inside["id"])]

        # Manholes use a different calculation type value for isolation.
        if layer_name == "manhole":
            isolated_value = 1
        else:
            isolated_value = 101

        # Update the selected features directly in the GeoPackage.
        set_isolated_1d(
            model_gpkg_path=model_gpkg_path,
            layer_name=layer_name,
            ids_to_isolate=layer_to_isolate["id"].tolist(),
            schematisation_type=schematisation_type,
            isolated_value=isolated_value,
        )


# %%
def run(
    model_gpkg_path: str | Path,
    polygon_path: str | Path,
    field_name: str,
    schematisation_type: SchematisationType,
    isolate_1d: bool = False,
) -> None:
    """Run cleanup: remove out-of-bound features and optionally isolate 1D.

    Selects the polygon matching the model name, computes features to remove,
    and applies deletions; optionally isolates 1D elements by setting flags.
    """
    polygon_gdf = gpd.read_file(polygon_path)

    model_name = Path(model_gpkg_path).parent.name

    polygon_gdf = polygon_gdf.loc[polygon_gdf[field_name] == model_name].copy()

    remove_dict = clean_submodel_boundary(
        model_path_gpkg=model_gpkg_path,
        polygon_path=polygon_path,
        schematisation_type=schematisation_type,
    )

    for layer_name, ids_to_remove in remove_dict.items():
        remove_selection(
            model_path_gpkg=model_gpkg_path,
            layer_name=layer_name,
            ids_to_remove=ids_to_remove,
            schematisation_type=schematisation_type,
        )

    if isolate_1d:
        isolate_1d_elements(
            model_gpkg_path=model_gpkg_path,
            polygon_gdf=polygon_gdf,
            schematisation_type=schematisation_type,
        )


# model_gpkg_path = r"H:\02.modellen\RegionalFloodModel\work in progress\schematisation\ROR PRI - dijktrajecten 13-8 en 13-9 - Stroom_NO\RegionalFloodModel_ROR PRI - dijktrajecten 13-8 en 13-9 - Stroom_NO.gpkg"
# polygon_path = r"H:\03.resultaten\Overstromingsberekeningenprimairedoorbraken2024\deelgebieden\ROR PRI - dijktrajecten 13-8 en 13-9 - Stroom_NO.gpkg"
