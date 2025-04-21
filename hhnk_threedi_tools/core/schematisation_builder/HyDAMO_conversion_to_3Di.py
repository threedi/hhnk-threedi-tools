# %%
import shutil
from pathlib import Path
from typing import Optional, Tuple, Union

import geopandas as gpd
import hhnk_research_tools as hrt
import pandas as pd
from shapely import Point

import hhnk_threedi_tools.resources.schematisation_builder as schematisation_builder_resources

HYDAMO_LAYERS = ["hydroobject"]
SCHEMATISATION_LAYERS = ["channel", "connection_node"]


def load_all_schematisation_layers(
    empty_schematisation_file_path: Optional[Path] = None,
) -> Tuple[dict[str, gpd.GeoDataFrame], Path]:
    """
    Load schematisation layers from an empty schema file into a dictionary.

    Parameters
    ----------
    empty_schematisation_file_path : Path, default is None
        File path containing the empty schematisation. When None, it will load the empty.gpkg
        from htt.resources.schematisation_builder

    Returns
    -------
    layers_data : dict[str, gpd.GeoDataFrame]
        Dictionary of GeoDataFrames for each layer
    empty_schematisation_file_path : Path
        Path to empty gpkg, wont be None from here on.
    """
    if empty_schematisation_file_path is None:
        empty_schematisation_file_path = hrt.get_pkg_resource_path(schematisation_builder_resources, "empty.gpkg")

    layers_data = {}
    for layer in SCHEMATISATION_LAYERS:
        layers_data[layer] = gpd.read_file(empty_schematisation_file_path, layer=layer)
    return layers_data, empty_schematisation_file_path


def get_unique_id(layer) -> int:
    """Get a unique ID for a layer."""
    return layer["id"].max() + 1 if not layer.empty else 1


def find_point_in_points(point, points, tolerance: float = 1e-2):
    """Find an existing point or return None."""
    for pt in points:
        if pt["geometry"].distance(point) < tolerance:
            return pt["id"]
    return None


def process_hydroobject_layer(
    hydroobject: gpd.GeoDataFrame,
    schematisation_layers: dict,
    connection_node_id: int,
    channel_id: int,
    output_path: Path,
    crs: Union[str, int],
) -> None:
    """
    Process the hydroobject layer and save connection_node and channel layers.

    Parameters
    ----------
    hydroobject : GeoDataFrame
        The hydroobject layer.
    schematisation_layers : dict
        Dictionary with the schematisation layers.
    connection_node_id : int
        Starting ID for connection nodes.
    channel_id : int
        Starting ID for channels.
    output_path : Path
        Path to save the output layers.
    crs : Union[str, int]
        Coordinate reference system for the GeoDataFrames.

    Writes
    ------
    output_path : Path
        Write 'connection_node' and 'channel' layer to gpkg.
    """
    connection_nodes: list[dict] = []
    channels: list[dict] = []

    for _, row in hydroobject.iterrows():
        # Add start and end points of the feature to the connection_node layer
        start_point = Point(row["geometry"].coords[0])
        end_point = Point(row["geometry"].coords[-1])

        # Find or add start point
        connection_node_start_id = find_point_in_points(start_point, connection_nodes)
        if connection_node_start_id is None:
            connection_node_start_id = connection_node_id
            connection_nodes.append({"id": connection_node_id, "geometry": start_point})
            connection_node_id += 1

        # Find or add end point
        connection_node_end_id = find_point_in_points(end_point, connection_nodes)
        if connection_node_end_id is None:
            connection_node_end_id = connection_node_id
            connection_nodes.append({"id": connection_node_id, "geometry": end_point})
            connection_node_id += 1

        # Add the feature to the channel layer
        channels.append(
            {
                "id": channel_id,
                "geometry": row["geometry"],
                "connection_node_start_id": connection_node_start_id,
                "connection_node_end_id": connection_node_end_id,
                "code": row["code"],
            }
        )
        channel_id += 1

    # Get empty GeoDataFrames from schematisation_layers
    connection_node_layer = schematisation_layers.get("connection_node", gpd.GeoDataFrame())
    channel_layer = schematisation_layers.get("channel", gpd.GeoDataFrame())

    # Create GeoDataFrames
    connection_node_gdf = gpd.GeoDataFrame(connection_nodes, geometry="geometry", crs=crs)
    channel_gdf = gpd.GeoDataFrame(channels, geometry="geometry", crs=crs)

    # Concatenate the GeoDataFrames
    connection_node_gdf_2 = gpd.GeoDataFrame(
        pd.concat([connection_node_layer, connection_node_gdf], ignore_index=True), crs=crs
    )
    channel_gdf_2 = gpd.GeoDataFrame(pd.concat([channel_layer, channel_gdf], ignore_index=True), crs=crs)

    # Save the layers
    connection_node_gdf_2.to_file(output_path, layer="connection_node", engine="pyogrio")
    channel_gdf_2.to_file(output_path, layer="channel", engine="pyogrio")


def convert_to_3Di(
    hydamo_file_path: Path,
    hydamo_layers: list,
    output_schematisation_directory: Path,
    empty_schematisation_file_path: Optional[Path] = None,
) -> None:
    """
    Convert the HyDAMO file to a 3Di schematisation.
    Writing is handled in process_hydroobject_layer.

    Parameters
    ----------
    hydamo_file_path : Path
        Path to the HyDAMO file.
    hydamo_layers : list
        TODO
    output_schematisation_directory : Path
        Path to the directory where the 3Di schematisation will be stored.
    empty_schematisation_file_path : Optional[Path], default is None
        File path containing the empty schematisation. When None, it will load the empty.gpkg
        from htt.resources.schematisation_builder
    """
    # Load the HyDAMO file layers
    layers = {layer: gpd.read_file(hydamo_file_path, layer=layer) for layer in hydamo_layers}

    # Load the empty schematisation layers
    schematisation_layers, empty_schematisation_file_path = load_all_schematisation_layers(
        empty_schematisation_file_path
    )

    # Create the output directory if it does not exist
    output_schematisation_directory = Path(output_schematisation_directory)
    output_schematisation_directory.mkdir(parents=True, exist_ok=True)
    output_path = output_schematisation_directory / "3Di_schematisation.gpkg"

    # Copy the empty schematisation file to the output path
    shutil.copy2(empty_schematisation_file_path, output_path)

    # Process the hydroobject layer if present
    if "HYDROOBJECT" in layers:
        hydroobject = layers["HYDROOBJECT"]
        connection_node_id = get_unique_id(schematisation_layers.get("connection_node", gpd.GeoDataFrame()))
        channel_id = get_unique_id(schematisation_layers.get("channel", gpd.GeoDataFrame()))
        process_hydroobject_layer(
            hydroobject, schematisation_layers, connection_node_id, channel_id, output_path, hydroobject.crs
        )
    else:
        raise ValueError("No HYDROOBJECT layer found in the HyDAMO file.")
