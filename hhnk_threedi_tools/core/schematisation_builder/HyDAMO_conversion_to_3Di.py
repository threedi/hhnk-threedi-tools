import geopandas as gpd
from pathlib import Path

LAYERS = ["hydroobject"]

def load_all_schematisation_layers(empty_schematisation_file_path):
    """Load all the layers of the empty schematisation file into a dictionary."""
    layers_data = {}
    for layer in LAYERS:
        layers_data[layer] = gpd.read_file(empty_schematisation_file_path, layer=layer)
    return layers_data

def get_unique_id(layer):
    """Get a unique ID for a layer."""
    return layer["id"].max() + 1 if not layer.empty else 1

def process_hydroobject_layer(hydroobject, connection_node_id, channel_id, output_path, crs):
    """
    Process the hydroobject layer and save connection_node and channel layers.

    Parameters
    ----------
    hydroobject : GeoDataFrame
        The hydroobject layer.
    connection_node_id : int
        Starting ID for connection nodes.
    channel_id : int
        Starting ID for channels.
    output_path : Path
        Path to save the output layers.
    crs : dict or str
        Coordinate reference system for the GeoDataFrames.
    """
    connection_nodes = []
    channels = []

    for _, row in hydroobject.iterrows():
        # Add start and end points of the feature to the connection_node layer
        start_point = row["geometry"].coords[0]
        end_point = row["geometry"].coords[-1]
        connection_nodes.append({"id": connection_node_id, "geometry": start_point})
        connection_node_id += 1
        connection_nodes.append({"id": connection_node_id, "geometry": end_point})
        connection_node_id += 1

        # Add the feature to the channel layer
        channels.append({
            "id": channel_id,
            "geometry": row["geometry"],
            "connection_node_start_id": connection_node_id - 2,
            "connection_node_end_id": connection_node_id - 1,
            "code": row["code"],
        })
        channel_id += 1

    # Create GeoDataFrames
    connection_node_gdf = gpd.GeoDataFrame(connection_nodes, crs=crs)
    channel_gdf = gpd.GeoDataFrame(channels, crs=crs)

    # Save the layers
    connection_node_gdf.to_file(output_path, layer="connection_node", driver="GPKG")
    channel_gdf.to_file(output_path, layer="channel", driver="GPKG")

def convert_to_3Di(hydamo_file_path, empty_schematisation_file_path, output_schematisation_directory):
    """
    Convert the HyDAMO file to a 3Di schematisation.

    Parameters
    ----------
    hydamo_file_path : str
        Path to the HyDAMO file.
    empty_schematisation_file_path : str
        Path to the empty schematisation file.
    output_schematisation_directory : str
        Path to the directory where the 3Di schematisation will be stored.
    """
    # Load the HyDAMO file layers
    hydamo_layers = {layer: gpd.read_file(hydamo_file_path, layer=layer) for layer in LAYERS}

    # Load the empty schematisation layers
    schematisation_layers = load_all_schematisation_layers(empty_schematisation_file_path)

    # Create the output directory if it does not exist
    output_schematisation_directory = Path(output_schematisation_directory)
    output_schematisation_directory.mkdir(parents=True, exist_ok=True)
    output_path = output_schematisation_directory / "3Di_schematisation.gpkg"

    # Process the hydroobject layer if present
    if "hydroobject" in hydamo_layers:
        hydroobject = hydamo_layers["hydroobject"]
        connection_node_id = get_unique_id(schematisation_layers.get("connection_node", gpd.GeoDataFrame()))
        channel_id = get_unique_id(schematisation_layers.get("channel", gpd.GeoDataFrame()))
        process_hydroobject_layer(hydroobject, connection_node_id, channel_id, output_path, hydroobject.crs)
