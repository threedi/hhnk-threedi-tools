import geopandas as gpd


# Example function
# TODO: list_features could also be summary validation/fix dataframe with codes to remove.
def remove_features(gdf_HyDAMO: "gpd.GeoDataFrame", layer: str, list_features: list, logger) -> "gpd.GeoDataFrame":
    """Remove features from the HyDAMO geodataframe.

    Args:
        gdf_HyDAMO (gpd.GeoDataFrame): HyDAMO geodataframe.
        layer (str): The layer from which to remove old features.
        list_features (list): List of old feature IDs to remove.

    Returns:
        gpd.GeoDataFrame: HyDAMO geodataframe without the specified features.
    """
    features_layer = gdf_HyDAMO[gdf_HyDAMO["layer"] == layer]
    if not features_layer.empty:
        try:
            features_layer_adjusted = features_layer[~features_layer["code"].isin(list_features)]
            gdf_HyDAMO.update(features_layer_adjusted)
            logger.info(
                f"Removed {len(features_layer) - len(features_layer_adjusted)} old features from layer {layer}."
            )
        except Exception as e:
            logger.error(f"Error removing features from layer {layer}: {e}")
            pass
    else:
        logger.warning(f"No features found in layer {layer} to remove.")

    return gdf_HyDAMO


# functions to add
# change attributes based on info from other layers
# change attributes based on given assumption(s)
# change attributes based on DEM?
