import geopandas as gpd


def remove_features(gdf_HyDAMO: "gpd.GeoDataFrame", layer: str, list_features: list, logger) -> "gpd.GeoDataFrame":
    """Remove old profiles from the HyDAMO geodataframe.

    Args:
        gdf_HyDAMO (gpd.GeoDataFrame): HyDAMO geodataframe.
        layer (str): The layer from which to remove old profiles.
        list_old_profiles (list): List of old profile IDs to remove.

    Returns:
        gpd.GeoDataFrame: HyDAMO geodataframe without the specified features.
    """
    features_layer = gdf_HyDAMO[gdf_HyDAMO["layer"] == layer]
    if not features_layer.empty:
        features_layer_adjusted = features_layer[~features_layer["code"].isin(list_features)]
        gdf_HyDAMO.update(features_layer_adjusted)
        logger.info(f"Removed {len(features_layer) - len(features_layer_adjusted)} old features from layer {layer}.")
    else:
        logger.warning(f"No features found in layer {layer} to remove.")

    return gdf_HyDAMO
