import geopandas as gpd


# Example function
# TODO: list_features could also be summary validation/fix dataframe with codes to remove.
def remove_features(gdf_HyDAMO: "gpd.GeoDataFrame", layer: str, list_features: list, logger) -> "gpd.GeoDataFrame":
    """Remove features from the HyDAMO geodataframe.

    Args:
        gdf_HyDAMO (gpd.GeoDataFrame): HyDAMO geodataframe.
        layer (str): The layer in which features need to be asingned as not usable.
        list_features (list): List of old feature IDs to remove.

    Returns:
        gpd.GeoDataFrame: HyDAMO geodataframe with extra column 'fix_not_useable' indicating features which arenot usable.
    """
    features_layer = gdf_HyDAMO[gdf_HyDAMO["layer"] == layer]
    if not features_layer.empty:
        try:
            # features_layer_adjusted = features_layer[~features_layer["code"].isin(list_features)]

            # add column 'fix_not_useable' to indicate features and set true for features in list_features
            features_layer_adjusted = features_layer.copy()
            features_layer_adjusted["fix_not_useable"] = False
            features_layer_adjusted.loc[features_layer_adjusted["code"].isin(list_features), "fix_not_useable"] = True

            gdf_HyDAMO.update(features_layer_adjusted)
            logger.info(f"Indicated {len(list_features)} features as not usable in layer {layer}.")
        except Exception as e:
            logger.error(f"Error indicating features as not usable in layer {layer}: {e}")
            pass
    else:
        logger.warning(f"No features found in layer {layer} which are not usable.")

    return gdf_HyDAMO


# functions to add
# change attributes based on info from other layers
# change attributes based on given assumption(s)
# change attributes based on DEM?
