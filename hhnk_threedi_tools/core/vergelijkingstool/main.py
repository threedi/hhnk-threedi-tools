#!/usr/bin/env python
r"""Module for the comparison of DAMO/HDB data and 3Di model data.
With this module the actuality of 3Di models can be assessed.
There are two main usages within this module:
    1. Compare current DAMO/HDB data with the DAMO/HDB data that was used to build the model.
    This gives an indication in how much the water system has changed
    2. Compare the 3Di model with (current) DAMO/HDB data.
    This gives an indication in how much the model differs from the (current) situation.
    Possibly the model was updated in the years together with the DAMO/HDB or the datachecker/modelbuilder induced
    differences
"""

import logging
import warnings

import geopandas as gpd
from hhnk_threedi_tools.core.vergelijkingstool.DAMO import DAMO
from hhnk_threedi_tools.core.vergelijkingstool.Threedimodel import Threedimodel

from hhnk_threedi_tools.core.vergelijkingstool.utils import ModelInfo, get_model_info


def main(
    model_info: ModelInfo,
    fn_DAMO_selection,
    fn_damo_new,
    fn_hdb_new,
    fn_damo_old,
    fn_hdb_old,
    fn_threedimodel,
    fn_DAMO_comparison_export,
    fn_threedi_comparison_export,
    compare_with: str = "Damo Updated vs 3Di model",
    layer_selection=True,
    layers_input_hdb_selection=None,
    layers_input_damo_selection=None,
    threedi_layer_selector=False,
    structure_codes=None,
):
    # fn_damo_new_translation
    # Set logging level
    logging.basicConfig(level=logging.INFO)

    # Supress fiona logging to keep logging readable
    for log_name, log_obj in logging.Logger.manager.loggerDict.items():
        if "fiona" in log_name:
            # log_obj may be a logging.PlaceHolder; only disable actual Logger instances
            if isinstance(log_obj, logging.Logger):
                log_obj.disabled = True

    # Supress GeoSeries.notna warning, as it warns about a changed operator. Currently using the new operator.
    warnings.filterwarnings("ignore", "GeoSeries.notna", UserWarning)
    if not fn_DAMO_selection.exists():
        # change to gpkg
        fn_shp = fn_DAMO_selection.with_suffix(".shp")
        gdf = gpd.read_file(fn_shp, engine="pyogrio")
        gdf.to_file(fn_DAMO_selection, driver="GPKG")

    gdf_selection = gpd.read_file(fn_DAMO_selection, engine="pyogrio")
    gdf_selection["geometry"] = gdf_selection.geometry.buffer(300)
    selection_shape = gdf_selection.union_all()

    # Create two damo_objects, supply with DAMO-file, HDB-file and optionally a translation_DAMO, translation_HDB or a
    # clip_shape

    selection_compare = compare_with

    if selection_compare == "Damo Export vs Damo Updated":
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
        print("doing only damo")
        # Compare damo objects with eachother and export result to geopackage
        DAMO_comparison, DAMO_statistics = damo_new.compare_with_damo(
            damo_old,
            filename=fn_DAMO_comparison_export,
            overwrite=True,
        )

        return fn_DAMO_comparison_export

    elif selection_compare == "Damo Updated vs 3Di model":
        # Create Threedimodel object
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

        threedi_comparison, threedi_statistics = threedi_model.compare_with_DAMO(
            damo_new,
            # attribute_comparison=fn_model_attribute_comparison,
            filename=fn_threedi_comparison_export,
            overwrite=True,
            threedi_layer_selector=threedi_layer_selector,
            structure_codes=structure_codes,
        )

        # return(fn_DAMO_comparison_export)
        return fn_threedi_comparison_export

    elif selection_compare == "Both":
        threedi_model = Threedimodel(fn_threedimodel, model_info=model_info)
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
        DAMO_comparison, DAMO_statistics = damo_new.compare_with_damo(
            damo_old,
            filename=fn_DAMO_comparison_export,
            overwrite=True,
        )
        threedi_comparison, threedi_statistics = threedi_model.compare_with_DAMO(
            damo_new,
            # attribute_comparison=fn_model_attribute_comparison,
            filename=fn_threedi_comparison_export,
            overwrite=True,
            threedi_layer_selector=threedi_layer_selector,
            structure_codes=structure_codes,
        )
        return (fn_DAMO_comparison_export, fn_threedi_comparison_export)

    else:
        print("You must select and option")


# %%

if __name__ == "__main__":
    # name
    path = r"H:\02.modellen\grootslag_leggertool"
    model_info = get_model_info(path)
    source_data = model_info.source_data

    fn_threedimodel = model_info.fn_threedimodel
    # Base folder initial files.
    input_data_new = model_info.input_data_new

    # output location.
    out_put_files = model_info.output_folder

    json_file = model_info.json_folder
    # Old DAMO (DCMB/FME export) location .
    fn_damo_old = model_info.fn_damo_old

    # the last version
    fn_damo_new = model_info.fn_damo_new

    # Old HDB (DCMB/FME export) location .
    fn_hdb_old = model_info.fn_hdb_old

    # the last version
    fn_hdb_new = model_info.fn_hdb_new

    # get polder polygon
    fn_DAMO_selection = model_info.damo_selection

    # Define outputs
    fn_DAMO_comparison_export = out_put_files / "DAMO_test_v2.gpkg"

    # Layers To Compare DAMO_DAMO
    layer_selection = False
    layers_input_damo_selection = ["AfvoergebiedAanvoergebied", "PeilafwijkingGebied", "PeilgebiedPraktijk"]
    layers_input_hdb_selection = ["Levee_overstromingsmodel", "Sturing_3Di"]
    fn_threedi_comparison_export = out_put_files / "3DI_v5.gpkg"
    # compare_with = "Damo Export vs Damo Updated"
    compare_with = "Damo Updated vs 3Di model"
    # config.UPDATE_SYMBOLOGY = True

    # Layers to Compare DAMO_3di
    threedi_layer_selector = False
    structure_codes = ["KST"]
    # %%
    main(
        model_info=model_info,
        fn_DAMO_selection=fn_DAMO_selection,
        fn_damo_new=fn_damo_new,
        fn_hdb_new=fn_hdb_new,
        fn_damo_old=fn_damo_old,
        fn_hdb_old=fn_hdb_old,
        fn_threedimodel=fn_threedimodel,
        fn_DAMO_comparison_export=fn_DAMO_comparison_export,
        fn_threedi_comparison_export=fn_threedi_comparison_export,
        compare_with=compare_with,
        layer_selection=layer_selection,
        layers_input_hdb_selection=layers_input_hdb_selection,
        layers_input_damo_selection=layers_input_damo_selection,
        threedi_layer_selector=threedi_layer_selector,
        structure_codes=structure_codes,
    )
# %%
