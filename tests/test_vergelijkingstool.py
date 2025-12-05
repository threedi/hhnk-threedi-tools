# %%
import json
import os
import shutil
import sys
from pathlib import Path
from typing import Tuple

import fiona
import geopandas as gpd
import numpy as np
import pandas as pd
import pytest

from hhnk_threedi_tools.core.vergelijkingstool import styling, utils
from hhnk_threedi_tools.core.vergelijkingstool.DAMO import DAMO
from hhnk_threedi_tools.core.vergelijkingstool.Threedimodel import Threedimodel
from hhnk_threedi_tools.core.vergelijkingstool.utils import ModelInfo, get_model_info
from tests.config import FOLDER_TEST

# model_info = get_model_info(FOLDER_TEST)


# %%
@pytest.fixture(scope="session")
def model_info() -> ModelInfo:
    """Return a ModelInfo instance for the test session.

    Uses tests.config.FOLDER_TEST to locate test data and build the ModelInfo.
    """
    return get_model_info(FOLDER_TEST)


@pytest.fixture(scope="session")
def damos(model_info: ModelInfo) -> Tuple[DAMO, DAMO]:
    """Initialize and return a tuple of (damo_old, damo_new).

    Both DAMO instances are created with the session model_info and same clip selection.
    """
    damo_old = DAMO(model_info, model_info.fn_damo_old, model_info.fn_hdb_old, clip_shape=model_info.damo_selection)
    damo_new = DAMO(model_info, model_info.fn_damo_new, model_info.fn_hdb_new, clip_shape=model_info.damo_selection)
    return damo_old, damo_new


@pytest.mark.skipif(sys.version_info < (3, 12), reason="Requires Python 3.12 or higher")
def test_model_info(model_info: ModelInfo) -> None:
    """Verify ModelInfo fields point to expected test files and types."""
    assert isinstance(model_info, ModelInfo)
    assert model_info.model_name == "model_test"
    assert model_info.source_data.exists()
    assert model_info.fn_damo_new.exists()
    assert model_info.fn_damo_old.exists()
    assert model_info.fn_hdb_new.exists()
    assert model_info.fn_hdb_old.exists()
    assert isinstance(model_info.json_folder, Path)


@pytest.mark.skipif(sys.version_info < (3, 12), reason="Requires Python 3.12 or higher")
def test_compare_with_damo(model_info: ModelInfo, damos: Tuple[DAMO, DAMO]) -> None:
    """Run compare_with_damo and assert expected output layers and counts.

    This function writes a GPKG file and then inspects layers 'stuw' and 'gemaal
    and test de function compare_with_damo from the DAMO class'.
    """
    # unpack damos tuple
    damo_old, damo_new = damos

    # set output filename
    filename = model_info.output_folder / "DAMO_comparison.gpkg"

    # run compare and write gpkg
    damo_new.compare_with_damo(damo_old, filename=filename, overwrite=True)

    # read generated layers
    stuw = gpd.read_file(filename, layer="stuw")
    gemaal = gpd.read_file(filename, layer="gemaal")

    # basic assertions on file and stats
    assert filename.exists()
    assert "statistics" in fiona.listlayers(str(filename))
    assert np.sum(stuw["number_of_critical"]) == 3
    assert np.sum(gemaal["number_of_critical"]) == 2


@pytest.mark.skipif(sys.version_info < (3, 12), reason="Requires Python 3.12 or higher")
def test_compare_with_threedi(model_info: ModelInfo, damos) -> None:
    """Compare a Threedimodel with DAMO and check KDU/KST warning counts.
    This function writes a GPKG file and then inspects layers 'stuw' and 'gemaal
    and test de function compare_with_damo from the Threedimodel class.
    """

    # unpack damos tuple
    damo_old, damo_new = damos

    # create Threedimodel instance for testing
    threedi_model = Threedimodel(model_info.fn_threedimodel, model_info=model_info)

    # set output filename
    filename = model_info.output_folder / "threedi_comparison.gpkg"

    # run the comparison and write gpkg
    threedi_model.compare_with_DAMO(
        damo_new,
        filename=filename,
        overwrite=True,
    )

    # read produced layers
    kdu = gpd.read_file(filename, layer="KDU")
    kst = gpd.read_file(filename, layer="KST")

    # assert expected warning counts
    assert filename.exists()
    assert "statistics" in fiona.listlayers(str(filename))
    assert np.sum(kdu["number_of_warning"]) == 26
    assert np.sum(kst["number_of_warning"]) == 3


@pytest.mark.skipif(sys.version_info < (3, 12), reason="Requires Python 3.12 or higher")
def test_compare_with_damo_builds_tableC_and_stats(damos: Tuple[DAMO, DAMO]) -> None:
    """Ensure compare_with_damo returns table_C and stats when no filename is provided.

    This variant calls compare_with_damo with filename=None to keep results in memory.
    """
    # unpack damos tuple
    damo_old, damo_new = damos

    # run compare with damo without filename to get in-memory results
    table_C, stats = damo_new.compare_with_damo(damo_old, filename=None, overwrite=True)

    # assert expected types
    assert isinstance(table_C, dict)
    assert isinstance(stats, pd.DataFrame)


@pytest.mark.skipif(sys.version_info < (3, 12), reason="Requires Python 3.12 or higher")
def test_compare_attribute_priority_function_name_change(damos: Tuple[DAMO, DAMO], model_info: ModelInfo) -> None:
    """Test compare_attribute by changing name of the fucntion in the json by test to test error"""

    # get damos objects
    damo_old, damo_new = damos

    # set and load json file
    json_folder = model_info.json_folder / "damo_attribute_comparison.json"
    with open(json_folder) as file:
        att_comp = json.load(file)
        comparison = att_comp["comparisons"][108]

    # change priority for function difference to force error
    comparison["function"]["test"] = comparison["function"].pop("difference")
    function = comparison["function"]

    # Get Table C to compare later the attribute.
    DAMO_comparison, _ = damo_new.compare_with_damo(damo_old, filename=None, overwrite=True)

    # select stuw from the dictionary
    stuw = DAMO_comparison["stuw"]

    # run compare_function and assert it returns None for unknown function
    result, change_na = damo_new.compare_function(stuw, function)
    assert result is None


@pytest.mark.skipif(sys.version_info < (3, 12), reason="Requires Python 3.12 or higher")
def test_compare_attribute_priority_function_add(damos: Tuple[DAMO, DAMO], model_info: ModelInfo) -> None:
    """Test compare_attribute by testing the function add"""

    # get damos objects
    damo_old, damo_new = damos
    # set and load json file
    json_folder = model_info.json_folder / "damo_attribute_comparison.json"
    with open(json_folder) as file:
        att_comp = json.load(file)
        comparison = att_comp["comparisons"][108]

    # change priority for function difference to add
    comparison["function"]["add"] = comparison["function"].pop("difference")
    function = comparison["function"]

    # Get Table C to compare later the attribute.
    DAMO_comparison, _ = damo_new.compare_with_damo(damo_old, filename=None, overwrite=True)
    # select the table stuw from the dictionary
    stuw = DAMO_comparison["stuw"]
    # compare function
    result, change_na = damo_new.compare_function(stuw, function)

    # Assert Results
    assert np.sum(result) == -32.71


@pytest.mark.skipif(sys.version_info < (3, 12), reason="Requires Python 3.12 or higher")
def test_compare_attribute_priority_function_minimum(damos: Tuple[DAMO, DAMO], model_info: ModelInfo) -> None:
    """Test compare_attribute by testing the function minimum"""
    # get damos objects
    damo_old, damo_new = damos
    # set and load json file
    json_folder = model_info.json_folder / "damo_attribute_comparison.json"
    with open(json_folder) as file:
        att_comp = json.load(file)
        comparison = att_comp["comparisons"][108]

    # change priority for function difference to minimum
    comparison["function"]["minimum"] = comparison["function"].pop("difference")
    # select function minimum
    function = comparison["function"]
    # Get Table C to compare later the attribute.
    DAMO_comparison, _ = damo_new.compare_with_damo(damo_old)
    # select the table stuw from the dictionary
    stuw = DAMO_comparison["stuw"]
    # compare function
    result, change_na = damo_new.compare_function(stuw, function)
    # Assert Results
    assert np.sum(result) == -16.910000000000004


@pytest.mark.skipif(sys.version_info < (3, 12), reason="Requires Python 3.12 or higher")
def test_apply_attribute_comparison(
    damos: Tuple[DAMO, DAMO], model_info: ModelInfo, caplog: pytest.LogCaptureFixture
) -> None:
    """Test apply_attribute_comparison logs an Error when the json file is not found"""
    # get damos objects
    damo_old, damo_new = damos
    # set and load json file
    json_folder = model_info.json_folder / "damo_attribute_comparison_test.json"

    # get Table C
    DAMO_comparison, _ = damo_new.compare_with_damo(damo_old)

    # use caplog to catch error messages.
    with caplog.at_level("ERROR"):
        result = damo_new.apply_attribute_comparison(json_folder, DAMO_comparison)

    # assert that the error message was logged
    assert any("Unable to load attribute comparison file. File" in message for message in caplog.messages)


@pytest.mark.skipif(sys.version_info < (3, 12), reason="Requires Python 3.12 or higher")
def test_update_channel_codes(model_info: ModelInfo, tmp_path) -> None:
    """Test channel codes updates in case they have not been updated"""

    dst = tmp_path / "test_update_channel_codes.gpkg"
    # get damo path
    shutil.copy(model_info.fn_threedimodel, dst)
    damo_path = model_info.fn_damo_new
    # read channel grom geockg
    channel = gpd.read_file(dst, layer="channel")
    # change code to test
    channel["code"] = "TEST"
    # get model path and read cross_section_location
    model_path = model_info.fn_threedimodel

    cross_section_location = gpd.read_file(model_path, layer="cross_section_location")
    cross_section_location_copy = cross_section_location.copy()

    # run the function a do the assert.
    gdf = utils.update_channel_codes(channel, cross_section_location_copy, damo_path, dst)
    assert (gdf["code"] != "TEST").any()


@pytest.mark.skipif(sys.version_info < (3, 12), reason="Requires Python 3.12 or higher")
def test_threedi_crs_iquals(
    model_info: ModelInfo,
    tmp_path: Path,
) -> None:
    """Test compare with damo to catch the case where the projection sytem is different"""
    # get damo object
    damo_new = DAMO(model_info, model_info.fn_damo_new, model_info.fn_hdb_new, clip_shape=model_info.damo_selection)
    # get 3di object
    threedi_model = Threedimodel(model_info.fn_threedimodel, model_info=model_info)

    # Change pump layer CRS to WGS84 and write to temp gpkg

    temp_gpkg = tmp_path / "temp_test.gpkg"
    for layer_name in threedi_model.data.keys():
        if isinstance(threedi_model.data[layer_name], gpd.GeoDataFrame):
            threedi_model.data[layer_name] = threedi_model.data[layer_name].to_crs(epsg=4326)
            threedi_model.data[layer_name].to_file(temp_gpkg, layer=layer_name, driver="GPKG")
    # create new 3di object with the temp file and compare with damo
    threedi_model = Threedimodel(temp_gpkg, model_info=model_info)
    resutl, statict = threedi_model.compare_with_DAMO(
        damo_new,
        filename=None,
        overwrite=True,
    )

    # verify expected layer exists and CRS is Amersfoort
    assert "KDU" in resutl.keys()
    assert resutl["KDU"].crs.source_crs.name == "Amersfoort"


@pytest.mark.skipif(sys.version_info < (3, 12), reason="Requires Python 3.12 or higher")
def test_translate_invalid_json(damos: Tuple[DAMO, DAMO], tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    """Test invalid json"""
    damo_new, damo_old = damos

    # Create a temporary file with invalid JSON content
    bad_json_file = tmp_path / "bad.json"
    bad_json_file.write_text('{"name": "cmp_laagstedoorstroomhoogte", "table": "stuw"')  # malformed JSON
    # get Table C
    DAMO_comparison, _ = damo_new.compare_with_damo(damo_old)

    # use caplog to catch error messages.
    with caplog.at_level("ERROR"):
        result = damo_new.apply_attribute_comparison(bad_json_file, DAMO_comparison)

    # use caplog to catch error messages.
    with caplog.at_level("ERROR"):
        with pytest.raises(json.decoder.JSONDecodeError):
            utils.translate(damo_new.data, bad_json_file)
    # Assert that the error message was logged
    assert any(
        "Unable to load attribute comparison file. JSON structure incorrect." in message for message in caplog.messages
    )
    # Assert that the error message from translate was logged
    assert any("Structure of DAMO-translation file is incorrect" in message for message in caplog.messages)


@pytest.mark.skipif(sys.version_info < (3, 12), reason="Requires Python 3.12 or higher")
def test_pprepare_layers_for_export(damos: Tuple[DAMO, DAMO], model_info: ModelInfo) -> None:
    """Test specific layer selection"""

    # get all damo variable to test function load_file_and_translate using select layer as true
    damo_filename = model_info.fn_damo_new
    hdb_filename = model_info.fn_hdb_new
    threedi_filename = model_info.fn_threedimodel
    translation_DAMO = model_info.json_folder / "damo_translation.json"
    translation_HDB = model_info.json_folder / "hdb_translation.json"
    translation_3Di = model_info.json_folder / "hdb_translation.json"
    layer_selection = True
    layers_input_damo_selection = ["AfvoergebiedAanvoergebied", "PeilafwijkingGebied", "PeilgebiedPraktijk"]
    layers_input_hdb_selection = ["poldercluster"]
    layers_input_threedi_selection = None
    mode = "damo"

    # run the utility and assert expected presence/absence of layers
    data = utils.load_file_and_translate(
        damo_filename=damo_filename,
        hdb_filename=hdb_filename,
        threedi_filename=threedi_filename,
        translation_DAMO=translation_DAMO,
        translation_HDB=translation_HDB,
        translation_3Di=translation_3Di,
        layer_selection=layer_selection,
        layers_input_damo_selection=layers_input_damo_selection,
        layers_input_hdb_selection=layers_input_hdb_selection,
        layers_input_threedi_selection=layers_input_threedi_selection,
        mode=mode,
    )
    # assert that the layer is in the dictionary
    assert "peilafwijkinggebied" in data.keys()
    assert "gemaal" not in data.keys()


@pytest.mark.skipif(sys.version_info < (3, 12), reason="Requires Python 3.12 or higher")
def test_prepare_layers_for_export(damos: Tuple[DAMO, DAMO], model_info: ModelInfo) -> None:
    """Test prepare_layers_for_export raises FileExistsError when overwrite is False and file exists."""
    # unpack damos
    damo_old, damo_new = damos
    # set output filename
    filename = model_info.output_folder / "DAMO_comparison.gpkg"
    # run compare to get table_C
    table_C, stats = damo_new.compare_with_damo(damo_old, filename=None, overwrite=False)

    # Expect FileExistsError when overwrite=False
    with pytest.raises(FileExistsError) as excinfo:
        # run fucntion to test error message
        styling.prepare_layers_for_export(table_C, filename=filename, overwrite=False)

    # assert check the error message
    assert f'The file "{filename}" already exists' in str(excinfo.value)


@pytest.mark.skipif(sys.version_info < (3, 12), reason="Requires Python 3.12 or higher")
def test_clip_no_gdf(damos: Tuple[DAMO, DAMO], model_info: ModelInfo, caplog: pytest.LogCaptureFixture) -> None:
    """Test prepare_layers_for_export raises FileExistsError when overwrite is False and file exists."""
    # unpack damos
    damo_old, damo_new = damos
    # set output filename
    damo_new.data["non_geo_layer"] = pd.DataFrame({"id": [1, 2]})
    clip_path = model_info.damo_selection
    clip_gdf = gpd.read_file(clip_path)

    with caplog.at_level("DEBUG"):
        result = damo_new.clip_data(damo_new.data, shape=clip_gdf)
    assert "non_geo_layer" not in result.keys()
    assert any("Layer 'non_geo_layer' is not a GeoDataFrame" in message for message in caplog.messages)


@pytest.mark.skipif(sys.version_info < (3, 12), reason="Requires Python 3.12 or higher")
def test_shape_5(
    damos: Tuple[DAMO, DAMO], model_info: ModelInfo, caplog: pytest.LogCaptureFixture, tmp_path: Path
) -> None:
    """Test prepare_layers_for_export raises FileExistsError when overwrite is False and file exists."""

    # create Threedimodel instance for testing
    threedi_model = Threedimodel(model_info.fn_threedimodel, model_info=model_info)
    # modify cross_section_location to have shape 5
    cross_section_location = threedi_model.data["cross_section_location"]
    cross_section_location.loc[cross_section_location["code"] == "49918", "cross_section_shape"] = 5
    cross_section_location.loc[cross_section_location["code"] == "49918", "cross_section_table"] = None
    cross_section_location.loc[cross_section_location["code"] == "49918", "cross_section_width"] = 1
    cross_section_location.loc[cross_section_location["code"] == "49918", "crest_level"] = 1

    # create temp gpkg to save modified cross_section_location
    temp_gpkg = tmp_path / "temp.gpkg"

    for layer_name in threedi_model.data.keys():
        if isinstance(threedi_model.data[layer_name], gpd.GeoDataFrame):
            threedi_model.data[layer_name].to_file(temp_gpkg, layer=layer_name, driver="GPKG")
    # create new Threedimodel instance with temp gpkg and run max_value_from_tabulated
    threedi_model_shape_5 = Threedimodel(temp_gpkg, model_info=model_info)
    threedi_model_shape_5.max_value_from_tabulated()
    assert (
        threedi_model_shape_5.data["cross_section_location"]
        .loc[threedi_model_shape_5.data["cross_section_location"]["code"] == "49918", "cross_section_max_width"]
        .values[0]
        == 1
    )


@pytest.mark.skipif(sys.version_info < (3, 12), reason="Requires Python 3.12 or higher")
def test_empty_table_c() -> None:
    """Test that prepare_layers_for_export handles empty Table C gracefully."""
    # build empty table C
    empty_gdf = gpd.GeoDataFrame(columns=["number_of_critical", "in_both", "geometry"])
    table_C = {"layer1": empty_gdf}

    # Run the function to test
    result = utils.build_summary_layers(table_C)

    # Assert that the file was None
    assert not result


# %%
