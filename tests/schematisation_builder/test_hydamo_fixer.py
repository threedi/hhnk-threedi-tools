import json
import shutil
import sys
import tempfile
from pathlib import Path

import fiona
import geopandas as gpd
import hhnk_research_tools as hrt
import pytest

import hhnk_threedi_tools.resources.schematisation_builder as schematisation_builder_resources
from tests.config import TEMP_DIR, TEST_DIRECTORY

LAYERS = ["duikersifonhevel"]

RUN_VALIDATION = False
MANUAL_FIX = False

TEST_FIX_RULE_BREEDTEOPENING = {
    "attribute_name": "breedteopening",
    "validation_ids": [13],
    "fix_id": 5,
    "fix_action": "Derived assumption",
    "fix_type": "automatic",
    "fix_method": {
        "custom_hydamo": {
            "custom_function_name": "if_else",
            "logic": {
                "ISIN": {
                    "parameter": "categorieinwatersysteem",
                    "array": ["primair"],
                }
            },
            "true": {"equal": {"to": 0.8}},
            "false": {"equal": {"to": 0.5}},
        }
    },
    "fix_description": "if duikersifonhevel in primair watersysteem: breedteopening = 0.8m, anders breedteopening = 0.5m",
}


def apply_test_settings_to_validationrules(validation_rules_json_path: Path) -> Path:
    """Read the validation rules JSON and ensure the breedteopening fix rule for
    duikersifonhevel is present and matches the expected definition.

    If the rule is absent it is added and the modified JSON is written to a
    temporary file; that temp path is returned.
    If the rule is present and correct, the original path is returned unchanged.
    If it is present but differs from the expected content, an AssertionError is raised.
    """
    with open(validation_rules_json_path) as f:
        rules = json.load(f)

    duikersifonhevel_obj: dict[str, list] = next(
        (obj for obj in rules["objects"] if obj["object"] == "duikersifonhevel"), None
    )
    assert duikersifonhevel_obj is not None, "duikersifonhevel not found in validation rules"

    fix_rules = duikersifonhevel_obj.get("fix_rules", [])
    existing = next((r for r in fix_rules if r.get("attribute_name") == "breedteopening"), None)

    if existing is None:
        fix_rules.append(TEST_FIX_RULE_BREEDTEOPENING)
        tmp_dir = Path(tempfile.mkdtemp())
        tmp_path = tmp_dir / "validationrules.json"
        with open(tmp_path, "w") as f:
            json.dump(rules, f, indent="\t", ensure_ascii=False)
        return tmp_path
    else:
        assert existing == TEST_FIX_RULE_BREEDTEOPENING, (
            f"breedteopening fix rule does not match expected.\n"
            f"Expected: {TEST_FIX_RULE_BREEDTEOPENING}\nGot: {existing}"
        )
        return Path(validation_rules_json_path)


@pytest.mark.skipif(sys.version_info < (3, 12), reason="Requires Python 3.12 or higher")
def test_hydamo_fixer():
    from hhnk_threedi_tools.core.schematisation_builder.HyDAMO_fixer import fix_hydamo
    from hhnk_threedi_tools.core.schematisation_builder.HyDAMO_validator import validate_hydamo

    hydamo_file_path_validated = TEST_DIRECTORY / "schematisation_builder" / "HyDAMO_validated.gpkg"

    if RUN_VALIDATION:
        validation_directory_path = TEMP_DIR / f"temp_HyDAMO_validator_{hrt.current_time(date=True)}"
        hydamo_file_path = TEST_DIRECTORY / "schematisation_builder" / "HyDAMO.gpkg"
        template_file_path = TEST_DIRECTORY / "schematisation_builder" / "style.gpkg"
        validation_rules_json_path = hrt.get_pkg_resource_path(
            schematisation_builder_resources, "validationrules.json"
        )
        test_coverage_location = TEST_DIRECTORY / "schematisation_builder" / "dtm"  # should hold index.shp

        datamodel, _ = validate_hydamo(
            hydamo_file_path=hydamo_file_path,
            validation_rules_json_path=validation_rules_json_path,
            validation_directory_path=validation_directory_path,
            template_file_path=template_file_path,
            coverages_dict={"AHN": test_coverage_location},
            output_types=["geopackage", "csv", "geojson"],
        )
        datamodel.to_geopackage(validation_directory_path / "HyDAMO_validated.gpkg", use_schema=False)
        shutil.copy2(validation_directory_path / "HyDAMO_validated.gpkg", hydamo_file_path_validated)
        shutil.copy2(
            validation_directory_path / "results" / "results.gpkg",
            TEST_DIRECTORY / "schematisation_builder" / "results.gpkg",
        )

    hydamo_file_path = TEST_DIRECTORY / "schematisation_builder" / "HyDAMO_validated.gpkg"
    validation_rules_json_path = apply_test_settings_to_validationrules(
        hrt.get_pkg_resource_path(schematisation_builder_resources, "validationrules.json")
    )
    results_gpkg_path = TEST_DIRECTORY / "schematisation_builder" / "results.gpkg"
    fix_directory_path = TEMP_DIR / f"temp_hydamo_fixer_{hrt.current_time(date=True)}"

    test_coverage_location = TEST_DIRECTORY / "schematisation_builder" / "dtm"  # should hold index.shp
    coverages_dict = {"AHN": test_coverage_location}

    datamodel, layer_summary, result_summary = fix_hydamo(
        hydamo_file_path=hydamo_file_path,
        validation_rules_json_path=validation_rules_json_path,
        results_gpkg_path=results_gpkg_path,
        fix_directory_path=fix_directory_path,
        coverages_dict=coverages_dict,
        output_types=["geopackage"],
    )

    # assert
    hydamo_fix_review_path = fix_directory_path / "review" / "fix_summary.gpkg"
    assert hydamo_fix_review_path.exists()

    hydamo_fix_log_path = fix_directory_path / "results" / "fixer.log"
    assert hydamo_fix_log_path.exists()

    hydamo_fix_results_path = fix_directory_path / "results" / "fix_result.json"
    assert hydamo_fix_results_path.exists()

    # check if expected layers are in report gpkg
    fix_layers = fiona.listlayers(hydamo_fix_review_path)
    expected_layers = LAYERS
    for layer in expected_layers:
        assert layer in fix_layers

    # check if validation rule 5 of duikersifonhevel is fixed correctly
    # open hydamo_fixed_gpkg_path and check if column breedteopening is added and if value of breedteopening is correct based on fix message in review gpkg
    gdf_review_duikersifonhevel = gpd.read_file(hydamo_fix_review_path, layer="duikersifonhevel")

    assert "breedteopening" in gdf_review_duikersifonhevel.columns
    assert "fixes_breedteopening" in gdf_review_duikersifonhevel.columns

    # specific check for feature with id 14 which should have fix applied based on validation rule 5
    assert (
        "if duikersifonhevel in primair watersysteem: breedteopening = 0.8m, anders breedteopening = 0.5m"
        in gdf_review_duikersifonhevel["fixes_breedteopening"][13]
    )
    assert gdf_review_duikersifonhevel["breedteopening"][13] == 0.8

    # Check if manual fix is applied correctly. Set variable MANUAL_FIX to True to apply this check
    assert "manual_overwrite_breedteopening" in gdf_review_duikersifonhevel.columns
    if MANUAL_FIX:
        # NOTE: If command prompt ask you for input, fill in 0.6 for feature with id 2 in column manual_overwrite_breedteopening
        # check if manual overwrite value is applied correctly for feature with id 2
        assert gdf_review_duikersifonhevel["breedteopening"][1] == 0.6

    # Check if is_usable column is added
    assert "is_usable" in gdf_review_duikersifonhevel.columns
    # NOTE: if code includes function to set features to unusable if topological fix is required, check if value in is_usable column is correct for one of the features of which you are sure that it is unusable or usable

    # Check breedteopening values in HyDAMO_fixed against the fix rule logic
    hydamo_fixed_path = fix_directory_path / "results" / "HyDAMO_fixed.gpkg"
    assert hydamo_fixed_path.exists()
    gdf_fixed_duikersifonhevel = gpd.read_file(hydamo_fixed_path, layer="duikersifonhevel")
    assert "breedteopening" in gdf_fixed_duikersifonhevel.columns
    assert "categorieinwatersysteem" in gdf_fixed_duikersifonhevel.columns

    fixed_idx = gdf_review_duikersifonhevel.index[
        gdf_review_duikersifonhevel["fixes_breedteopening"].str.strip().str.len() > 0
    ]
    assert len(fixed_idx) > 0, "Expected at least one feature with breedteopening fix applied"
    assert gdf_fixed_duikersifonhevel.loc[fixed_idx, "breedteopening"].notna().all(), (
        "Some fixed features have a null breedteopening in HyDAMO_fixed"
    )
    for idx in fixed_idx:
        categorie = gdf_fixed_duikersifonhevel.loc[idx, "categorieinwatersysteem"]
        expected_breedte = 0.8 if categorie == "primair" else 0.5
        assert gdf_fixed_duikersifonhevel.loc[idx, "breedteopening"] == expected_breedte, (
            f"Feature {idx}: expected breedteopening={expected_breedte} "
            f"(categorieinwatersysteem={categorie!r}), "
            f"got {gdf_fixed_duikersifonhevel.loc[idx, 'breedteopening']}"
        )


if __name__ == "__main__":
    test_hydamo_fixer()
