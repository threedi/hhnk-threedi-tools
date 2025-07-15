# %%
from pathlib import Path

import hhnk_research_tools as hrt

from hhnk_threedi_tools.core.schematisation_builder.profile_intermediate_converter import ProfileIntermediateConverter
from tests.config import TEMP_DIR, TEST_DIRECTORY


# %%
def test_profile_intermediate_converter():
    """
    Test the ProfileIntermediateConverter class functionality:
    - Load and validate DAMO and CSO layers.
    - Test line merge processing and results.
    - Test profile creation: profielpunt, profiellijn, profielgroep.
    - Create and test linkage between profiles and hydroobjects.
    - Create and test connection of hydroobjects without profiles to nearest profiles on linemerge.
    - Compute deepest points for profiellijnen and hydroobjects.
    - Write results to output GPKG
    - (optional) Convert to HyDAMO format.
    """
    # %%
    damo_file_path = TEST_DIRECTORY / "schema_builder" / "DAMO anna paulowna.gpkg"
    cso_file_path = (
        TEST_DIRECTORY / "schema_builder" / "CSO anna paulowna.gpkg"
    )  # TODO fake data, not extracted from CSO
    temp_dir_out = TEMP_DIR / f"temp_profile_intermediate_converter_{hrt.current_time(date=True)}"
    Path(temp_dir_out).mkdir(parents=True, exist_ok=True)

    self = converter = ProfileIntermediateConverter(damo_file_path=damo_file_path, ods_cso_file_path=cso_file_path)

    # %%
    # Load and validate layers
    converter.load_layers()

    # Check if layers are loaded
    assert converter.data.hydroobject is not None
    assert converter.data.gecombineerde_peilen is not None

    # %%
    # Process line merge
    converter.process_linemerge()

    # Check if line merge result is stored
    assert converter.data.hydroobject_linemerged is not None

    # %%
    # Test variables for profile on primary watergang
    hydroobject_code = "OAF-QJ-14396"
    peilgebied_id = 62149
    profielpunt_id = 12578
    profiellijn_code = 58395
    diepste_punt_profiel = -3.16
    lengte_nat_profiel = 5.54
    diepte_nat_profiel = 1.32
    jaarinwinning = 2012

    # Check for a single hydroobject
    linemerge_id = converter.find_linemerge_id_by_hydroobject_code(hydroobject_code)
    assert linemerge_id is not None
    linemerge = converter.data.hydroobject_linemerged.query("linemergeID == @linemerge_id").iloc[0]
    assert linemerge["categorie"] == "primary"
    peilgebied_id_result = converter.find_peilgebied_id_by_hydroobject_code(hydroobject_code)
    assert peilgebied_id_result == peilgebied_id

    # Create profielgroep, profiellijn and profielpunt
    converter.create_profile_tables()

    # Tests for profile tables
    profielpunt = converter.data.profielpunt
    profiellijn = converter.data.profiellijn
    profielgroep = converter.data.profielgroep
    hydroobject = converter.data.hydroobject

    # Filter profielpunt on id
    pp = profielpunt[profielpunt["id"] == profielpunt_id]
    assert len(pp) == 1

    # Get profiellijnID
    lijn_id = pp.iloc[0]["profielLijnID"]

    # Filter profiellijn on GlobalID
    pl = profiellijn[profiellijn["GlobalID"] == lijn_id]
    assert len(pl) == 1
    assert pl.iloc[0]["code"] == profiellijn_code

    # Get profielgroepID
    groep_id = pl.iloc[0]["profielgroepID"]

    # Filter profielgroep on GlobalID
    pg = profielgroep[profielgroep["GlobalID"] == groep_id]
    assert len(pg) == 1
    assert pg.iloc[0]["code"] == profiellijn_code

    # Check hydroobjectID of profielgroep
    ho = hydroobject[hydroobject["CODE"] == hydroobject_code]
    assert pg["hydroobjectID"].iloc[0] == ho["GlobalID"].iloc[0]

    # Compute the deepest point of profiel
    converter._compute_deepest_point_profiellijn()
    assert converter.data.profiellijn["diepstePunt"].notnull().any()
    assert (
        converter.data.profiellijn[converter.data.profiellijn["code"] == profiellijn_code]["diepstePunt"].iloc[0]
        == diepste_punt_profiel
    )

    # Test variables for profile on primary watergang without profile
    hydroobject_code_no_profile = "OAF-Q-36452"  # near end peilgebied
    nearest_profiellijn_code_it_should_connect_to = 62421
    deepest_point_hydroobject_no_profile = -4.57

    # Check it is not connected to a profile
    pl_2 = converter._find_profiellijn_by_hydroobject_code(hydroobject_code_no_profile)
    assert pl_2 is None

    # Now connect profiles to hydroobject without profiles
    converter.connect_profiles_to_hydroobject_without_profiles(max_distance=250)

    # Check it is connected to a profile now
    pl_2 = converter._find_profiellijn_by_hydroobject_code(hydroobject_code_no_profile)
    assert pl_2 is not None
    assert not pl_2.empty and len(pl_2) == 1
    assert pl_2["code"].iloc[0] == nearest_profiellijn_code_it_should_connect_to

    # Compute the deepest point per hydroobject
    converter.compute_deepest_point_hydroobjects()
    dp = converter.find_deepest_point_by_hydroobject_code(hydroobject_code_no_profile)
    assert dp == deepest_point_hydroobject_no_profile

    # VALIDATION PARAMETER (1): Compute distance wet profile
    converter.compute_distance_and_depth_wet_profile()
    assert (
        converter.data.profiellijn[converter.data.profiellijn["code"] == profiellijn_code]["afstandNatProfiel"].iloc[0]
        == lengte_nat_profiel
    )
    assert (
        converter.data.profiellijn[converter.data.profiellijn["code"] == profiellijn_code]["diepteNatProfiel"].iloc[0]
        == diepte_nat_profiel
    )

    # Compute number of profielpunt features per profiellijn (NOTE: this is also implemented in the validation module)
    """''
    converter.compute_number_of_profielpunt_features_per_profiellijn()
    assert (
        converter.profiellijn[converter.profiellijn["code"] == profiellijn_code]["aantalProfielPunten"].iloc[0]
        == aantal_profielpunt_features
    )
    """

    # REQUIRED FOR VALIDATION: add Z to the profile points
    converter.add_z_to_point_geometry_based_on_column(column_name="hoogte")

    # VALIDATION PARAMETER (2): add breedte value from hydroobject
    converter.add_breedte_value_from_hydroobject()

    # VALIDATION PARAMETER (3): compute jaarinwinning
    converter.compute_jaarinwinning()
    assert (
        converter.data.profiellijn[converter.data.profiellijn["code"] == profiellijn_code]["jaarinwinning"].iloc[0]
        == jaarinwinning
    )

    # VALIDATION PARAMETER (4): compute max cross product profiellijn to check if line is straight
    converter.add_maxcross_to_profiellijn()
    assert (
        converter.data.profiellijn[converter.data.profiellijn["code"] == profiellijn_code]["max_cross_product"].iloc[0]
        is not None
    )

    # VALIDATION PARAMETER (5): check if profielpunt is in ascending order
    converter.compute_if_ascending()
    assert converter.data.profiellijn[converter.data.profiellijn["code"] == 3849]["isAscending"].iloc[0] == 1
    assert converter.data.profiellijn[converter.data.profiellijn["code"] == 58315]["isAscending"].iloc[0] == 0

    # Write the result to a new file
    output_file_path = temp_dir_out / "output.gpkg"
    converter.write_outputs(output_path=output_file_path)

    assert output_file_path.exists()

    # Commented out HyDAMO conversion and validation, as it is not part of this test.
    """
    from hhnk_threedi_tools.core.schematisation_builder.DAMO_HyDAMO_converter import DAMO_to_HyDAMO_Converter

    hydamo_file_path = temp_dir_out / "HyDAMO.gpkg"
    converter_hydamo = DAMO_to_HyDAMO_Converter(
        damo_file_path=output_file_path,
        hydamo_file_path=hydamo_file_path,
        layers=["HYDROOBJECT", "PROFIELPUNT", "PROFIELLIJN", "PROFIELGROEP"],
        overwrite=False,
    )

    converter_hydamo.run()

    # Check if HyDAMO.gpkg is created
    assert hydamo_file_path.exists()

    # Temp test validate HyDAMO as well
    import hhnk_threedi_tools.resources.schematisation_builder as schematisation_builder_resources
    from hhnk_threedi_tools.core.schematisation_builder.HyDAMO_validator import validate_hydamo

    validation_directory_path = TEMP_DIR / f"temp_HyDAMO_validator_{hrt.current_time(date=True)}"
    validation_rules_json_path = hrt.get_pkg_resource_path(schematisation_builder_resources, "validationrules.json")

    test_coverage_location = TEST_DIRECTORY / "schema_builder" / "dtm"  # should hold index.shp

    result_summary = validate_hydamo(
        hydamo_file_path=hydamo_file_path,
        validation_rules_json_path=validation_rules_json_path,
        validation_directory_path=validation_directory_path,
        coverages_dict={"AHN": test_coverage_location},
        output_types=["geopackage", "csv", "geojson"],
    )

    assert result_summary["success"] is True
    assert validation_directory_path.joinpath("datasets", hydamo_file_path.name).exists()
    assert validation_directory_path.joinpath("results", "results.gpkg").exists()

    # Some checks on the validation results
    import geopandas as gpd

    validated_profiellijn = gpd.read_file(
        validation_directory_path.joinpath("results", "results.gpkg"), layer="PROFIELLIJN"
    )
    invalid_profiellijn_code = "64405"
    invalid_profiellijn = validated_profiellijn[validated_profiellijn["code"] == invalid_profiellijn_code].iloc[0]
    assert invalid_profiellijn["invalid"] == "0;100"  # both 0 and 100 are invalid
    """


# %%
if __name__ == "__main__":
    test_profile_intermediate_converter()

# %%
