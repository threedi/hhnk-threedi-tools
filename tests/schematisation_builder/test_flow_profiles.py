# %%
import sys
from pathlib import Path

import geopandas as gpd
import hhnk_research_tools as hrt
import pytest

import hhnk_threedi_tools.resources.schematisation_builder as schematisation_builder_resources
from tests.config import TEMP_DIR, TEST_DIRECTORY


# %%
# TODO remove skip when py312 implemented.
@pytest.mark.skipif(sys.version_info < (3, 12), reason="Requires Python 3.12 or higher")
def test_profile_intermediate_converter():
    from hhnk_threedi_tools.core.schematisation_builder.DAMO_HyDAMO_converter import DAMO_to_HyDAMO_Converter
    from hhnk_threedi_tools.core.schematisation_builder.HyDAMO_validator import validate_hydamo
    from hhnk_threedi_tools.core.schematisation_builder.intermediate_converter import (
        ProfileIntermediateConverter,
    )

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
    logger = hrt.logging.get_logger(__name__)

    raw_export_file_path = TEST_DIRECTORY / "schematisation_builder" / "raw_export.gpkg"
    temp_dir_out = TEMP_DIR / f"temp_profile_intermediate_converter_{hrt.current_time(date=True)}"
    Path(temp_dir_out).mkdir(parents=True, exist_ok=True)

    converter = ProfileIntermediateConverter(raw_export_file_path=raw_export_file_path, logger=logger)

    # Load and validate layers
    converter.load_layers()  # STEP 1 in run method

    # Check if layers are loaded
    assert converter.data.hydroobject is not None
    assert converter.data.gecombineerde_peilen is not None

    # Process line merge
    converter.process_linemerge()  # STEP 2 in run method

    # Check if line merge result is stored
    assert converter.data.hydroobject_linemerged is not None

    # Test variables for profile on primary watergang
    hydroobject_code = "OAF-Q-35669"  #
    peilgebied_id = 49637  #
    profielpunt_code = 1706671  #
    profiellijn_code = 49937  #
    diepste_punt_profiel = -1.51  #
    lengte_nat_profiel = 9.07 - 4.83  #
    hydroobject_breedte = 4.03  #
    breedte_profiel = 13.56  #
    nr_of_profielpunten = 15  #
    diepte_nat_profiel = -0.52 - diepste_punt_profiel  #
    max_hoogte_profiel = 0.21  #
    jaarinwinning = 2013  #
    max_cross_product = 0.0029  # TODO

    # Test variables for profile on primary watergang without profile
    hydroobject_code_no_profile_should_connect = "OAF-J-2354"  #
    nearest_profiellijn_code_it_should_connect_to = 49930  #
    deepest_point_hydroobject_no_profile = -1.35  #
    hydroobject_code_no_profile_should_not_connect = "OAF-Q-36736"  #

    # Test variables for profileline ascending and descending
    profiellijn_code_ascending = 49939  # TODO
    profiellijn_code_descending = 49939  # TODO

    # Check for a single hydroobject
    linemerge_id = converter.find_linemerge_id_by_hydroobject_code(hydroobject_code)
    assert linemerge_id is not None
    linemerge = converter.data.hydroobject_linemerged.query("linemergeid == @linemerge_id").iloc[0]
    assert linemerge["categorie"] == "primary"
    peilgebied_id_result = converter.find_peilgebied_id_by_hydroobject_code(hydroobject_code)
    assert peilgebied_id_result == peilgebied_id

    # Create profielgroep, profiellijn and profielpunt
    converter.create_profile_tables()  # STEP 3 in run method

    # Tests for profile tables
    profielpunt = converter.data.profielpunt
    profiellijn = converter.data.profiellijn
    profielgroep = converter.data.profielgroep
    hydroobject = converter.data.hydroobject

    # Filter profielpunt on code
    pp = profielpunt[profielpunt["code"] == profielpunt_code]
    assert len(pp) == 1

    # Get profiellijnID
    lijn_id = pp.iloc[0]["profiellijnid"]

    # Filter profiellijn on globalid
    pl = profiellijn[profiellijn["globalid"] == lijn_id]
    assert len(pl) == 1
    assert pl.iloc[0]["code"] == profiellijn_code

    # Get profielgroepid
    groep_id = pl.iloc[0]["profielgroepid"]

    # Filter profielgroep on globalid
    pg = profielgroep[profielgroep["globalid"] == groep_id]
    assert len(pg) == 1
    assert pg.iloc[0]["code"] == profiellijn_code

    # Check hydroobjectid of profielgroep
    ho = hydroobject[hydroobject["code"] == hydroobject_code]
    assert pg["hydroobjectid"].iloc[0] == ho["globalid"].iloc[0]

    # Compute the deepest point of profiel
    converter._compute_deepest_point_profiellijn()
    assert converter.data.profiellijn["diepstepunt"].notnull().any()
    assert (
        converter.data.profiellijn[converter.data.profiellijn["code"] == profiellijn_code]["diepstepunt"].iloc[0]
        == diepste_punt_profiel
    )

    # Check it is not connected to a profile
    pl_2 = converter._find_profiellijn_by_hydroobject_code(hydroobject_code_no_profile_should_connect)
    assert pl_2 is None, f"pl_2 code column value: {pl_2['code'].iloc[0]}"

    # Now connect profiles to hydroobject without profiles
    converter.connect_profiles_to_hydroobject_without_profiles(max_distance=500)  # STEP 4 in run method

    # Check it is connected to a profile now
    pl_2 = converter._find_profiellijn_by_hydroobject_code(hydroobject_code_no_profile_should_connect)
    assert pl_2 is not None
    assert not pl_2.empty and len(pl_2) == 1
    assert pl_2["code"].iloc[0] == nearest_profiellijn_code_it_should_connect_to

    # Compute the deepest point per hydroobject
    converter.compute_deepest_point_hydroobjects()
    dp = converter.find_deepest_point_by_hydroobject_code(hydroobject_code_no_profile_should_connect)
    assert dp == deepest_point_hydroobject_no_profile

    # Check the profile that should not connect
    pl_3 = converter._find_profiellijn_by_hydroobject_code(hydroobject_code_no_profile_should_not_connect)
    assert pl_3 is None

    # Write the result to a new file
    output_file_path = temp_dir_out / "damo.gpkg"
    converter.write_outputs(output_path=output_file_path)  # STEP 5 in run method

    assert output_file_path.exists()

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

    # Test validate HyDAMO as well
    validation_directory_path = TEMP_DIR / f"temp_HyDAMO_validator_{hrt.current_time(date=True)}"
    validation_rules_json_path = hrt.get_pkg_resource_path(schematisation_builder_resources, "validationrules.json")

    test_coverage_location = TEST_DIRECTORY / "schematisation_builder" / "dtm"  # should hold index.shp

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

    # Some checks on the general and validation rule results
    validated_profiellijn = gpd.read_file(
        validation_directory_path.joinpath("results", "results.gpkg"), layer="PROFIELLIJN"
    )

    assert not validated_profiellijn.empty

    # some checks on the general rules
    filtered_ascending = validated_profiellijn[validated_profiellijn["code"] == str(profiellijn_code_ascending)]
    filtered_descending = validated_profiellijn[validated_profiellijn["code"] == str(profiellijn_code_descending)]
    filtered_profiellijn = validated_profiellijn[validated_profiellijn["code"] == str(profiellijn_code)]

    assert not filtered_ascending.empty, f"No profiellijn found with code {profiellijn_code_ascending}"
    assert not filtered_descending.empty, f"No profiellijn found with code {profiellijn_code_descending}"
    assert not filtered_profiellijn.empty, f"No profiellijn found with code {profiellijn_code}"

    ## 100
    assert filtered_ascending["general_100_isascending"].iloc[0] == 1
    assert filtered_descending["general_100_isascending"].iloc[0] == 0

    ## 101
    assert filtered_profiellijn["general_101_hydroobject_breedte"].iloc[0] == hydroobject_breedte

    ## 102
    assert filtered_profiellijn["general_102_jaarinwinning"].iloc[0] == jaarinwinning

    ## 103
    assert (
        round(
            filtered_profiellijn["general_103_max_cross_product"].iloc[0],
            4,
        )
        == max_cross_product
    )

    ## 104
    assert filtered_profiellijn["general_104_afstandnatprofiel"].iloc[0] == lengte_nat_profiel

    ## 105
    assert filtered_profiellijn["general_105_dieptenatprofiel"].iloc[0] == diepte_nat_profiel

    # 106
    assert filtered_profiellijn["general_106_nr_of_profielpunten"].iloc[0] == nr_of_profielpunten

    # 108
    assert (
        round(
            filtered_profiellijn["general_108_maximalehoogteprofiel"].iloc[0],
            2,
        )
        == max_hoogte_profiel
    )

    ## 109
    assert (
        round(
            filtered_profiellijn["general_109_breedteprofiel"].iloc[0],
            2,
        )
        == breedte_profiel
    )


#
# %%
if __name__ == "__main__":
    test_profile_intermediate_converter()

# %%
