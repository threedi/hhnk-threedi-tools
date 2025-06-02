# %%
from pathlib import Path

# %%
import hhnk_research_tools as hrt

# %%
from hhnk_threedi_tools.core.schematisation_builder.profile_intermediate_converter import ProfileIntermediateConverter
from tests.config import FOLDER_TEST, TEMP_DIR, TEST_DIRECTORY


# %%
def test_profile_intermediate_converter():
    """
    Test the ProfileIntermediateConverter class.
    - Check if the converter can load and validate layers.
    - Check if the line merge algorithm works correctly.
    """
    damo_file_path = TEST_DIRECTORY / "schema_builder" / "DAMO anna paulowna.gpkg"
    cso_file_path = (
        TEST_DIRECTORY / "schema_builder" / "CSO anna paulowna.gpkg"
    )  # TODO fake data, not extracted from CSO
    temp_dir_out = TEMP_DIR / f"temp_profile_intermediate_converter_{hrt.current_time(date=True)}"
    Path(temp_dir_out).mkdir(parents=True, exist_ok=True)

    converter = ProfileIntermediateConverter(damo_file_path=damo_file_path, ods_cso_file_path=cso_file_path)

    # Load and validate layers
    converter.load_layers()

    # Check if layers are loaded
    assert converter.hydroobject is not None
    assert converter.gecombineerde_peilen is not None

    # Process line merge
    converter.process_linemerge()

    # Check if line merge result is stored
    assert converter.hydroobject_linemerged is not None

    # Test variables for profile on primary watergang
    hydroobject_code = "OAF-QJ-14396"
    peilgebied_id = 62149
    profielpunt_id = 12578
    profiellijn_code = 58395
    diepste_punt_profiel = -3.16

    # Check for a single hydroobject
    linemerge_id = converter.find_linemerge_id_by_hydroobject_code(hydroobject_code)
    assert linemerge_id is not None
    linemerge = converter.hydroobject_linemerged.query("linemergeID == @linemerge_id").iloc[0]
    assert linemerge["categorie"] == "primary"
    peilgebied_id_result = converter.find_peilgebied_id_by_hydroobject_code(hydroobject_code)
    assert peilgebied_id_result == peilgebied_id

    # Create profielgroep, profiellijn and profielpunt
    converter.create_profile_tables()

    # Tests for profile tables
    profielpunt = converter.profielpunt
    profiellijn = converter.profiellijn
    profielgroep = converter.profielgroep
    hydroobject = converter.hydroobject

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
    converter.compute_deepest_point_profiellijn()
    assert converter.profiellijn["diepstePunt"].notnull().any()
    assert converter.profiellijn[converter.profiellijn["code"] == profiellijn_code]["diepstePunt"].iloc[0] == diepste_punt_profiel

    # Test variables for profile on primary watergang without profile
    hydroobject_code_no_profile = "OAF-Q-36452" # near end peilgebied
    nearest_profiellijn_code_it_should_connect_to = 62421
    deepest_point_hydroobject_no_profile = -4.57

    # Check it is not connected to a profile
    pl_2 = converter.find_profiellijn_by_hydroobject_code(hydroobject_code_no_profile)
    assert pl_2 is None

    # Now connect profiles to hydroobject without profiles
    converter.connect_profiles_to_hydroobject_without_profiles(max_distance=250)

    # Check it is connected to a profile now
    pl_2 = converter.find_profiellijn_by_hydroobject_code(hydroobject_code_no_profile)
    assert pl_2 is not None 
    assert not pl_2.empty and len(pl_2) == 1
    assert pl_2["code"].iloc[0] == nearest_profiellijn_code_it_should_connect_to

    # Compute the deepest point per hydroobject
    converter.compute_deepest_point_hydroobjects()
    dp = converter.find_deepest_point_by_hydroobject_code(hydroobject_code_no_profile)
    assert dp == deepest_point_hydroobject_no_profile
    
    # Write the result to a new file
    output_file_path = temp_dir_out / "output.gpkg"
    converter.write_outputs(output_path=output_file_path)

    assert output_file_path.exists()

    # Temp test converter to HyDAMO as well
    from hhnk_threedi_tools.core.schematisation_builder.DAMO_HyDAMO_converter import DAMO_to_HyDAMO_Converter
    hydamo_file_path = temp_dir_out / "HyDAMO anna paulowna.gpkg"
    converter_hydamo = DAMO_to_HyDAMO_Converter(
        damo_file_path=output_file_path,
        hydamo_file_path=hydamo_file_path,
        layers=["HYDROOBJECT", "PROFIELPUNT", "PROFIELLIJN", "PROFIELGROEP"],
        overwrite=False,
    )

    converter_hydamo.run()

    # Check if HyDAMO.gpkg is created
    assert hydamo_file_path.exists()


# %%
if __name__ == "__main__":
    test_profile_intermediate_converter()

# %%
