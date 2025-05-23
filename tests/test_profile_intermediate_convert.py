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

    # Check for a single hydroobject OAF-JF-3094
    linemerge_id = converter.find_linemerge_id_by_hydroobject_code("OAF-JF-3094")
    assert linemerge_id is not None
    linemerge = converter.hydroobject_linemerged.query("linemergeID == @linemerge_id").iloc[0]
    assert linemerge["categorie"] == "secondary"
    peilgebied_id = converter.find_peilgebied_id_by_hydroobject_code("OAF-JF-3094")
    assert peilgebied_id == 62170

    # Create profielgroep, profiellijn and profielpunt
    converter.create_profile_tables()

    # Tests for profile tables
    profielpunt = converter.profielpunt
    profiellijn = converter.profiellijn
    profielgroep = converter.profielgroep
    hydroobject = converter.hydroobject

    # Filter profielpunt on id 16564
    pp = profielpunt[profielpunt["id"] == 16564]
    assert len(pp) == 1

    # Get profiellijnID
    lijn_id = pp.iloc[0]["profielLijnID"]

    # Filter profiellijn on GlobalID
    pl = profiellijn[profiellijn["GlobalID"] == lijn_id]
    assert len(pl) == 1
    assert pl.iloc[0]["code"] == 42315

    # Get profielgroepID
    groep_id = pl.iloc[0]["profielgroepID"]

    # Filter profielgroep on GlobalID
    pg = profielgroep[profielgroep["GlobalID"] == groep_id]
    assert len(pg) == 1
    assert pg.iloc[0]["code"] == 42315

    # Check hydroobjectID of profielgroep
    ho = hydroobject[hydroobject["CODE"] == "OAF-QJ-16158"]
    assert pg["hydroobjectID"].iloc[0] == ho["GlobalID"].iloc[0]

    # Compute the deepest point
    converter.compute_deepest_point_profiellijn()
    assert converter.profiellijn["diepstePunt"].notnull().any()
    assert converter.profiellijn[converter.profiellijn["code"] == 42315]["diepstePunt"].iloc[0] == -1.6

    # Connect profiles to hydroobject without profiles
    converter.connect_profiles_to_hydroobject_without_profiles()
    # TODO test something

    # Write the result to a new file
    output_file_path = temp_dir_out / "output.gpkg"
    converter.write_outputs(output_path=output_file_path)

    assert output_file_path.exists()


# %%
if __name__ == "__main__":
    test_profile_intermediate_converter()

# %%
