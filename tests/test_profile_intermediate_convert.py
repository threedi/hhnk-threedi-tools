# %%
import hhnk_research_tools as hrt
from hhnk_threedi_tools.core.schematisation_builder.profile_intermediate_converter import ProfileIntermediateConverter
from tests.config import FOLDER_TEST, TEMP_DIR, TEST_DIRECTORY

# %%
def test_profile_intermediate_converter():
    """
    Test the ProfileIntermediateConverter class.
    - Check if the converter can load and validate DAMO layers.
    - Check if the line merge algorithm works correctly.
    """
    damo_file_path = TEST_DIRECTORY / "schema_builder" / "DAMO_anna_paulowna.gpkg"
    cso_file_path = TEST_DIRECTORY / "schema_builder" / "CSO_anna_paulowna.gpkg"
    temp_dir_out = TEMP_DIR / f"temp_profile_intermediate_converter_{hrt.current_time(date=True)}"
    
    converter = ProfileIntermediateConverter(
        damo_file_path=damo_file_path,
        ods_cso_file_path=cso_file_path
    )
    
    # Load and validate DAMO layers
    converter.load_layers()
    
    # Check if layers are loaded
    assert converter.hydroobject_raw is not None
    assert converter.gecombineerde_peilen is not None
    
    # Process line merge
    converter.process_linemerge()
    
    # Check if line merge result is stored
    assert converter.hydroobject_linemerged is not None

    # Check single hydroobject
    linemerge = converter.find_linemerge_by_code("OAF-JF-3094")
    assert linemerge is not None
    assert linemerge["peilgebiedID"].values[0] == 62170
     
    # Write the result to a new file
    output_file_path = temp_dir_out / "output.gpkg"
    converter.write_outputs(output_file_path=output_file_path)

    assert output_file_path.exists()

# %%
if __name__ == "__main__":
    test_profile_intermediate_converter()

# %%