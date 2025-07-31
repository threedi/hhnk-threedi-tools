if __name__ == "__main__":
    import sys
    from pathlib import Path

    root = Path(__file__).resolve()
    while root.name != "hhnk-threedi-tools" or root.parent.name == "hhnk-threedi-tools":
        root = root.parent

    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

import fiona
import geopandas as gpd
import hhnk_research_tools as hrt

import hhnk_threedi_tools.resources.schematisation_builder as schematisation_builder_resources
from hhnk_threedi_tools.core.schematisation_builder.DAMO_HyDAMO_converter import DAMO_to_HyDAMO_Converter
from tests.config import TEMP_DIR, TEST_DIRECTORY

LAYERS = ["profielpunt"]
temp_dir_out = TEMP_DIR / f"temp_DAMO_HyDAMO_converter_{hrt.current_time(date=True)}"


def test_DAMO_HyDAMO_versions():
    damo_file_path = TEST_DIRECTORY / "schema_builder" / "DAMO.gpkg"
    hydamo_file_path = temp_dir_out / "HyDAMO.gpkg"

    assert damo_file_path.exists()
    damo_layers = fiona.listlayers(damo_file_path)
    assert all([layer in damo_layers for layer in LAYERS])
    DAMO_profielpunt_gdf = gpd.read_file(damo_file_path, layer=LAYERS[0])

    damo_version = hydamo_version = None
    if "typeProfielPunt" in DAMO_profielpunt_gdf.columns:
        damo_version = "2.3"
        hydamo_version = "2.3"
    if "profielTypePunt" in DAMO_profielpunt_gdf.columns:
        damo_version = "2.4.1"
        hydamo_version = "2.4"
    assert damo_version != None and hydamo_version != None

    converter = DAMO_to_HyDAMO_Converter(
        damo_file_path=damo_file_path,
        hydamo_file_path=hydamo_file_path,
        damo_version=damo_version,
        hydamo_version=hydamo_version,
        layers=LAYERS,
        overwrite=False,
    )
    converter.run()

    assert hydamo_file_path.exists()
    HyDAMO_profielpunt_gdf = gpd.read_file(hydamo_file_path, layer=LAYERS[0])
    row_id = None

    if damo_version == "2.3":
        assert "typeProfielPunt" in DAMO_profielpunt_gdf.columns
        assert DAMO_profielpunt_gdf["typeProfielPunt"].apply(lambda x: isinstance(x, (int))).all()
        for i, row in DAMO_profielpunt_gdf.iterrows():
            if row["typeProfielPunt"] == 1:
                row_id = i
    if hydamo_version == "2.3":
        assert "typeprofielpunt" in HyDAMO_profielpunt_gdf.columns
        assert HyDAMO_profielpunt_gdf["typeprofielpunt"].apply(lambda x: isinstance(x, (str))).all()
        if row_id:
            assert HyDAMO_profielpunt_gdf.loc[row_id, "typeprofielpunt"] == "linker insteek landzijde"

    if damo_version == "2.4.1":
        assert "profielTypePunt" in DAMO_profielpunt_gdf.columns
        assert DAMO_profielpunt_gdf["profielTypePunt"].apply(lambda x: isinstance(x, (int))).all()
        for i, row in DAMO_profielpunt_gdf.iterrows():
            if row["profielTypePunt"] == 1:
                row_id = i
    if hydamo_version == "2.4":
        assert "profieltypepunt" in HyDAMO_profielpunt_gdf.columns
        assert HyDAMO_profielpunt_gdf["profieltypepunt"].apply(lambda x: isinstance(x, (str))).all()
        if row_id:
            assert HyDAMO_profielpunt_gdf.loc[row_id, "profieltypepunt"] == "linker insteek sloot binnendijks"


# %%
if __name__ == "__main__":
    test_DAMO_HyDAMO_versions()
# %%
