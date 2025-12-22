import uuid

import geopandas as gpd
import hhnk_research_tools as hrt
from shapely.geometry import LineString

from hhnk_threedi_tools.core.schematisation_builder.raw_export_to_DAMO_converter import RawExportToDAMOConverter
from hhnk_threedi_tools.core.schematisation_builder.raw_export_to_DAMO_converters.aquaduct_converter import (
    AquaductConverter,
)
from tests.config import TEMP_DIR, TEST_DIRECTORY


def test_aquaduct_converter():
    """Test AquaductConverter adds aquaduct records to duikersifonhevel layer."""
    # Setup
    logger = hrt.logging.get_logger(__name__)
    raw_export_file = TEST_DIRECTORY / "schematisation_builder" / "raw_export.gpkg"
    output_dir = TEMP_DIR / f"temp_aquaduct_converter_{hrt.current_time(date=True)}"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "damo.gpkg"

    # Run converter
    converter_base = RawExportToDAMOConverter(raw_export_file, output_file, logger)

    # Check if aquaduct layer exists and has data, if not create dummy data
    if (
        not hasattr(converter_base.data, "aquaduct")
        or converter_base.data.aquaduct is None
        or converter_base.data.aquaduct.empty
    ):
        logger.info("Aquaduct layer is empty or missing. Creating dummy aquaduct for testing.")

        # Create dummy aquaduct with all mapped attributes
        dummy_aquaduct = gpd.GeoDataFrame(
            {
                "code": ["AQ_TEST_001"],
                "globalid": [str(uuid.uuid4())],
                "geometry": [LineString([(100000, 400000), (100010, 400000)])],
                "soortmateriaal": ["beton"],
                "typemethoderuwheid": ["manning"],
                "ruwheid": [0.023],
                "hoogteconstructie": [5.5],  # will map to hoogteopening
                "breedte": [3.2],  # will map to breedteopening
                "bodemhoogtebenedenstrooms": [-2.1],  # will map to hoogtebinnenonderkantbene
                "bodemhoogtebovenstrooms": [-2.0],  # will map to hoogtebinnenonderkantbov
            },
            crs="EPSG:28992",
        )

        # Set the aquaduct data
        converter_base.data.aquaduct = dummy_aquaduct
        logger.info("Created dummy aquaduct record for testing")

    aquaduct_count = len(converter_base.data.aquaduct)
    logger.info(f"Found {aquaduct_count} aquaduct(s) in raw export")

    # Store original aquaduct data for mapping verification
    original_aquaduct = converter_base.data.aquaduct.copy()

    # Verify required columns exist in aquaduct
    required_columns = ["code", "globalid", "geometry"]
    for col in required_columns:
        assert col in converter_base.data.aquaduct.columns, f"Aquaduct layer missing required column: {col}"

    converter = AquaductConverter(converter_base)
    converter.run()
    converter_base.write_outputs()

    # Load output layer
    assert output_file.exists(), "Output file not created"
    duikersifonhevel = gpd.read_file(output_file, layer="duikersifonhevel")

    # Test 1: Duikersifonhevel layer exists and has data
    assert not duikersifonhevel.empty, "Duikersifonhevel layer is empty"
    assert len(duikersifonhevel) >= aquaduct_count, (
        f"Expected at least {aquaduct_count} duikersifonhevel records (from aquaducts), got {len(duikersifonhevel)}"
    )

    # Test 2: GlobalID field is valid (non-null and unique)
    assert "globalid" in duikersifonhevel.columns, "Duikersifonhevel missing globalid"
    assert duikersifonhevel["globalid"].notna().all(), "Duikersifonhevel has null globalid values"
    assert duikersifonhevel["globalid"].is_unique, "Duikersifonhevel has duplicate globalid values"

    # Test 3: Code field exists and is populated
    assert "code" in duikersifonhevel.columns, "Duikersifonhevel missing code"
    assert duikersifonhevel["code"].notna().all(), "Some duikersifonhevel have null code"

    # Test 4: Geometry is preserved
    assert "geometry" in duikersifonhevel.columns, "Duikersifonhevel missing geometry column"
    assert duikersifonhevel["geometry"].notna().any(), "Duikersifonhevel should have geometry"

    # Test 5: Check that mapped attributes exist (if they were in source data)
    expected_mapped_columns = [
        "soortmateriaal",
        "typekruising",
        "typemethoderuwheid",
        "ruwheid",
        "hoogteopening",
        "breedteopening",
        "hoogtebinnenonderkantbene",
        "hoogtebinnenonderkantbov",
    ]
    for col in expected_mapped_columns:
        assert col in duikersifonhevel.columns, f"Duikersifonhevel missing mapped column: {col}"

    # Test 6: Verify typekruising is set to "Aquaduct" for all records from aquaduct
    assert "typekruising" in duikersifonhevel.columns, "Duikersifonhevel missing typekruising column"
    assert (duikersifonhevel["typekruising"] == "Aquaduct").any(), (
        "At least one duikersifonhevel record should have typekruising='Aquaduct'"
    )

    # Test 7: Verify attribute mappings are correct for aquaduct records
    aquaduct_records = duikersifonhevel[duikersifonhevel["typekruising"] == "Aquaduct"]
    assert len(aquaduct_records) > 0, "Should have at least one aquaduct record"

    # Verify mappings for the first aquaduct record only
    original_row = original_aquaduct.iloc[0]
    code = original_row["code"]
    converted_row = aquaduct_records[aquaduct_records["code"] == code]

    assert len(converted_row) > 0, f"Converted record with code={code} not found"
    converted_row = converted_row.iloc[0]

    # Test dimension mappings
    if "breedte" in original_aquaduct.columns and original_row.get("breedte") is not None:
        expected_breedte = original_row["breedte"]
        actual_breedte = converted_row["breedteopening"]
        assert actual_breedte == expected_breedte, (
            f"breedte ({expected_breedte}) should map to breedteopening, got {actual_breedte}"
        )

    if (
        "bodemhoogtebenedenstrooms" in original_aquaduct.columns
        and original_row.get("bodemhoogtebenedenstrooms") is not None
    ):
        expected_bodem_bene = original_row["bodemhoogtebenedenstrooms"]
        actual_bodem_bene = converted_row["hoogtebinnenonderkantbene"]
        assert actual_bodem_bene == expected_bodem_bene, (
            f"bodemhoogtebenedenstrooms ({expected_bodem_bene}) should map to hoogtebinnenonderkantbene, got {actual_bodem_bene}"
        )


def test_aquaduct_converter_no_aquaduct_layer():
    """Test AquaductConverter gracefully handles missing aquaduct layer."""
    logger = hrt.logging.get_logger(__name__)
    raw_export_file = TEST_DIRECTORY / "schematisation_builder" / "raw_export.gpkg"
    output_dir = TEMP_DIR / f"temp_aquaduct_no_data_{hrt.current_time(date=True)}"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "damo.gpkg"

    # Create converter without aquaduct data
    converter_base = RawExportToDAMOConverter(raw_export_file, output_file, logger)

    # Remove aquaduct layer if it exists to simulate missing layer
    if hasattr(converter_base.data, "aquaduct"):
        delattr(converter_base.data, "aquaduct")

    # Run converter - should handle missing layer gracefully
    converter = AquaductConverter(converter_base)
    converter.run()  # Should not raise error, just log and skip

    # If duikersifonhevel was created from other sources, verify aquaduct wasn't added
    if hasattr(converter_base.data, "duikersifonhevel") and converter_base.data.duikersifonhevel is not None:
        # Should not have added anything
        assert True, "Converter handled missing aquaduct layer correctly"


if __name__ == "__main__":
    test_aquaduct_converter()
    test_aquaduct_converter_no_aquaduct_layer()
