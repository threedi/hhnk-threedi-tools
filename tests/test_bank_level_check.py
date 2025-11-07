# %%

import sys

import pytest

from hhnk_threedi_tools.core.checks.bank_levels import BankLevelCheck
from tests.config import FOLDER_TEST


@pytest.fixture(scope="session")
def bl_check():
    """Fixture that provides a BankLevelCheck instance with prepared data.

    Scope is set to 'session' to reuse the same instance across all tests
    for better performance.
    """
    check = BankLevelCheck(FOLDER_TEST)
    check.prepare_data()
    return check


@pytest.mark.skipif(sys.version_info < (3, 12), reason="Requires Python 3.12 or higher")
def test_import_data(bl_check: BankLevelCheck):
    """Test if the import of data works, if the correct amount is imported"""
    assert bl_check.fixeddrainage_gdf.count()["peil_id"] == 32
    assert bl_check.fixeddrainage_boundary_gdf.count()["peil_id"] == 35
    assert bl_check.lines_1d2d.count()["id"] == 105
    assert bl_check.channel_gdf.count()["code"] == 49
    assert bl_check.connection_node_gdf.count()["code"] == 72
    assert bl_check.cross_section_gdf.count()["code"] == 96
    assert bl_check.obstacle_gdf.count()["code"] == 54
    assert bl_check.obstacle_gdf["crest_level"][54] == 0.159


@pytest.mark.skipif(sys.version_info < (3, 12), reason="Requires Python 3.12 or higher")
def test_intersections(bl_check: BankLevelCheck):
    """Test intersection detection between obstacles and 1d2d lines.

    Verifies that:
    1. Specific intersection (id_1d2d=166) has correct crest level (0.510)
    2. Total number of intersections is correct (10)
    3. First intersection is correctly typed as obstacle crossing
    """
    result = bl_check.line_intersections()
    assert result[result["id_1d2d"] == 166]["crest_level"].to_numpy() == 0.510
    assert result.count()["intersect_type"] == 10
    assert bl_check.line_intersects["intersect_type"][0] == "1d2d_crosses_obstacle"


@pytest.mark.skipif(sys.version_info < (3, 12), reason="Requires Python 3.12 or higher")
def test_divergent_waterlevel_nodes(bl_check: BankLevelCheck):
    """Test detection of nodes with divergent water levels.

    Checks if nodes that are in incorrect fixed drainage areas are properly
    identified. These are nodes where the water level doesn't match the
    expected level for their location.
    """
    result = bl_check.divergent_waterlevel_nodes()
    assert result["type"][0] == "node_in_wrong_fixeddrainage_area"


@pytest.mark.skipif(sys.version_info < (3, 12), reason="Requires Python 3.12 or higher")
def test_get_new_manholes(bl_check: BankLevelCheck):
    """Test generation of new manholes at problematic locations.

    This test verifies the creation of new manholes where needed, specifically:
    1. Runs prerequisite checks (divergent nodes and line intersections)
    2. Generates new manholes where needed
    3. Verifies correct tagging of manholes (e.g., leak detection)
    """
    bl_check.divergent_waterlevel_nodes()
    bl_check.line_intersections()
    result = bl_check.get_new_manholes()
    assert result["tags"][9] == "leak across obstacle from node"


@pytest.mark.skipif(sys.version_info < (3, 12), reason="Requires Python 3.12 or higher")
def test_generate_cross_section_locations(bl_check: BankLevelCheck):
    """Test generation of cross-section locations with adjusted bank levels.

    Verifies that:
    1. Bank levels are properly reset with appropriate safety margins (+10cm)
    2. Specific bank level differences are correctly calculated
    3. Cross-sections are properly tagged with their adjustments

    Prerequisites:
    - Line intersections must be calculated first
    """
    bl_check.line_intersections()
    result = bl_check.generate_cross_section_locations()

    assert result["tags"][0] == "bank_level reset to lowest possible + 10 cm"
    assert result["bank_level_diff"][82] == -1.662


@pytest.mark.skipif(sys.version_info < (3, 12), reason="Requires Python 3.12 or higher")
def test_generate_channels(bl_check: BankLevelCheck):
    """Test channel generation with updated bank levels.

    Tests both channels with and without obstacle crossings:
    1. Regular channel (id=18):
        - Verifies water level average
        - Checks original bank level
        - Validates new adjusted bank level

    2. Channel with obstacle crossing (id=518):
        - Confirms proper water level handling
        - Verifies bank level preservation where needed

    Prerequisites:
    - Line intersections must be calculated
    - Cross-section locations must be generated
    """
    bl_check.line_intersections()
    bl_check.generate_cross_section_locations()
    result = bl_check.generate_channels()

    # Channel without crossing with obstacle
    assert result["initial_water_level_average"][18] == -0.55
    assert result["bank_level"][18] == 0.245
    assert result["new_bank_level"][18] == -0.45

    # Channel with crossing with obstacle
    row = result[result["channel_id"] == 518]
    assert row["initial_water_level_average"].iloc[0] == -1.0
    assert row["bank_level"].iloc[0] == 0.244
    assert row["new_bank_level"].iloc[0] == 0.244


@pytest.mark.skipif(sys.version_info < (3, 12), reason="Requires Python 3.12 or higher")
def test_results(bl_check: BankLevelCheck):
    """Test the complete bank level check workflow results.

    Runs the entire bank level check process and verifies the final results,
    specifically checking:
    1. Number of line intersections detected
    2. Completeness of results dictionary
    3. Integrity of final intersection data
    """
    bl_check.run()
    results = bl_check.results

    assert results["line_intersects"].count()["id_1d2d"] == 10


# %%
