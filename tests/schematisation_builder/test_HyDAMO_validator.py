# %%

import sys

import hhnk_research_tools as hrt
import pytest

if __name__ == "__main__":
    from pathlib import Path

    root = str(Path(__file__).resolve().parents[2])
    if root not in sys.path:
        sys.path.insert(0, root)

from tests.config import TEMP_DIR, TEST_DIRECTORY


# TODO remove skip when py312 implemented.
@pytest.mark.skipif(sys.version_info < (3, 12), reason="Requires Python 3.12 or higher")
def test_HyDAMO_validator():
    import hhnk_threedi_tools.resources.schematisation_builder as schematisation_builder_resources
    from hhnk_threedi_tools.core.schematisation_builder.HyDAMO_validator import validate_hydamo

    validation_directory_path = TEMP_DIR / f"temp_HyDAMO_validator_{hrt.current_time(date=True)}"
    hydamo_file_path = TEST_DIRECTORY / "schematisation_builder" / "HyDAMO.gpkg"
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
    assert validation_directory_path.joinpath("datasets", "HyDAMO.gpkg").exists()
    # TODO some checks on the output

    # TODO brug and stuw is removed from HyDAMO package
    # We do not have a kunstwerkopening layer from our export

    # TODO gemaal removed for now, no pomp layer


if __name__ == "__main__":
    test_HyDAMO_validator()

# %%
