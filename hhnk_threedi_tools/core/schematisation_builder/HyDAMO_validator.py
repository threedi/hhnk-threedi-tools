import os
import shutil
from pathlib import Path

from hydamo_validation import validator

# TODO make import work by local installation of HyDAMOValidatieModule


def validate_hydamo(
    hydamo_file_path,
    validation_rules_json_path,
    validation_directory_path,
    coverages_dict,
    output_types=["geopackage", "csv", "geojson"],
):
    """
    Validate the HyDAMO file

    Parameters
    ----------
    hydamo_file_path : str
        Path to the HyDAMO file
    validation_rules_json_path : str
        Path to the JSON file with validation rules
    coverages_dict : dict
        Dictionary with the coverages, e.g. {"AHN": r"../tests/data/dtm"}
    output_types : list, optional
        List with the output types, by default ["geopackage", "csv", "geojson"]
    """
    # Prepare the validation directory containing the HyDAMO file and the validation rules
    validation_directory_path.mkdir(parents=True, exist_ok=True)
    hydamo_file_path = Path(hydamo_file_path)
    validation_rules_json_path = Path(validation_rules_json_path)

    # Copy the HyDAMO file and the validation rules to the validation directory if they are not already there
    if not (validation_directory_path / "datasets" / hydamo_file_path.name).exists():
        Path(validation_directory_path / "datasets").mkdir(parents=True, exist_ok=True)
        shutil.copy2(hydamo_file_path, validation_directory_path / "datasets" / hydamo_file_path.name)
    if not (validation_directory_path / "validationrules.json").exists():
        shutil.copy2(validation_rules_json_path, validation_directory_path / "validationrules.json")

    # Prepare the validator
    hydamo_validator = validator(coverages=coverages_dict, output_types=output_types)

    # Validate the HyDAMO file
    datamodel, layer_summary, result_summary = hydamo_validator(directory=validation_directory_path, raise_error=True)

    return result_summary.to_dict()
