from pathlib import Path
from HyDAMOValidatieModule.hydamo_validation import validator

# TODO make import work by local installation of HyDAMOValidatieModule
# TODO how to prepare coverages_dict?
# TODO create standard validation_rules_json_path in project folder
# TODO make test for this function

def validate_hydamo(hydamo_file_path, validation_rules_json_path, validation_directory, coverages_dict, output_types=["geopackage", "csv", "geojson"]):
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
    validation_directory.mkdir(parents=True, exist_ok=True)
    hydamo_file_path = Path(hydamo_file_path)
    validation_rules_json_path = Path(validation_rules_json_path)
    hydamo_file_path.copy2(validation_directory / hydamo_file_path.name)
    validation_rules_json_path.copy2(validation_directory / validation_rules_json_path.name)

    # Prepare the validator
    hydamo_validator = validator(
        coverages=coverages_dict,
        output_types=output_types
    )

    # Validate the HyDAMO file
    datamodel, layer_summary, result_summary = hydamo_validator(
        directory=validation_directory,
        raise_error=True
    )

    return result_summary.to_dict()