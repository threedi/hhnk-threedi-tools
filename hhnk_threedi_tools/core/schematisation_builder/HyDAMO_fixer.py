import shutil
from pathlib import Path

import hhnk_research_tools as hrt
from core.schematisation_builder.fixer.fixer import fixer
from core.schematisation_builder.fixer.summaries import ExtendedHyDAMO


def fix_hydamo(
    hydamo_file_path: Path,
    validation_rules_json_path: Path,
    results_gpkg_path: Path,
    fix_directory_path: Path,
    coverages_dict: dict,
    output_types: list[str] = ["geopackage"],
    logger=None,
) -> tuple[ExtendedHyDAMO, dict]:
    r"""
    Validate the HyDAMO file

    Parameters
    ----------
    hydamo_file_path : Path
        Path to the HyDAMO file
    validation_rules_json_path : Path
        Path to the JSON file with validation rules
    coverages_dict : dict
        Dictionary with the coverages, e.g. {"AHN": r"../tests/data/dtm"}. This dtm dir needs to
        hold an index.shp file, see hydamo_validation/functions/general.py buffer(). Here it uses
        the COVERAGES dict to load an index.shp to gdf.
    output_types : list[str], optional
        List with the output types, by default ["geopackage"]

    Writes
    ------
    TODO

    Returns
    -------
    result_summary : dict
        Output dict with summary of validation, including; succesful, missing_layers, logs.
        This is also written to results\validation_result.json.
    """
    if not logger:
        logger = hrt.logging.get_logger(__name__)
    logger.info("Start fixer")

    # Prepare the fix directory containing the HyDAMO file and the validation rules
    fix_directory_path.mkdir(parents=True, exist_ok=True)
    hydamo_file_path = Path(hydamo_file_path)
    validation_rules_json_path = Path(validation_rules_json_path)

    hydamo_file_path2 = fix_directory_path.joinpath("datasets", hydamo_file_path.name)
    # Copy the HyDAMO file and the validation rules to the fix directory, to ensure you use the most recent HyDAMO file
    hydamo_file_path2.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(hydamo_file_path, hydamo_file_path2)

    validation_rules_json_path2 = fix_directory_path.joinpath("validationrules.json")
    if not validation_rules_json_path2.exists():
        shutil.copy2(validation_rules_json_path, validation_rules_json_path2)

    results_gkpg_path2 = fix_directory_path.joinpath("results.gpkg")
    if not results_gkpg_path2.exists():
        shutil.copy2(results_gpkg_path, results_gkpg_path2)

    # Prepare the fixer
    hydamo_fixer = fixer(coverages=coverages_dict, output_types=output_types)
    # TODO how to get logging in logger

    # Fix the HyDAMO file
    datamodel, layer_summary, result_summary = hydamo_fixer(directory=fix_directory_path, raise_error=True)

    return datamodel, layer_summary, result_summary.to_dict()
