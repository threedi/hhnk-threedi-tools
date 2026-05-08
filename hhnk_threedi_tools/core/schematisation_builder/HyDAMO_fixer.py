import shutil
from pathlib import Path

import hhnk_research_tools as hrt
from core.schematisation_builder.fixer.data import ExtendedHyDAMO
from core.schematisation_builder.fixer.fixer import fixer


def fix_hydamo(
    hydamo_file_path: Path,
    validation_rules_json_path: Path,
    results_gpkg_path: Path,
    fix_directory_path: Path,
    coverages_dict: dict,
    output_types: list[str] = ["geopackage"],
    logger=None,
) -> tuple[ExtendedHyDAMO, dict, dict]:
    r"""
    Apply automated fixes to a validated HyDAMO file.

    Copies the HyDAMO file, validation rules, and validation results into
    ``fix_directory_path``, then runs the fixer pipeline which:
    1. Builds a review layer with fix suggestions and validation summaries.
    2. Pauses for manual inspection (the user can edit ``manual_overwrite`` columns).
    3. Applies staged fixes and manual overwrites to produce ``HyDAMO_fixed.gpkg``.

    Parameters
    ----------
    hydamo_file_path : Path
        Path to the validated HyDAMO GeoPackage.
    validation_rules_json_path : Path
        Path to the JSON file with validation rules.
    results_gpkg_path : Path
        Path to the validation results GeoPackage (from a prior validation run).
    fix_directory_path : Path
        Working directory for the fix process. Created if it does not exist.
        Receives copies of the input files and all intermediate outputs.
    coverages_dict : dict
        Coverage lookup used by topologic/general rules,
        e.g. ``{"AHN": r"../tests/data/dtm"}``. The directory must contain an
        ``index.shp`` file (see ``hydamo_validation/functions/general.py``).
    output_types : list[str], optional
        Output file formats to write. Default is ``["geopackage"]``.
    logger : logging.Logger, optional
        Logger instance. A default logger is created if not provided.

    Returns
    -------
    datamodel : ExtendedHyDAMO
        The corrected HyDAMO datamodel.
    layer_summary : dict
        Per-layer fix summary produced during the review phase.
    result_summary : dict
        Overall fix result summary including status, warnings, and errors.
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
