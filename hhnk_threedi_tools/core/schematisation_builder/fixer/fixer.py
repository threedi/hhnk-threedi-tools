import json
import logging
import shutil
import time
import traceback
from functools import partial
from pathlib import Path
from typing import Callable, List, Literal, Optional, Tuple, Union

import geopandas as gpd
import hhnk_research_tools as hrt
import hydamo_validation.schemas as hydamo_validation_schemas
import pandas as pd
from core.schematisation_builder.fixer import hydamo_fixes
from core.schematisation_builder.fixer.summaries import ExtendedHyDAMO, ExtendedLayersSummary, ExtendedResultSummary
from hydamo_validation import logical_validation
from hydamo_validation.datamodel import HyDAMO
from hydamo_validation.datasets import DataSets
from hydamo_validation.syntax_validation import (
    datamodel_layers,
    missing_layers,
)
from hydamo_validation.utils import Timer
from hydamo_validation.validator import read_validation_rules

import hhnk_threedi_tools.resources.schematisation_builder as schematisation_builder_resources

OUTPUT_TYPES = ["geopackage"]
LOG_LEVELS = Literal["INFO", "DEBUG"]
SCHEMAS_PATH = hrt.get_pkg_resource_path(schematisation_builder_resources, "schemas")
HYDAMO_SCHEMAS_PATH = hrt.get_pkg_resource_path(hydamo_validation_schemas, "hydamo")


def _continue(message="Would you like to proceed? (y/n): "):
    while True:
        choice = input(message).strip().lower()
        if choice in ("y", "yes"):
            return True
        elif choice in ("n", "no"):
            return False
        else:
            print("Please answer with 'y' or 'n'.")


def pause_for_review(file_path: Path, logger: logging.Logger, result_summary: ExtendedResultSummary) -> bool:
    """
    Log a review pause, prompt the user to continue, and record the outcome.

    Returns True if the user chooses to proceed, False otherwise.
    """
    logger.info("User review")
    logger.info(f"Review: You can inspect or edit the review geopackage here: \n{file_path}")
    logger.info("Review: Edit staged fixes manually by filling in values in the manual_overwrite columns.")
    time.sleep(0.1)
    if not _continue("Do you wish to proceed? Answering no will terminate the process. (y/n): "):
        logger.info("Review: User does not want to proceed.")
        result_summary.append_warning("Review: User has terminated the process during the review stage.")
        result_summary.status = "terminated by user during review"
        raise SystemExit("Process terminated by user during review.")


def _read_schema(version: str, schemas_path: Path):
    schema_json = schemas_path.joinpath(rf"fixes_{version}.json").resolve()
    with open(schema_json) as src:
        schema = json.load(src)
    return schema


def _check_attributes(gdf, attributes):
    for i in attributes:
        if type(i) == str:
            if not i in gdf.columns:
                raise KeyError(rf"'{i}' not in columns: {gdf.columns.to_list()}. Rule cannot be executed")
    ## maybe write logic to check whether attributes are dependent on another dataframe or smth


def _init_logger(log_level: str):
    """Init logger for validator."""
    logger = logging.getLogger(__name__)
    logger.setLevel(getattr(logging, log_level))
    return logger


def _add_log_file(logger: logging.Logger, log_file: Path):
    """Add log-file to existing logger"""
    fh = logging.FileHandler(log_file)
    fh.setFormatter(logging.Formatter("%(asctime)s %(name)s %(levelname)s - %(message)s"))
    fh.setLevel(logging.DEBUG)
    logger.addHandler(fh)
    return logger


def _close_log_file(logger: logging.Logger):
    """Remove log-file from existing logger"""
    for h in logger.handlers:
        h.close()
        logger.removeHandler(h)


def _log_to_results(log_file: Path, result_summary: ExtendedResultSummary):
    result_summary.log = log_file.read_text().split("\n")


def fixer(
    output_types: List[str] = OUTPUT_TYPES,
    log_level: Literal["INFO", "DEBUG"] = "INFO",
    coverages: dict = {},
) -> Callable:
    """

    Parameters
    ----------
    output_types : List[str], optional
        The types of output files that will be written. Options are
        ["geojson", "csv", "geopackage"]. By default all will be written
    log_level : Literal['INFO', 'DEBUG'], optional
        Level for logger. The default is "INFO".
    coverages : dict, optional
    Location of coverages. E.g. {"AHN: path_to_ahn_dir} The default is {}.

    Returns
    -------
    Callable[[str], dict]
        Partial of _validator function

    """

    return partial(
        _fixer,
        output_types=output_types,
        log_level=log_level,
        coverages=coverages,
    )


def _fixer(
    directory,
    output_types: List[str] = OUTPUT_TYPES,
    log_level: Literal["INFO", "DEBUG"] = "INFO",
    coverages: dict = {},
    raise_error: bool = False,
):
    """
    Parameters
    ----------
    directory : str
        Directory with datasets sub-directory and validation_rules.json
    output_types : List[str], optional
        The types of output files that will be written. Options are
        ["geojson", "csv", "geopackage"]. By default all will be written
    log_level : Literal['INFO', 'DEBUG'], optional
        Level for logger. The default is "INFO".
    coverages : dict, optional
       Location of coverages. E.g. {"AHN: path_to_ahn_dir} The default is {}.
    raise_error: bool, optional
        Will raise an error (or not) when Exception is raised. The default is False

    Returns
    -------
    HyDAMO, LayersSummary, ResultSummary
        Will return a tuple with a filled HyDAMO datamodel, layers_summary and result_summary
    """
    timer = Timer()
    try:
        results_path = None
        dir_path = Path(directory)
        logger = _init_logger(
            log_level=log_level,
        )

        logger.info("init fixer")
        date_check = pd.Timestamp.now().isoformat()
        fix_summary = ExtendedLayersSummary(date_check=date_check)
        result_summary = ExtendedResultSummary(date_check=date_check)

        # check if all files are present
        # create a results_path
        results_permission_error = review_permission_error = False
        if dir_path.exists():
            review_path = dir_path.joinpath("review")
            if review_path.exists():
                try:
                    shutil.rmtree(review_path)
                except PermissionError:
                    review_permission_error = True
            review_path.mkdir(parents=True, exist_ok=True)
            results_path = dir_path.joinpath("results")
            if results_path.exists():
                try:
                    shutil.rmtree(results_path)
                except PermissionError:
                    results_permission_error = True
            results_path.mkdir(parents=True, exist_ok=True)
        else:
            raise FileNotFoundError(f"{dir_path.absolute().resolve()} does not exist")

        log_file = results_path.joinpath("fixer.log")
        logger = _add_log_file(logger, log_file=log_file)
        logger.info("start fixer")
        if review_permission_error:
            logger.warning(f"Kan pad {review_path} niet verwijderen. Dit kan later tot problemen leiden!")
        if results_permission_error:
            logger.warning(f"Kan pad {results_path} niet verwijderen. Dit kan later tot problemen leiden!")
        dataset_path = dir_path.joinpath("datasets")
        validation_rules_json = dir_path.joinpath("validationrules.json")
        validation_results_gpkg = dir_path.joinpath("results.gpkg")
        hydamo_gpkg = dataset_path.joinpath("HyDAMO_validated.gpkg")
        missing_paths = []
        for path in [dataset_path, validation_rules_json, validation_results_gpkg, hydamo_gpkg]:
            if not path.exists():
                missing_paths += [str(path)]
        if missing_paths:
            result_summary.error += [f"missing_paths: {','.join(missing_paths)}"]
            raise FileNotFoundError(f"missing_paths: {','.join(missing_paths)}")
        else:
            validation_rules_sets = read_validation_rules(validation_rules_json, result_summary)
            validation_rules_objects = validation_rules_sets["objects"]

        # check if output-files are supported
        unsupported_output_types = [item for item in output_types if item not in OUTPUT_TYPES]
        if unsupported_output_types:
            error_message = r"unsupported output types: " f"{','.join(unsupported_output_types)}"
            result_summary.error += [error_message]
            raise TypeError(error_message)

        # set coverages
        if coverages:
            for key, item in coverages.items():
                logical_validation.general_functions._set_coverage(key, item)

        # start fixing
        # read data-model
        result_summary.status = "load data-model"
        datasets = DataSets(dataset_path)
        try:
            hydamo_version = validation_rules_sets["hydamo_version"]
            hydamo_schema_layers = HyDAMO(
                version=hydamo_version,
                schemas_path=HYDAMO_SCHEMAS_PATH,
            ).layers
            schema_layers_not_in_dataset = [i for i in hydamo_schema_layers if i not in datasets.layers]
            datamodel = ExtendedHyDAMO.from_geopackage(
                hydamo_path=hydamo_gpkg,
                results_path=validation_results_gpkg,
                rules_objects=validation_rules_objects,
                version=hydamo_version,
                ignored_layers=schema_layers_not_in_dataset,
            )
        except Exception as e:
            result_summary.error = ["datamodel cannot be loaded (see exception)"]
            raise e

        # validate dataset syntax
        result_summary.status = "fix-preparation (layers)"
        result_summary.dataset_layers = datasets.layers

        ## validate syntax of datasets on layers-level and append to result
        logger.info("start fix-voorbereiding van object-lagen")
        valid_layers = datamodel_layers(datamodel.layers, datasets.layers)
        result_summary.missing_layers = missing_layers(datamodel.layers, datasets.layers)

        ## validate valid_layers on fields-level and add them to data_model
        result_summary.status = "fix-preparation (staging)"
        fix_preparation_result = []

        ## get status_object if any
        status_object = None
        if "status_object" in validation_rules_sets.keys():
            status_object = validation_rules_sets["status_object"]
            ## allows us to filter the invalid rows. Only need to add status_object to gdf based on validation result. Should be valid or invalid

        # do fix execution: apply fixes and export results
        datamodel_check, fix_summary, result_summary = hydamo_fixes.execute(
            datamodel,
            fix_summary,
            result_summary,
            logger,
            raise_error,
            keep_general=False,
        )
        # do fix review: append result to fix_summary
        result_summary.status = "fix-preparation (reviewing)"
        fix_preparation_result = []
        fix_summary, result_summary = hydamo_fixes.review(
            datamodel_check,
            fix_summary,
            result_summary,
            logger,
            raise_error,
        )
        fix_layers = fix_summary.export(
            results_path=review_path, gpkg_name="fix_summary.gpkg", output_types=OUTPUT_TYPES
        )
        fix_preparation_result = fix_layers

        # user review: pause the process and allow the user to review fixes
        result_summary.status = "fix-review (pause for review)"
        pause_for_review(review_path / "fix_summary.gpkg", logger, result_summary)

        # read fix review: review and prepare for fix execution
        result_summary.status = "load fix summary"
        logger.info("start inladen van fix summary")
        fix_summary = ExtendedLayersSummary.from_geopackage(file_path=review_path / "fix_summary.gpkg")
        fix_preparation_result = fix_summary.data_layers

        # do fix execution: apply fixes and export results
        result_summary.status = "fix execution"
        logger.info("start toepassen fixes op object-lagen")
        datamodel, fix_summary, result_summary = hydamo_fixes.execute(
            datamodel,
            fix_summary,
            result_summary,
            logger,
            raise_error,
            keep_general=False,
        )
        result_summary.status = "fix review (manual overwrites)"
        logger.info("start fix review van object-lagen met handmatige aanpassingen")
        fix_summary, result_summary = hydamo_fixes.review(
            datamodel,
            fix_summary,
            result_summary,
            logger,
            raise_error,
        )
        fix_layers = fix_summary.export(
            results_path=review_path, gpkg_name="fix_summary.gpkg", output_types=OUTPUT_TYPES
        )

        # finish validation and export results
        logger.info("exporteren resultaten")
        datamodel.to_geopackage(results_path / "HyDAMO_fixed.gpkg")
        result_summary.status = "export results"
        result_summary.fix_layers = fix_layers
        result_summary.error_layers = [i for i in datasets.layers if i.lower() not in fix_layers]
        result_summary.prep_result = fix_preparation_result
        result_summary.fix_result = [
            i["object"] for i in validation_rules_sets["objects"] if i["object"] in fix_layers
        ]
        result_summary.success = True
        result_summary.status = "finished"
        result_summary.duration = timer.report()
        logger.info(f"klaar in {result_summary.duration:.2f} seconden")

        _log_to_results(log_file, result_summary)

        result_summary.to_json(results_path, "fix_result.json")

        _close_log_file(logger)

        return datamodel, fix_summary, result_summary

    except Exception as e:
        stacktrace = rf"\n{traceback.format_exc(limit=0, chain=False)}".split("\n")
        if result_summary.error is not None:
            result_summary.error += stacktrace
        else:
            result_summary.error = stacktrace
        if results_path is not None:
            fix_layers = fix_summary.export(results_path, "fix_summary.gpkg", output_types)
            _log_to_results(log_file, result_summary)
            result_summary.to_json(results_path, "fix_result.json")
        if raise_error:
            raise e
        else:
            result_summary.to_dict()

        _close_log_file(logger)

        return None, fix_summary, result_summary


def fixer(
    output_types: List[str] = OUTPUT_TYPES,
    log_level: Literal["INFO", "DEBUG"] = "INFO",
    coverages: dict = {},
) -> Callable:
    """

    Parameters
    ----------
    output_types : List[str], optional
        The types of output files that will be written. Options are
        ["geojson", "csv", "geopackage"]. By default all will be written
    log_level : Literal['INFO', 'DEBUG'], optional
        Level for logger. The default is "INFO".
    coverages : dict, optional
    Location of coverages. E.g. {"AHN: path_to_ahn_dir} The default is {}.

    Returns
    -------
    Callable[[str], dict]
        Partial of _validator function

    """

    return partial(
        _fixer,
        output_types=output_types,
        log_level=log_level,
        coverages=coverages,
    )
