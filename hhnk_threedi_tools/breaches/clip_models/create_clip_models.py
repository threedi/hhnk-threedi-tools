"""Create and clean clipped 3Di submodels.

This module provides the main entry point for the submodel clipping workflow.

The process consists of two steps:

1. Create a clipped submodel for each polygon in the subareas dataset.
2. Clean each generated GeoPackage by removing 1D elements that should not
   remain in the final submodel.

Each generated model is matched to its corresponding polygon using the
model GeoPackage stem and the values in ``field_name``.
"""

from pathlib import Path

from hhnk_threedi_tools.breaches.clip_models.submodel_constants import SchematisationType
from hhnk_threedi_tools.breaches.clip_models.submodels import run_submodel as create_submodels
from hhnk_threedi_tools.breaches.clip_models.submodels_clean import run as clean_submodel


def run(
    schematisation_directory: str | Path,
    subareas_path: str | Path,
    field_name: str,
    calculation_grid_cells_path: str | Path,
    subareas_layer_name: str | None = None,
    calculation_grid_cells_layer_name: str | None = None,
    isolate_1d: bool = False,
    schematisation_type: SchematisationType = SchematisationType.RANA,
) -> list[Path]:

    model_gpkg_paths = create_submodels(
        schematisation_directory=schematisation_directory,
        subareas_path=subareas_path,
        field_name=field_name,
        calculation_grid_cells_path=calculation_grid_cells_path,
        subareas_layer_name=subareas_layer_name,
        calculation_grid_cells_layer_name=calculation_grid_cells_layer_name,
        isolate_1d=isolate_1d,
        schematisation_type=schematisation_type,
    )

    for model_gpkg_path in model_gpkg_paths:
        clean_submodel(
            model_gpkg_path=model_gpkg_path,
            polygon_path=subareas_path,
            field_name=field_name,
            schematisation_type=schematisation_type,
        )
    return model_gpkg_paths
