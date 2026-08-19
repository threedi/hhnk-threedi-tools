from pathlib import Path

from hhnk_threedi_tools.breaches.submodels_rana import Submodels
from hhnk_threedi_tools.breaches.submodel_constants import SchematisationType


def create_submodels_for_debug() -> Submodels:
    """Return a `Submodels` instance for debugger use.

    NOTE: This function is NOT called when importing the module. Call it
    interactively from your debugger to step into `Submodels.__init__`
    without accidentally running it in normal execution.
    """
    return Submodels(
        schematisation_directory=Path(r"H:\02.modellen\RegionalFloodModel\work in progress\schematisation"),
        subareas_path=Path(
            r"H:\03.resultaten\Overstromingsberekeningenprimairedoorbraken2024\deelgebieden\ROR PRI - dijktrajecten 13-8 en 13-9 - Stroom_NO.gpkg"
        ),
        field_name="Deelgebied",
        calculation_grid_cells_path=Path(r"H:\02.modellen\RegionalFloodModel\work in progress\regional_calculation_grid.gpkg"),
        subareas_layer_name=None,
        calculation_grid_cells_layer_name="cell",
        isolate_1d=True,
        schematisation_type=SchematisationType.THREEDI,
    )


if __name__ == "__main__":
    print(
        "Module provides `create_submodels_for_debug()` — import and call it from the debugger."
    )
