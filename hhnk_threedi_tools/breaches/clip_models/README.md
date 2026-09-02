# 3Di Submodel Clipping

Tools for creating spatial submodels from a 3Di schematisation.

It allows the user to define one or more areas of interest and create a separate 3Di submodel for each of them. These areas are supplied as polygons in a vector dataset, preferably a GeoPackage.

A polygon can represent any area for which a separate submodel is required, for example a polder, catchment, flood compartment or project area.

Each polygon must contain a unique name in an attribute column. The column containing these names is specified through the `field_name` parameter.

For example, a GeoPackage could contain:

| Deelgebied |
| --- |
| Stroom_NO |
| Stroom_ZW |
| Stroom_ZO |

and the tool would be called with:

```python
field_name="Deelgebied"
```

In this example, three separate submodels will be created.

The value in `Deelgebied` is used as the name of the corresponding submodel and its output directory.

## Supported schematisation types

The tool supports:

- `SchematisationType.RANA`
- `SchematisationType.THREEDI`

Layer and column names that differ between the two formats are handled through the mappings defined in `submodel_constants.py`.

## Workflow

For each polygon in the subareas dataset, the tool:

1. Creates an output directory for the submodel.
2. Copies the source GeoPackage and SQLite database.
3. Selects the model elements belonging to the subarea.
4. Cleans model elements around the submodel boundary.
5. Optionally isolates 1D elements outside the subarea.
6. Clips available rasters to the calculation grid cells intersecting the subarea.

The generated GeoPackage keeps the original GeoPackage structure and schema. Features that do not belong to the submodel are removed from the copied model.

## Required input data

The tool requires three main inputs:

1. A source folder containing the 3Di schematisation.
2. A polygon dataset defining the desired submodel areas.
3. A dataset containing the 3Di calculation grid cells.

### Source schematisation

The source schematisation is provided through `schematisation_directory`.

The directory must contain:

- the schematisation GeoPackage;
- the corresponding SQLite database;
- optionally, a `rasters` directory containing GeoTIFF rasters.

For example:

```text
schematisation/
├── RegionalFloodModel.gpkg
├── RegionalFloodModel.sqlite
└── rasters/
    ├── dem.tif
    ├── friction.tif
    └── infiltration.tif
```

### Submodel areas

The desired submodel areas are provided through `subareas_path`.

The dataset contains the polygons that define the spatial extent of the submodels. Each polygon can represent any area for which a separate model is required.

The attribute column containing the submodel names is specified through `field_name`.

If the dataset is a GeoPackage containing multiple layers, the relevant polygon layer can be specified through `subareas_layer_name`.

### Calculation grid cells

The 3Di calculation grid cells are provided through `calculation_grid_cells_path`.

For each submodel, all calculation grid cells that intersect the submodel polygon are selected. These cells are dissolved into a single geometry and used as the clipping mask for the schematisation rasters.

As a result, the raster extent follows the selected calculation grid cells rather than being clipped exactly to the submodel polygon.

If the calculation grid cells are stored in a GeoPackage containing multiple layers, the relevant layer can be specified through `calculation_grid_cells_layer_name`.

## Usage

The main entry point is the `run` function from `create_clip_models.py`.

A basic example is shown below:

```python
from pathlib import Path

from hhnk_threedi_tools.breaches.clip_models.create_clip_models import (
    SchematisationType,
    run,
)

schematisation_directory = Path(
    r"path/to/schematisation"
)

subareas_path = Path(
    r"path/to/subareas.gpkg"
)

calculation_grid_cells_path = Path(
    r"path/to/calculation_grid.gpkg"
)

model_gpkg_paths = run(
    schematisation_directory=schematisation_directory,
    subareas_path=subareas_path,
    field_name="Deelgebied",
    calculation_grid_cells_path=calculation_grid_cells_path,
    subareas_layer_name=None,
    calculation_grid_cells_layer_name="cell",
    isolate_1d=True,
    schematisation_type=SchematisationType.THREEDI,
)
```

The function returns a list containing the paths to the generated submodel GeoPackages.

For example:

```python
[
    Path(".../Stroom_NO/RegionalFloodModel_Stroom_NO.gpkg"),
    Path(".../Stroom_ZW/RegionalFloodModel_Stroom_ZW.gpkg"),
    Path(".../Stroom_ZO/RegionalFloodModel_Stroom_ZO.gpkg"),
]
```
