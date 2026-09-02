# Impermeable Surface Areas

Tools for creating 3Di `impervious_surface` and `impervious_surface_map` layers based on connection nodes and fixed drainage level areas (FDLA).

The workflow creates Voronoi-based subcatchments around valid 3Di connection nodes, clips them to FDLA polygons, assigns orphan polygons to neighbouring areas, calculates the weighted afvoernorm per surface, and writes the resulting impervious surface layers back to the model.

---

## Overview

This folder contains two scripts:

| File                             | Purpose                                                                         |
| -------------------------------- | ------------------------------------------------------------------------------- |
| `create_impervious_surface.py`   | Core reusable logic for creating impervious surface layers in a 3Di GeoPackage. |
| `run_impervious_surface_dcmb.py` | Runner for the Datachecker ModelBuilder workflow.                               |

---

## Workflow

1. Select valid 3Di connection nodes.
2. Assign connection nodes to FDLA polygons.
3. Create Voronoi subcatchments per FDLA.
4. Correct orphan Voronoi polygons.
5. Create the `impervious_surface` layer.
6. Calculate the weighted afvoernorm per surface.
7. Create the `impervious_surface_map` layer.
8. Write the result back to the model.

---

## `create_impervious_surface.py`

Core module for creating impervious surface layers directly in a 3Di GeoPackage.

Use this script when the model is already available as a GeoPackage, for example from 3Di tooling or during local development.

### Main steps

| Step                          | Description                                                                     |
| ----------------------------- | ------------------------------------------------------------------------------- |
| `get_nodes_within_fdla()`     | Selects connection nodes used by the network and assigns them to FDLA polygons. |
| `createa_voronoi_polygons()`  | Creates Voronoi polygons around connection nodes within each FDLA.              |
| `correct_voronoi_polygons()`  | Assigns orphan polygons to neighbouring polygons within the same FDLA.          |
| `create_surface_layer()`      | Creates the `impervious_surface` layer.                                         |
| `get_percentage_afvoernorm()` | Calculates the weighted afvoernorm using HDB `polders_v4`.                      |
| `create_surface_map_layer()`  | Creates lines from each surface to its assigned connection node.                |
| `update_model_geopackage()`   | Writes the new layers to the model GeoPackage.                                  |

### Usage

```python
from hhnk_threedi_tools.breaches.impermeable_surface_areas import create_impervious_surface

create_impervious_surface.run(
    model_path_gpkg=model_path_gpkg,
    datacheker_path=datachecker_path,
    polder_polygon_path=polder_polygon_path,
    hdb_path=hdb_path,
    sure_update=True,
)
```

### Required inputs

| Input                 | Description                                                               |
| --------------------- | ------------------------------------------------------------------------- |
| `model_path_gpkg`     | 3Di model GeoPackage.                                                     |
| `datachecker_path`    | Datachecker output GeoPackage containing `fixeddrainagelevelarea`.        |
| `polder_polygon_path` | Polygon layer used to clip the FDLA areas.                                |
| `hdb_path`            | HDB GeoPackage containing `polders_v4` and `Historische_afvoernorm_mm_d`. |

## `run_impervious_surface_dcmb.py`

Runner for the Datachecker ModelBuilder workflow.

This script is specific to the Datachecker ModelBuilder setup. In this workflow, the model starts as a 3Di SQLite and the generated impervious surface layers need to be exported back to that SQLite.

### Workflow

1. Initialize the required 3Di/QGIS dependencies.
2. Convert the 3Di SQLite to a GeoPackage.
3. Run the impervious surface generation workflow.
4. Clear existing impervious surface records in the SQLite.
5. Export the new `impervious_surface` and `impervious_surface_map` layers back to the SQLite.

### Use case

Use this runner only when the workflow starts from the Datachecker ModelBuilder.

Do not use this script for general HHNK 3Di tooling. For tooling, call `create_impervious_surface.run()` directly.

---

## Important notes

* Close QGIS, 3Di Modeller Interface, DB Browser, or any other program using the model GeoPackage before updating it.
* The `impervious_surface_map` lines are created from a representative point inside the polygon to the assigned connection node.
* The `percentage` field stores the weighted afvoernorm value used for the 3Di impervious surface map.
* Orphan Voronoi polygons are assigned to neighbouring polygons within the same FDLA based on shared boundary length.
* The Datachecker ModelBuilder runner assumes that the required input files are available in the expected `data` folder structure.

---

## Folder structure

```text
impermeable_surface_areas/
├── create_impervious_surface.py
├── run_impervious_surface_dcmb.py# Impermeable Surface Areas
