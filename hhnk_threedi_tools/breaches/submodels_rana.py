# %%
"""
submodels.py

Clips a 3Di schematisation (GeoPackage + SQLite + rasters) into spatial
sub-models based on a set of polygon sub-areas.

Supports two schematisation formats via the `schematisation_type` parameter:
- SchematisationType.RANA    (default) — newer RANA toolchain format
- SchematisationType.THREEDI — classic 3Di schematisation builder format

Usage
-
    from hhnk_threedi_tools.breaches.submodels import run
    from hhnk_threedi_tools.breaches.constants import SchematisationType

    run(
        schematisation_directory="path/to/schematisation",
        subareas_path="path/to/subareas.gpkg",
        field_name="Deelgebied",
        calculation_grid_cells_path="path/to/grid.gpkg",
        calculation_grid_cells_layer_name="cell",
        isolate_1d=True,
        schematisation_type=SchematisationType.RANA,
    )
"""

from __future__ import annotations

import os
import shutil
import tempfile
import warnings
from pathlib import Path

import fiona
import geopandas as gpd
import pandas as pd
import shapely
from osgeo import gdal, ogr, osr
from shapely import unary_union
from tqdm import tqdm

from hhnk_threedi_tools.breaches.exceptions import (
    FieldNameNotFoundError,
    GeoPackageFileNotFoundError,
    LayerNotFoundError,
    NoCalcGridCellsSelectedError,
    SchematisationFileNotFoundError,
    SQLiteFileNotFoundError,
    SubareaLayerEmptyError,
    SubareaNamesNotUniqueError,
)
from hhnk_threedi_tools.breaches.submodel_constants import COLUMNS_NAMES, LAYER_NAMES, SchematisationType

# %%
warnings.filterwarnings(
    "ignore",
    category=UserWarning,
    message="You are attempting to write an empty DataFrame to file.*",
)

gdal.UseExceptions()


# ---
# Main class
# ---


class Submodels:
    """Clips a 3Di schematisation into sub-models for each sub-area polygon."""

    def __init__(
        self,
        schematisation_directory: str | Path,
        subareas_path: str | Path,
        field_name: str,
        calculation_grid_cells_path: str | Path,
        subareas_layer_name: str | None = None,
        calculation_grid_cells_layer_name: str | None = None,
        isolate_1d: bool = False,
        schematisation_type: SchematisationType = SchematisationType.RANA,
    ) -> None:
        self.schematisation_directory = Path(schematisation_directory)
        self.subareas_path = Path(subareas_path)
        self.subareas_layer_name = subareas_layer_name
        self.field_name = field_name
        self.calculation_grid_cells_path = Path(calculation_grid_cells_path)
        self.calculation_grid_cells_layer_name = calculation_grid_cells_layer_name
        self.isolate_1d = isolate_1d
        self.layer_names = LAYER_NAMES[schematisation_type]
        self.columns_names = COLUMNS_NAMES[schematisation_type]

        # Locate required files / directories
        self.schematisation_gpkg = self._find_file("*.gpkg", GeoPackageFileNotFoundError)
        self.schematisation_sqlite = self._find_file("*.sqlite", SQLiteFileNotFoundError)
        self.rasters_directory = self._find_rasters_directory()

        # Load and validate sub-areas
        self._check_file_exists(self.subareas_path)
        self.subareas = self._read_to_gdf(self.subareas_path, self.subareas_layer_name)
        self._check_field_existence_and_uniqueness()

        # Load calculation-grid cells
        self._check_file_exists(self.calculation_grid_cells_path)
        self.calculation_grid_cells = self._read_to_gdf(
            self.calculation_grid_cells_path,
            self.calculation_grid_cells_layer_name,
        )

        # Align CRS of calculation grid to sub-areas
        if (
            self.calculation_grid_cells.crs is not None
            and self.subareas.crs is not None
            and self.calculation_grid_cells.crs != self.subareas.crs
        ):
            self.calculation_grid_cells = self.calculation_grid_cells.to_crs(self.subareas.crs)

        # Process each sub-area
        for _, subarea in tqdm(
            self.subareas.iterrows(),
            total=len(self.subareas),
            desc="Clipping sub-areas",
            unit="subarea",
        ):
            self._clip(subarea, schematisation_type)

    # helpers

    def _find_file(self, pattern: str, error_cls: type[Exception]) -> Path:
        """Return the first file matching *pattern* in the schematisation directory."""
        match = next(self.schematisation_directory.glob(pattern), None)
        if match is None:
            raise error_cls(f"No file matching '{pattern}' found in '{self.schematisation_directory}'.")
        return match

    def _find_rasters_directory(self) -> Path | None:
        """Return the 'rasters' sub-folder, or None if it does not exist."""
        rasters_dir = self.schematisation_directory / "rasters"
        return rasters_dir if rasters_dir.is_dir() else None

    def _check_file_exists(self, file_path: Path) -> None:
        if not file_path.exists():
            raise SchematisationFileNotFoundError(f"Required file not found: '{file_path}'.")

    def _read_to_gdf(self, path: Path, layer_name: str | None) -> gpd.GeoDataFrame:
        """Read a vector file into a GeoDataFrame."""
        if path.suffix.lower() == ".gpkg":
            try:
                return gpd.read_file(path, layer=layer_name)
            except Exception as exc:
                raise LayerNotFoundError(f"Layer '{layer_name}' not found in '{path}'.") from exc
        return gpd.read_file(path)

    def _check_field_existence_and_uniqueness(self) -> None:
        if self.subareas.empty:
            raise SubareaLayerEmptyError(f"Sub-areas file '{self.subareas_path}' contains no features.")
        if self.field_name not in self.subareas.columns:
            raise FieldNameNotFoundError(f"Field '{self.field_name}' not found in '{self.subareas_path}'.")
        if self.subareas[self.field_name].duplicated().any():
            raise SubareaNamesNotUniqueError(f"Values in field '{self.field_name}' are not unique.")

    # --
    # GeoPackage reading (via Fiona — preserves model IDs)
    # --

    def _read_geopackage_layers(
        self,
        gpkg_path: Path,
    ) -> dict[str, gpd.GeoDataFrame]:
        """Read all layers from a GeoPackage using Fiona.

        The 'id' column is always the ID used for filtering and relations:
        - RANA: Fiona feature ID
        - THREEDI: model 'id' property
        """

        layers_dict: dict[str, gpd.GeoDataFrame] = {}

        for layer_name in fiona.listlayers(gpkg_path):
            with fiona.open(gpkg_path, layer=layer_name) as src:
                records = list(src)
                crs = src.crs

            if records:
                gdf = gpd.GeoDataFrame.from_features(records, crs=crs)

                if self.schematisation_type == SchematisationType.RANA:
                    # RANA: use Fiona FID as id
                    gdf["id"] = [int(feat["id"]) for feat in records]

                elif self.schematisation_type == SchematisationType.THREEDI:
                    # THREEDI: keep properties["id"] from from_features()
                    # Only fallback to Fiona FID if the layer has no id property
                    if "id" not in gdf.columns:
                        gdf["id"] = [int(feat["id"]) for feat in records]

            else:
                gdf = gpd.read_file(
                    gpkg_path,
                    layer=layer_name,
                    engine="fiona",
                )

                if "id" not in gdf.columns:
                    gdf["id"] = pd.array([], dtype="int64")

            layers_dict[layer_name] = gdf

        return layers_dict

    @staticmethod
    def _write_layer(
        gdf: gpd.GeoDataFrame,
        gpkg_path: Path,
        layer_name: str,
    ) -> None:
        if gdf.empty:
            return

        # IDs a mantener
        valid_ids = set(gdf["id"].astype(int))

        # Abrir con GDAL y eliminar features que no están en valid_ids
        ds = ogr.Open(str(gpkg_path), update=1)  # update=1 → escritura
        layer = ds.GetLayerByName(layer_name)

        ids_to_delete = []
        for feature in layer:
            if feature.GetFID() not in valid_ids:
                ids_to_delete.append(feature.GetFID())

        for fid in ids_to_delete:
            layer.DeleteFeature(fid)

        ds = None  # cerrar

    # Spatial helpers

    @staticmethod
    def _spatial_join(
        layer: gpd.GeoDataFrame,
        mask: gpd.GeoDataFrame,
        how: str,
        predicate: str,
    ) -> gpd.GeoDataFrame:
        """Spatial join that returns only the original columns (no join artefacts)."""
        original_columns = layer.columns.tolist()
        joined = gpd.sjoin(layer, mask, how=how, predicate=predicate, rsuffix="_mask")
        return joined[original_columns]

    # clip raster

    def _clip_raster(
        self,
        input_path: Path,
        mask_geometry: shapely.Geometry,
        output_path: Path,
    ) -> None:
        """Clip a GeoTIFF raster to a Shapely polygon mask."""

        src_ds = gdal.Open(str(input_path))
        if src_ds is None:
            raise RuntimeError(f"Cannot open raster: {input_path}")

        input_crs = osr.SpatialReference()
        input_crs.ImportFromWkt(src_ds.GetProjection())

        # Force 2-D geometry
        mask_geom_2d = shapely.force_2d(mask_geometry)
        if mask_geom_2d.is_empty:
            raise RuntimeError("Mask geometry is empty.")

        # Build a single-feature GeoDataFrame for the cutline
        gdf = gpd.GeoDataFrame(
            geometry=[mask_geom_2d],
            crs=input_crs.ExportToWkt(),
        )
        gdf = gdf.dissolve()
        gdf["geometry"] = gdf.geometry.buffer(0)

        if gdf.geometry.iloc[0].is_empty:
            raise RuntimeError("Mask geometry is empty after dissolve + buffer(0).")

        # Write cutline to a temporary GeoJSON
        with tempfile.NamedTemporaryFile(delete=False, suffix=".geojson") as tmp:
            cutline_path = tmp.name

        try:
            gdf.to_file(cutline_path, driver="GeoJSON")

            # Resolve actual OGR layer name (varies across GDAL versions)
            cutline_ds = ogr.Open(cutline_path)
            if cutline_ds is None:
                raise RuntimeError(f"Cannot open cutline file: {cutline_path}")
            cutline_layer_name = cutline_ds.GetLayer(0).GetName()
            cutline_ds = None

            options = gdal.WarpOptions(
                format="GTiff",
                cutlineDSName=cutline_path,
                cutlineLayer=cutline_layer_name,
                cropToCutline=True,
                dstSRS=input_crs.ExportToWkt(),
                creationOptions=["COMPRESS=DEFLATE"],
            )

            clipped_ds = gdal.Warp(str(output_path), src_ds, options=options)
            if clipped_ds is None:
                raise RuntimeError("gdal.Warp failed without an error message.")

            src_ds = None
            clipped_ds = None

        finally:
            try:
                os.remove(cutline_path)
            except OSError:
                pass

    # clip layers per subarea

    def _clip(self, subarea: pd.Series, schematisation_type) -> None:
        """Clip all schematisation data for a single sub-area."""

        ln = self.layer_names
        name: str = subarea[self.field_name]

        #  Output directories
        output_directory = self.schematisation_directory / name
        output_directory.mkdir(parents=True, exist_ok=True)

        output_gpkg = output_directory / (self.schematisation_gpkg.stem + "_" + name + self.schematisation_gpkg.suffix)
        output_sqlite = output_directory / (
            self.schematisation_sqlite.stem + "_" + name + self.schematisation_sqlite.suffix
        )

        # Copy base schematisation files.
        # This preserves the original GeoPackage schema (including primary keys,
        # column types and empty layers) for all layers before any filtering.
        shutil.copy(self.schematisation_gpkg, output_gpkg)
        shutil.copy(self.schematisation_sqlite, output_sqlite)

        #  Read all layers from the copied GeoPackage
        layers = self._read_geopackage_layers(output_gpkg)

        connection_node = layers[ln["connection_node"]]
        pipe = layers[ln["pipe"]]
        weir = layers[ln["weir"]]
        orifice = layers[ln["orifice"]]
        culvert = layers[ln["culvert"]]
        cross_section_loc = layers[ln["cross_section_location"]]
        channel = layers[ln["channel"]]
        pump_map = layers[ln["pump_map"]]
        pump = layers[ln["pump"]]
        bc_1d = layers[ln["boundary_condition_1d"]]
        bc_2d = layers[ln["boundary_condition_2d"]]
        lateral_1d = layers[ln["lateral_1d"]]
        lateral_2d = layers[ln["lateral_2d"]]
        surface_map = layers[ln["surface_map"]]
        surface = layers[ln["surface"]]
        obstacle = layers[ln["obstacle"]]
        potential_breach = layers[ln["potential_breach"]]
        exchange_line = layers[ln["exchange_line"]]
        grid_ref_line = layers[ln["grid_refinement_line"]]
        grid_ref_area = layers[ln["grid_refinement_area"]]

        # Sub-area as single-row GeoDataFrame
        subarea_gdf = gpd.GeoDataFrame(subarea.to_frame().T, geometry="geometry", crs=self.subareas.crs)

        # column name
        cn = self.columns_names

        # Select connection nodes inside the sub-area
        filtered_cn = self._spatial_join(connection_node, subarea_gdf, how="inner", predicate="intersects")
        valid_cn_ids = set(filtered_cn["id"])

        # Filter 1-D structures by their endpoint connection nodes
        filtered_pump = pump[pump["connection_node_id"].isin(valid_cn_ids)]

        filtered_pipe = pipe[
            pipe[cn["connection_node_id_start"]].isin(valid_cn_ids)
            & pipe[cn["connection_node_id_end"]].isin(valid_cn_ids)
        ]
        filtered_weir = weir[
            weir[cn["connection_node_id_start"]].isin(valid_cn_ids)
            & weir[cn["connection_node_id_end"]].isin(valid_cn_ids)
        ]
        filtered_orifice = orifice[
            orifice[cn["connection_node_id_start"]].isin(valid_cn_ids)
            & orifice[cn["connection_node_id_end"]].isin(valid_cn_ids)
        ]
        filtered_culvert = culvert[
            culvert[cn["connection_node_id_start"]].isin(valid_cn_ids)
            & culvert[cn["connection_node_id_end"]].isin(valid_cn_ids)
        ]
        filtered_pump_map = pump_map[pump_map[cn["connection_node_id_end"]].isin(valid_cn_ids)]
        filtered_channel = channel[
            channel[cn["connection_node_id_start"]].isin(valid_cn_ids)
            & channel[cn["connection_node_id_end"]].isin(valid_cn_ids)
        ]
        filtered_cross_section_loc = cross_section_loc[cross_section_loc["channel_id"].isin(filtered_channel["id"])]

        # Rebuild connection-node set from connected structures only
        # Removes 'floating' nodes not actually connected to any element.
        connected_cn_ids: set = set()
        for structure in (
            filtered_channel,
            filtered_pipe,
            filtered_orifice,
            filtered_culvert,
            filtered_weir,
            filtered_pump,
        ):
            for col in ("connection_node_id", cn["connection_node_id_start"], cn["connection_node_id_end"]):
                if col in structure.columns:
                    connected_cn_ids.update(structure[col].dropna())

        filtered_cn = connection_node[connection_node["id"].isin(connected_cn_ids)]
        valid_cn_ids = set(filtered_cn["id"])

        # Filter remaining 1-D elements
        filtered_bc_1d = bc_1d[bc_1d["connection_node_id"].isin(valid_cn_ids)]
        filtered_lateral_1d = lateral_1d[lateral_1d["connection_node_id"].isin(valid_cn_ids)]

        filtered_surface_map = surface_map[surface_map["connection_node_id"].isin(valid_cn_ids)]
        filtered_surface = surface[surface["id"].isin(filtered_surface_map["surface_id"])]

        if schematisation_type == SchematisationType.THREEDI:
            impervious_surface_map = layers["impervious_surface_map"]
            impervious_surface = layers["impervious_surface"]

            filtered_impervious_surface_map = impervious_surface_map[
                impervious_surface_map["connection_node_id"].isin(valid_cn_ids)
            ]

            filtered_impervious_surface = impervious_surface[
                impervious_surface["id"].isin(filtered_impervious_surface_map["impervious_surface_id"])
            ]

        # Exchange lines: channel must exist AND geometry must intersect sub-area
        temp_exchange_line = exchange_line[exchange_line["channel_id"].isin(filtered_channel["id"])]
        filtered_exchange_line = self._spatial_join(
            temp_exchange_line, subarea_gdf, how="inner", predicate="intersects"
        )

        filtered_lateral_2d = self._spatial_join(lateral_2d, subarea_gdf, how="inner", predicate="within")
        filtered_bc_2d = self._spatial_join(bc_2d, subarea_gdf, how="inner", predicate="within")
        filtered_potential_breach = self._spatial_join(
            potential_breach, subarea_gdf, how="inner", predicate="intersects"
        )
        filtered_obstacle = self._spatial_join(obstacle, subarea_gdf, how="inner", predicate="intersects")
        filtered_grid_ref_line = self._spatial_join(grid_ref_line, subarea_gdf, how="inner", predicate="intersects")
        filtered_grid_ref_area = self._spatial_join(grid_ref_area, subarea_gdf, how="inner", predicate="intersects")

        # Isolate 1-D elements outside sub-area
        if self.isolate_1d:
            isolated_pipe = pipe[~pipe["id"].isin(filtered_pipe["id"])].copy()
            isolated_culvert = culvert[~culvert["id"].isin(filtered_culvert["id"])].copy()
            isolated_channel = channel[~channel["id"].isin(filtered_channel["id"])].copy()

            isolated_pipe["exchange_type"] = 101
            isolated_culvert["exchange_type"] = 101
            isolated_channel["exchange_type"] = 101

            filtered_pipe = gpd.GeoDataFrame(pd.concat([filtered_pipe, isolated_pipe], ignore_index=True))
            filtered_culvert = gpd.GeoDataFrame(pd.concat([filtered_culvert, isolated_culvert], ignore_index=True))
            filtered_channel = gpd.GeoDataFrame(pd.concat([filtered_channel, isolated_channel], ignore_index=True))

            # Restore all other 1-D elements to the full original set
            filtered_cn = connection_node
            filtered_pump = pump
            filtered_weir = weir
            filtered_orifice = orifice
            filtered_pump_map = pump_map
            filtered_cross_section_loc = cross_section_loc
            filtered_bc_1d = bc_1d
            filtered_lateral_1d = lateral_1d
            filtered_surface_map = surface_map
            if schematisation_type == SchematisationType.THREEDI:
                filtered_impervious_surface_map = impervious_surface_map
                filtered_impervious_surface = impervious_surface

        # print(filtered_channel[["id", cn["connection_node_id_start"], cn["connection_node_id_end"]]].head(10))
        # Write filtered layers to the output GeoPackage
        # _write_layer() drops the helper 'id' column and skips empty layers
        # to preserve the original GeoPackage schema from shutil.copy().
        write_pairs = [
            (filtered_cn, ln["connection_node"]),
            (filtered_pipe, ln["pipe"]),
            (filtered_weir, ln["weir"]),
            (filtered_orifice, ln["orifice"]),
            (filtered_culvert, ln["culvert"]),
            (filtered_cross_section_loc, ln["cross_section_location"]),
            (filtered_channel, ln["channel"]),
            (filtered_pump_map, ln["pump_map"]),
            (filtered_pump, ln["pump"]),
            (filtered_bc_1d, ln["boundary_condition_1d"]),
            (filtered_bc_2d, ln["boundary_condition_2d"]),
            (filtered_lateral_1d, ln["lateral_1d"]),
            (filtered_lateral_2d, ln["lateral_2d"]),
            (filtered_surface_map, ln["surface_map"]),
            (filtered_surface, ln["surface"]),
            (filtered_obstacle, ln["obstacle"]),
            (filtered_potential_breach, ln["potential_breach"]),
            (filtered_exchange_line, ln["exchange_line"]),
            (filtered_grid_ref_line, ln["grid_refinement_line"]),
            (filtered_grid_ref_area, ln["grid_refinement_area"]),
        ]

        if schematisation_type == SchematisationType.THREEDI:
            write_pairs.extend(
                [
                    (filtered_impervious_surface_map, "impervious_surface_map"),
                    (filtered_impervious_surface, "impervious_surface"),
                ]
            )

        for gdf, layer_name in write_pairs:
            self._write_layer(gdf, output_gpkg, layer_name)

        # Clip rasters (if present)
        if self.rasters_directory is None:
            return

        tif_files = [
            f for f in self.rasters_directory.iterdir() if f.is_file() and f.suffix.lower() in {".tif", ".tiff"}
        ]
        if not tif_files:
            return

        intersecting_cells = self.calculation_grid_cells[self.calculation_grid_cells.intersects(subarea.geometry)]
        if intersecting_cells.empty:
            raise NoCalcGridCellsSelectedError(
                f"No calculation-grid cells intersect sub-area '{name}'. Check your sub-area extent."
            )

        dissolved_mask = unary_union(intersecting_cells.geometry)

        output_rasters_dir = output_directory / "rasters"
        output_rasters_dir.mkdir(parents=True, exist_ok=True)

        for tif_path in tif_files:
            self._clip_raster(tif_path, dissolved_mask, output_rasters_dir / tif_path.name)


# Public API


def run(
    schematisation_directory: str | Path,
    subareas_path: str | Path,
    field_name: str,
    calculation_grid_cells_path: str | Path,
    subareas_layer_name: str | None = None,
    calculation_grid_cells_layer_name: str | None = None,
    isolate_1d: bool = False,
    schematisation_type: SchematisationType = SchematisationType.RANA,
) -> None:
    """Entry point for creating sub-models from a 3Di schematisation.

    Parameters
    --
    schematisation_directory:
        Folder containing the .gpkg, .sqlite and optional rasters/ sub-folder.
    subareas_path:
        Vector file with sub-area polygons.
    field_name:
        Column in *subareas_path* with unique sub-area names.
    calculation_grid_cells_path:
        Vector file with 3Di calculation-grid cells.
    subareas_layer_name:
        Layer name inside *subareas_path* (GeoPackage only).
    calculation_grid_cells_layer_name:
        Layer name inside *calculation_grid_cells_path* (GeoPackage only).
    isolate_1d:
        If True, 1-D elements outside the sub-area are kept but their
        exchange_type is set to 101 (isolated) instead of being removed.
    schematisation_type:
        SchematisationType.RANA (default) or SchematisationType.THREEDI.
        Controls which GeoPackage layer names are used for reading and writing.
    """
    Submodels(
        schematisation_directory=schematisation_directory,
        subareas_path=subareas_path,
        field_name=field_name,
        calculation_grid_cells_path=calculation_grid_cells_path,
        subareas_layer_name=subareas_layer_name,
        calculation_grid_cells_layer_name=calculation_grid_cells_layer_name,
        isolate_1d=isolate_1d,
        schematisation_type=schematisation_type,
    )


# %%
if __name__ == "__main__":
    run(
        schematisation_directory=Path(r"H:\02.modellen\RegionalFloodModel\work in progress\schematisation"),
        subareas_path=r"H:\03.resultaten\Overstromingsberekeningenprimairedoorbraken2024\deelgebieden\ROR PRI - dijktrajecten 13-8 en 13-9 - Stroom_NO.gpkg",
        field_name="Deelgebied",
        calculation_grid_cells_path=r"H:\02.modellen\RegionalFloodModel\work in progress\regional_calculation_grid.gpkg",
        subareas_layer_name=None,
        calculation_grid_cells_layer_name="cell",
        isolate_1d=True,
        schematisation_type=SchematisationType.THREEDI,
    )

# %%

gpkg_path = r"H:\02.modellen\RegionalFloodModel\work in progress\schematisation\ROR PRI - dijktrajecten 13-8 en 13-9 - Stroom_NO\RegionalFloodModel_ROR PRI - dijktrajecten 13-8 en 13-9 - Stroom_NO.gpkg"
polygon_path = r"H:\03.resultaten\Overstromingsberekeningenprimairedoorbraken2024\deelgebieden\ROR PRI - dijktrajecten 13-8 en 13-9 - Stroom_NO.gpkg"
from hhnk_threedi_tools.breaches.submodel_constants import COLUMNS_NAMES, LAYER_NAMES, SchematisationType

list_layers = ["connection_node", "1d_boundary_condition", "orifice", "cross_section_location", "channel"]


def read_geopackage_layers(
    gpkg_path: Path,
    schematisation_type: SchematisationType,
    selected_layers: bool = False,
    list_layers: list | None = None,
) -> dict[str, gpd.GeoDataFrame]:

    layers_dict: dict[str, gpd.GeoDataFrame] = {}

    if selected_layers:
        if list_layers is None:
            raise ValueError("list_layers must be provided when selected_layers=True")
        layer_names = list_layers
    else:
        layer_names = fiona.listlayers(gpkg_path)

    for layer_name in layer_names:
        with fiona.open(gpkg_path, layer=layer_name) as src:
            records = list(src)
            crs = src.crs

        if records:
            gdf = gpd.GeoDataFrame.from_features(records, crs=crs)

            if schematisation_type == SchematisationType.RANA:
                # RANA: use Fiona feature ID
                gdf["id"] = [int(feat["id"]) for feat in records]

            elif schematisation_type == SchematisationType.THREEDI:
                # THREEDI: from_features() already reads properties["id"]
                # Only use Fiona ID if the layer has no 'id' property
                if "id" not in gdf.columns:
                    gdf["id"] = [int(feat["id"]) for feat in records]

        else:
            gdf = gpd.read_file(
                gpkg_path,
                layer=layer_name,
                engine="fiona",
            )

            if "id" not in gdf.columns:
                gdf["id"] = pd.array([], dtype="int64")

        layers_dict[layer_name] = gdf

    return layers_dict


# %%


def clean_geopackge(polygon_path):

    cn = COLUMNS_NAMES[SchematisationType.THREEDI]

    polygon_gdf = gpd.read_file(polygon_path)

    layers_dict = read_geopackage_layers(
        gpkg_path=gpkg_path,
        schematisation_type=SchematisationType.THREEDI,
        selected_layers=True,
        list_layers=list_layers,
    )

    boundary_condition = layers_dict["1d_boundary_condition"]
    connection_node = layers_dict["connection_node"]
    orifice = layers_dict["orifice"]
    cross_section_location = layers_dict["cross_section_location"]
    channel = layers_dict["channel"]

    # Boundary conditions outside polygon
    boundary_condition_overlay = gpd.overlay(
        boundary_condition,
        polygon_gdf[["geometry"]],
        how="intersection",
    )

    bc_out_of_intersection = boundary_condition.loc[
        ~boundary_condition["id"].isin(boundary_condition_overlay["id"])
    ].copy()

    bc_condition_connection_node_id = bc_out_of_intersection["connection_node_id"].tolist()

    # Orifices connected to those BC connection nodes
    orifice_out_of_intersection = orifice.loc[
        orifice[cn["connection_node_id_end"]].isin(bc_condition_connection_node_id)
    ].copy()

    orifice_connection_node_start = orifice_out_of_intersection[cn["connection_node_id_start"]].tolist()

    # Connection nodes Start of those orifices
    connection_node_selected = connection_node.loc[connection_node["id"].isin(orifice_connection_node_start)].copy()

    # Buffer around them
    connection_node_buffer = connection_node_selected.copy()
    connection_node_buffer["geometry"] = connection_node_buffer.geometry.buffer(0.1)

    bc_out_of_intersection_buffer = bc_out_of_intersection.copy()
    bc_out_of_intersection_buffer["geometry"] = bc_out_of_intersection_buffer.geometry.buffer(0.1)

    channel_orifice_overlay = gpd.overlay(
        channel,
        connection_node_buffer[["geometry"]],
        how="intersection",
    )

    channel_bc_overlay = gpd.overlay(
        channel,
        bc_out_of_intersection_buffer[["geometry"]],
        how="intersection",
    )

    # Recover original channel records
    channel_selected = channel.loc[
        channel["id"].isin(channel_orifice_overlay["id"]) | channel["id"].isin(channel_bc_overlay["id"])
    ].copy()

    connection_node_channel_start = channel_selected[cn["connection_node_id_start"]].to_list()
    connection_node_channel_end = channel_selected[cn["connection_node_id_end"]].to_list()
    connection_node_bc = bc_out_of_intersection["connection_node_id"].to_list()
    connection_node_selected = connection_node.loc[
        connection_node["id"].isin(connection_node_channel_start)
        | connection_node["id"].isin(connection_node_channel_end)
        | connection_node["id"].isin(connection_node_bc)
    ].copy()

    channel_buffer = channel_selected.copy()
    channel_buffer = channel_buffer.set_geometry(channel_buffer.geometry.buffer(0.1))
    crosssection_selection = cross_section_location.overlay(channel_buffer[["geometry"]])

    crosssection_overlay = gpd.overlay(
        cross_section_location,
        channel_buffer[["geometry"]],
        how="intersection",
    )
    # Recover original cross-section records
    crosssection_selection = cross_section_location.loc[
        cross_section_location["id"].isin(crosssection_overlay["id"])
    ].copy()

    return {
        "1d_boundary_condition": bc_out_of_intersection["id"].tolist(),
        "orifice": orifice_out_of_intersection["id"].tolist(),
        "connection_node": connection_node_selected["id"].tolist(),
        "channel": channel_selected["id"].tolist(),
        "cross_section_location": crosssection_selection["id"].tolist(),
    }


def remove_selection(
    gpkg_path,
    layer_name,
    ids_to_remove,
    schematisation_type,
):
    ds = ogr.Open(str(gpkg_path), update=1)
    layer = ds.GetLayerByName(layer_name)

    fids_to_remove = []

    for feature in layer:
        if schematisation_type == SchematisationType.RANA:
            feature_id = feature.GetFID()
        else:
            feature_id = feature.GetField("id")

        if feature_id in ids_to_remove:
            fids_to_remove.append(feature.GetFID())

    # Close/reset the active read cursor before deleting
    layer.ResetReading()

    for fid in fids_to_remove:
        layer.DeleteFeature(fid)

    layer = None
    ds = None


remove_dict = clean_geopackge(polygon_path)

for layer_name, ids in remove_dict.items():
    remove_selection(
        gpkg_path,
        layer_name,
        ids,
        SchematisationType.THREEDI,
    )
# %%
