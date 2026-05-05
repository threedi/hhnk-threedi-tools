"""

Clips a Rana Model (GeoPackage + SQLite + rasters) into spatial
sub-models based on a set of polygon sub-areas.

Usage
-----
    from hhnk_threedi_tools.breaches.submodels import run

    run(
        schematisation_directory="path/to/schematisation",
        subareas_path="path/to/subareas.gpkg",
        field_name="Deelgebied",
        calculation_grid_cells_path="path/to/grid.gpkg",
        calculation_grid_cells_layer_name="cell",
        isolate_1d=True,
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

warnings.filterwarnings(
    "ignore",
    category=UserWarning,
    message="You are attempting to write an empty DataFrame to file.*",
)

gdal.UseExceptions()


# ---------------------------------------------------------------------------
# Layer name mapping
# ---------------------------------------------------------------------------
# Maps internal variable names to GeoPackage layer names.
# Read and write use the same name.

LAYER_NAMES: dict[str, str] = {
    "connection_node": "connection_node",
    "pipe": "pipe",
    "weir": "weir",
    "orifice": "orifice",
    "culvert": "culvert",
    "cross_section_location": "cross_section_location",
    "channel": "channel",
    "pump_map": "pump_map",
    "pump": "pump",
    "boundary_condition_1d": "boundary_condition_1d",
    "boundary_condition_2d": "boundary_condition_2d",
    "lateral_1d": "lateral_1d",
    "lateral_2d": "lateral_2d",
    "surface_map": "surface_map",
    "surface": "surface",
    "obstacle": "obstacle",
    "potential_breach": "potential_breach",
    "exchange_line": "exchange_line",
    "grid_refinement_line": "grid_refinement_line",
    "grid_refinement_area": "grid_refinement_area",
}


# Main class


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
    ) -> None:
        self.schematisation_directory = Path(schematisation_directory)
        self.subareas_path = Path(subareas_path)
        self.subareas_layer_name = subareas_layer_name
        self.field_name = field_name
        self.calculation_grid_cells_path = Path(calculation_grid_cells_path)
        self.calculation_grid_cells_layer_name = calculation_grid_cells_layer_name
        self.isolate_1d = isolate_1d

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
            self._clip(subarea)

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

    # GeoPackage reading (via Fiona — preserves model IDs)

    def _read_geopackage_layers(self, gpkg_path: Path) -> dict[str, gpd.GeoDataFrame]:
        """Read all layers from a GeoPackage using Fiona to preserve feature IDs.

        The 'id' column is added to every layer regardless of whether it has
        features, so downstream filtering on 'id' never raises a KeyError.
        """
        layers_dict: dict[str, gpd.GeoDataFrame] = {}

        for layer_name in fiona.listlayers(gpkg_path):
            with fiona.open(gpkg_path, layer=layer_name) as src:
                records = list(src)
                crs = src.crs

            if records:
                gdf = gpd.GeoDataFrame.from_features(records, crs=crs)
                gdf["id"] = [int(feat["id"]) for feat in records]
            else:
                gdf = gpd.read_file(gpkg_path, layer=layer_name, driver="GPKG")
                if "id" not in gdf.columns:
                    gdf["id"] = pd.array([], dtype="int64")

            layers_dict[layer_name] = gdf

        return layers_dict

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

    # ------------------------------------------------------------------
    # Raster clipping
    # ------------------------------------------------------------------

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

    # Core clip logic

    def _clip(self, subarea: pd.Series) -> None:
        """Clip all schematisation data for a single sub-area."""

        name: str = subarea[self.field_name]

        # ---- Output directories ----
        output_directory = self.schematisation_directory / name
        output_directory.mkdir(parents=True, exist_ok=True)

        output_gpkg = output_directory / (self.schematisation_gpkg.stem + "_" + name + self.schematisation_gpkg.suffix)
        output_sqlite = output_directory / (
            self.schematisation_sqlite.stem + "_" + name + self.schematisation_sqlite.suffix
        )

        # Copy base schematisation files
        shutil.copy(self.schematisation_gpkg, output_gpkg)
        shutil.copy(self.schematisation_sqlite, output_sqlite)

        # ---- Read all layers from the copied GeoPackage ----
        layers = self._read_geopackage_layers(output_gpkg)

        connection_node = layers[LAYER_NAMES["connection_node"]]
        pipe = layers[LAYER_NAMES["pipe"]]
        weir = layers[LAYER_NAMES["weir"]]
        orifice = layers[LAYER_NAMES["orifice"]]
        culvert = layers[LAYER_NAMES["culvert"]]
        cross_section_loc = layers[LAYER_NAMES["cross_section_location"]]
        channel = layers[LAYER_NAMES["channel"]]
        pump_map = layers[LAYER_NAMES["pump_map"]]
        pump = layers[LAYER_NAMES["pump"]]
        bc_1d = layers[LAYER_NAMES["boundary_condition_1d"]]
        bc_2d = layers[LAYER_NAMES["boundary_condition_2d"]]
        lateral_1d = layers[LAYER_NAMES["lateral_1d"]]
        lateral_2d = layers[LAYER_NAMES["lateral_2d"]]
        surface_map = layers[LAYER_NAMES["surface_map"]]
        surface = layers[LAYER_NAMES["surface"]]
        obstacle = layers[LAYER_NAMES["obstacle"]]
        potential_breach = layers[LAYER_NAMES["potential_breach"]]
        exchange_line = layers[LAYER_NAMES["exchange_line"]]
        grid_ref_line = layers[LAYER_NAMES["grid_refinement_line"]]
        grid_ref_area = layers[LAYER_NAMES["grid_refinement_area"]]

        # Sub-area as single-row GeoDataFrame
        subarea_gdf = gpd.GeoDataFrame(subarea.to_frame().T, geometry="geometry", crs=self.subareas.crs)

        # ---- Step 1: Select connection nodes inside the sub-area ----
        filtered_cn = self._spatial_join(connection_node, subarea_gdf, how="inner", predicate="intersects")
        valid_cn_ids = set(filtered_cn["id"])

        # ---- Step 2: Filter 1-D structures by their endpoint connection nodes ----
        filtered_pump = pump[pump["connection_node_id"].isin(valid_cn_ids)]

        filtered_pipe = pipe[
            pipe["connection_node_id_start"].isin(valid_cn_ids) & pipe["connection_node_id_end"].isin(valid_cn_ids)
        ]
        filtered_weir = weir[
            weir["connection_node_id_start"].isin(valid_cn_ids) & weir["connection_node_id_end"].isin(valid_cn_ids)
        ]
        filtered_orifice = orifice[
            orifice["connection_node_id_start"].isin(valid_cn_ids)
            & orifice["connection_node_id_end"].isin(valid_cn_ids)
        ]
        filtered_culvert = culvert[
            culvert["connection_node_id_start"].isin(valid_cn_ids)
            & culvert["connection_node_id_end"].isin(valid_cn_ids)
        ]
        filtered_pump_map = pump_map[pump_map["connection_node_id_end"].isin(valid_cn_ids)]
        filtered_channel = channel[
            channel["connection_node_id_start"].isin(valid_cn_ids)
            & channel["connection_node_id_end"].isin(valid_cn_ids)
        ]
        filtered_cross_section_loc = cross_section_loc[cross_section_loc["channel_id"].isin(filtered_channel["id"])]

        # ---- Step 3: Rebuild connection-node set from connected structures only ----
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
            for col in ("connection_node_id", "connection_node_id_start", "connection_node_id_end"):
                if col in structure.columns:
                    connected_cn_ids.update(structure[col].dropna())

        filtered_cn = connection_node[connection_node["id"].isin(connected_cn_ids)]
        valid_cn_ids = set(filtered_cn["id"])

        # ---- Step 4: Filter remaining 1-D elements ----
        filtered_bc_1d = bc_1d[bc_1d["connection_node_id"].isin(valid_cn_ids)]
        filtered_lateral_1d = lateral_1d[lateral_1d["connection_node_id"].isin(valid_cn_ids)]
        filtered_surface_map = surface_map[surface_map["connection_node_id"].isin(valid_cn_ids)]
        filtered_surface = surface[surface["id"].isin(filtered_surface_map["surface_id"])]

        # ---- Step 5: Filter 2-D / spatial elements ----
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

        # ---- Step 6 (optional): Isolate 1-D elements outside sub-area ----
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

        # ---- Step 7: Drop the helper 'id' column before writing ----
        # 'id' was added during reading to enable filtering; must not be
        # written back to avoid schema conflicts with the original GeoPackage.
        def _drop_id(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
            return gdf.drop(columns=["id"], errors="ignore")

        # ---- Step 8: Write filtered layers to the output GeoPackage ----
        write_pairs = [
            (filtered_cn, LAYER_NAMES["connection_node"]),
            (filtered_pipe, LAYER_NAMES["pipe"]),
            (filtered_weir, LAYER_NAMES["weir"]),
            (filtered_orifice, LAYER_NAMES["orifice"]),
            (filtered_culvert, LAYER_NAMES["culvert"]),
            (filtered_cross_section_loc, LAYER_NAMES["cross_section_location"]),
            (filtered_channel, LAYER_NAMES["channel"]),
            (filtered_pump_map, LAYER_NAMES["pump_map"]),
            (filtered_pump, LAYER_NAMES["pump"]),
            (filtered_bc_1d, LAYER_NAMES["boundary_condition_1d"]),
            (filtered_bc_2d, LAYER_NAMES["boundary_condition_2d"]),
            (filtered_lateral_1d, LAYER_NAMES["lateral_1d"]),
            (filtered_lateral_2d, LAYER_NAMES["lateral_2d"]),
            (filtered_surface_map, LAYER_NAMES["surface_map"]),
            (filtered_surface, LAYER_NAMES["surface"]),
            (filtered_obstacle, LAYER_NAMES["obstacle"]),
            (filtered_potential_breach, LAYER_NAMES["potential_breach"]),
            (filtered_exchange_line, LAYER_NAMES["exchange_line"]),
            (filtered_grid_ref_line, LAYER_NAMES["grid_refinement_line"]),
            (filtered_grid_ref_area, LAYER_NAMES["grid_refinement_area"]),
        ]

        for gdf, layer_name in write_pairs:
            _drop_id(gdf).to_file(output_gpkg, layer=layer_name, driver="GPKG")

        # ---- Step 9: Clip rasters (if present) ----
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


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def run(
    schematisation_directory: str | Path,
    subareas_path: str | Path,
    field_name: str,
    calculation_grid_cells_path: str | Path,
    subareas_layer_name: str | None = None,
    calculation_grid_cells_layer_name: str | None = None,
    isolate_1d: bool = False,
) -> None:
    """Entry point for creating sub-models from a 3Di schematisation.

    Parameters
    ----------
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
    """
    Submodels(
        schematisation_directory=schematisation_directory,
        subareas_path=subareas_path,
        field_name=field_name,
        calculation_grid_cells_path=calculation_grid_cells_path,
        subareas_layer_name=subareas_layer_name,
        calculation_grid_cells_layer_name=calculation_grid_cells_layer_name,
        isolate_1d=isolate_1d,
    )


# test_submodels.ipynb o scratch.py (fuera del paquete)
# from hhnk_threedi_tools.breaches.submodels import run

# run(
#     schematisation_directory=r"Y:\...\schematisation",
#     subareas_path=r"Y:\...\flood_area_oost.gpkg",
#     field_name="Deelgebied",
#     calculation_grid_cells_path=r"Y:\...\computational_grid_sbhz.gpkg",
#     calculation_grid_cells_layer_name="cell",
#     isolate_1d=True,
# )
