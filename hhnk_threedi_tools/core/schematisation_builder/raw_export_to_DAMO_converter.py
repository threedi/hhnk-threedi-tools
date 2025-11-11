import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import geopandas as gpd
import hhnk_research_tools as hrt
from shapely.validation import make_valid

CRS = "EPSG:28992"


@dataclass
class _Data:
    # Output tables (explicitly defined, because not in raw export)
    profielgroep: gpd.GeoDataFrame = field(default_factory=gpd.GeoDataFrame)
    profiellijn: gpd.GeoDataFrame = field(default_factory=gpd.GeoDataFrame)
    profielpunt: gpd.GeoDataFrame = field(default_factory=gpd.GeoDataFrame)

    # Dynamically loaded raw export layers will be added as attributes

    def _ensure_loaded(self, layers: list[str], previous_method: str) -> None:
        for layer in layers:
            gdf = getattr(self, layer, None)
            if gdf is None or (gdf.empty and len(gdf.columns) == 0):
                raise ValueError(f"Layer '{layer}' not loaded. Call {previous_method}() first.")


class RawExportToDAMOConverter:
    _executed = set()

    def __init__(
        self,
        raw_export_file_path: Path,
        output_file_path: Path,
        logger: Optional[logging.Logger] = None,
    ):
        self.raw_export_file_path = Path(raw_export_file_path)
        self.hrt_raw_export_file_path = hrt.SpatialDatabase(raw_export_file_path)
        self.output_file_path = Path(output_file_path)
        self.logger = logger or logging.getLogger(__name__)
        self.data = _Data()

        self.load_layers()

    def load_layers(self):
        self.logger.info("Loading all raw export layers...")
        for layer_name in self.hrt_raw_export_file_path.available_layers():
            gdf = self._load_and_validate(self.raw_export_file_path, layer_name)

            if layer_name == "peilgebiedpraktijk":
                self.data.peilgebiedpraktijk = gdf.explode(index_parts=False).reset_index(drop=True)

            setattr(self.data, layer_name.lower(), gdf)
        self.logger.info("All raw export layers loaded.")

    def mark_executed(self):
        RawExportToDAMOConverter._executed.add(self.__class__)
        self.logger.debug(f"{self.__class__.__name__} marked as executed.")

    @classmethod
    def has_executed(cls) -> bool:
        return cls in RawExportToDAMOConverter._executed

    def write_outputs(self):
        # Collect all GeoDataFrames in self.data (raw + output tables)
        output_gdf_dict = {name: val for name, val in self.data.__dict__.items() if isinstance(val, gpd.GeoDataFrame)}

        if not output_gdf_dict:
            self.logger.warning("No GeoDataFrames found to write.")
            return

        if self.output_file_path.suffix != ".gpkg":
            self.output_file_path.mkdir(parents=True, exist_ok=True)
            self.output_file_path = self.output_file_path / f"{self.output_file_path.name}.gpkg"

        for name, gdf in output_gdf_dict.items():
            if isinstance(gdf, gpd.GeoDataFrame) and "geometry" in gdf.columns:
                gdf.to_file(self.output_file_path, layer=name, engine="pyogrio", overwrite=True, driver="GPKG")
            else:  # dataframe handling
                gdf_no_geom = gpd.GeoDataFrame(gdf, geometry=[None] * len(gdf), crs=CRS)
                gdf_no_geom.to_file(self.output_file_path, layer=name, engine="pyogrio", overwrite=True, driver="GPKG")
                if gdf_no_geom.empty:
                    self.logger.warning(f"Layer {name} is empty.")
                else:
                    self.logger.info(f"Layer {name} has no geometry.")
            self.logger.info(f"Wrote layer '{name}' to {self.output_file_path}")

    @staticmethod
    def _load_and_validate(source_path: Path, layer_name: str) -> gpd.GeoDataFrame:
        gdf = gpd.read_file(source_path, layer=layer_name)
        if "geometry" in gdf.columns:
            gdf["geometry"] = gdf["geometry"].apply(make_valid)
        return gdf

    @staticmethod
    def _geometrycollection_to_linestring(geometry: "shapely.Geometry") -> "shapely.Geometry":
        if geometry.geom_type == "GeometryCollection":
            fixed_geometry = [geom for geom in geometry.geoms if geom.geom_type in ["LineString", "MultiLineString"]]
            if fixed_geometry:
                merged_geometry = gpd.GeoSeries(fixed_geometry).union_all()
                return merged_geometry
            else:
                return geometry
        return geometry
