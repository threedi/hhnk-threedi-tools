# %%
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import geopandas as gpd
import shapely
from shapely.validation import make_valid

CRS = "EPSG:28992"


@dataclass
class _Data:
    """Available as .data under RawExportToDAMOConverter"""

    # DAMO tables
    # Tables are not Optional[gpd.GeoDataFrame] because it causes issues with mypy typechecking.
    gemaal: gpd.GeoDataFrame = field(default_factory=gpd.GeoDataFrame)
    hydroobject: gpd.GeoDataFrame = field(default_factory=gpd.GeoDataFrame)
    pomp: gpd.GeoDataFrame = field(default_factory=gpd.GeoDataFrame)

    # Specific hhnk tables
    hydroobject_linemerged: gpd.GeoDataFrame = field(default_factory=gpd.GeoDataFrame)
    gw_pro: gpd.GeoDataFrame = field(default_factory=gpd.GeoDataFrame)
    gw_prw: gpd.GeoDataFrame = field(default_factory=gpd.GeoDataFrame)
    gw_pbp: gpd.GeoDataFrame = field(default_factory=gpd.GeoDataFrame)
    iws_geo_beschr_profielpunten: gpd.GeoDataFrame = field(default_factory=gpd.GeoDataFrame)

    # CSO tables
    peilgebiedpraktijk: gpd.GeoDataFrame = field(default_factory=gpd.GeoDataFrame)
    polder: gpd.GeoDataFrame = field(default_factory=gpd.GeoDataFrame)

    # Output tables
    profielgroep: gpd.GeoDataFrame = field(default_factory=gpd.GeoDataFrame)
    profiellijn: gpd.GeoDataFrame = field(default_factory=gpd.GeoDataFrame)
    profielpunt: gpd.GeoDataFrame = field(default_factory=gpd.GeoDataFrame)

    def _ensure_loaded(self, layers: list[str], previous_method: str) -> None:
        """
        Ensure a table is properly loaded. Will raise ValueError when Empty.

        Parameters
        ----------
        layers : list
            These layers are checked. raises ValueError if the table is empty without columns
        previous_method : str
            The previous function that should be run to load the given layer.
        """
        for layer in layers:
            gdf = getattr(self, layer)
            if gdf.empty and len(gdf.columns) == 0:
                raise ValueError(f"Layer '{layer}' not loaded. Call {previous_method}() first.")


class RawExportToDAMOConverter:
    """
    Base class for intermediate converters, converting raw export data to DAMO format.
    Handles reading, validating, and writing layers, and provides general utility functions.
    """

    _executed = set()  # keeps track of executed converters (child classes)

    def __init__(
        self,
        raw_export_file_path: Path,
        output_file_path: Path,
        logger: Optional[logging.Logger] = None,
    ):
        self.raw_export_file_path = Path(raw_export_file_path)
        self.output_file_path = Path(output_file_path)
        self.logger = logger or logging.getLogger(__name__)
        self.data = _Data()

    def mark_executed(self):
        RawExportToDAMOConverter._executed.add(self.__class__)
        self.logger.debug(f"{self.__class__.__name__} marked as executed.")

    def has_executed(cls) -> bool:
        return cls in RawExportToDAMOConverter._executed

    def write_outputs(self):
        output_gdf_dict = {name: val for name, val in self.data.__dict__.items() if isinstance(val, gpd.GeoDataFrame)}

        if not output_gdf_dict:
            self.logger.warning("No GeoDataFrames found to write.")
            return

        if self.output_file_path.suffix != ".gpkg":
            self.output_file_path.mkdir(parents=True, exist_ok=True)
            self.output_file_path = self.output_file_path / f"{self.output_file_path.name}.gpkg"

        for name, gdf in output_gdf_dict.items():
            if isinstance(gdf, gpd.GeoDataFrame) and "geometry" in gdf.columns:
                gdf.to_file(self.output_file_path, layer=name, engine="pyogrio")
            else:  # dataframe handling
                gdf_no_geom = gpd.GeoDataFrame(gdf, geometry=[None] * len(gdf), crs=CRS)
                gdf_no_geom.to_file(self.output_file_path, layer=name, engine="pyogrio", driver="GPKG")
                if gdf_no_geom.empty:
                    self.logger.warning(f"Layer {name} is empty.")
                else:
                    self.logger.info(f"Layer {name} has no geometry.")

            self.logger.info(f"Wrote layer '{name}' to {self.output_file_path}")

    # ---------- Generic Helpers ----------

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
