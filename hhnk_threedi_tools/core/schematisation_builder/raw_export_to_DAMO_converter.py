# %%
import logging
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely
from shapely.geometry import LineString, MultiLineString, Point
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

    def find_linemerge_id_by_hydroobject_code(self, code: str) -> str:
        """
        Utility/debug function and not used in the main class workflow.

        Find the linemergeid by hydroobject code.
        """
        if code in self.hydroobject_map:
            return self.hydroobject_map[code]
        else:
            self.logger.warning(f"No linemerge found for hydroobject with code {code}.")
            return None

    def find_peilgebied_id_by_hydroobject_code(self, code: str):
        """
        Utility/debug function and not used in the main class workflow.

        Find the peilgebiedid by hydroobject code.
        """

        self.data._ensure_loaded(layers=["hydroobject_linemerged"], previous_method="process_linemerge")

        # First, find the linemergeid for this hydroobject code
        if code in self.hydroobject_map:
            linemerge_id = self.hydroobject_map[code]

            # Now, lookup the peilgebiedid in the linemerged GeoDataFrame
            match = self.data.hydroobject_linemerged[self.data.hydroobject_linemerged["linemergeid"] == linemerge_id]

            if match.empty:
                self.logger.warning(f"No matching linemerge found for hydroobject code '{code}'.")
                return None
            else:
                peilgebied_id = match.iloc[0]["peilgebiedid"]
                return peilgebied_id
        else:
            self.logger.warning(f"Hydroobject code '{code}' not found in the linemerged map.")
            return None

    def find_deepest_point_by_hydroobject_code(self, code: str):
        """
        Utility/debug function and not used in the main class workflow.

        Find the deepest point for a hydroobject by its code.
        """
        self.data._ensure_loaded(layers=["hydroobject"], previous_method="load_layers")
        self.data._ensure_loaded(layers=["profielgroep"], previous_method="create_profile_tables")

        if "diepstepunt" not in self.data.hydroobject.columns:
            raise ValueError(
                "diepstepunt column is not present in hydroobject. Call compute_deepest_point_hydroobjects() first."
            )

        # Find the hydroobject by code
        hydroobject = self.data.hydroobject[self.data.hydroobject["code"] == code]
        if hydroobject.empty:
            self.logger.warning(f"No hydroobject found with code '{code}'.")
            return None

        # Get the deepest point
        diepste_punt = hydroobject["diepstepunt"].values[0]
        return diepste_punt

    def compute_deepest_point_hydroobjects(self) -> None:
        """Compute the deepest point for each hydroobject that has a profile."""
        self.logger.info("Computing the deepest point for hydroobjects...")

        self.data._ensure_loaded(layers=["hydroobject"], previous_method="load_layers")
        self.data._ensure_loaded(layers=["profielgroep"], previous_method="create_profile_tables")

        # filter the hydroobject on reference in profielgroep
        # these are the hydroobjects that have a profile
        hydroobject_with_profiles = self.data.hydroobject[
            self.data.hydroobject["globalid"].isin(self.data.profielgroep["hydroobjectid"])
        ]
        if hydroobject_with_profiles.empty:
            self.logger.warning("No hydroobjects with profiles found. Skipping deepest point computation.")
            return

        # Check if "diepstepunt" is already computed
        if "diepstepunt" not in self.data.profiellijn.columns:
            self._compute_deepest_point_profiellijn()

        # collect the profiellijn features per hydroobject
        hydroobject_deepest_points = []
        for _, hydro_row in hydroobject_with_profiles.iterrows():
            hydro_code = hydro_row["code"]

            # Find the corresponding profiellijn
            profiellijn = self._find_profiellijn_by_hydroobject_code(hydro_code)
            if profiellijn is None or profiellijn.empty:
                continue

            # Find the deepest point in the profiellijn
            diepste_punt = profiellijn["diepstepunt"].min()
            if pd.isna(diepste_punt):
                continue

            # Append to results
            hydroobject_deepest_points.append(
                {"globalid": hydro_row["globalid"], "diepstepunt": round(diepste_punt, 2)}
            )

        # Create a DataFrame from the results
        hydroobject_deepest_points_df = pd.DataFrame(hydroobject_deepest_points)
        if hydroobject_deepest_points_df.empty:
            self.logger.warning("No deepest points computed for hydroobjects.")
            return

        # Merge the deepest points back to the hydroobject GeoDataFrame
        self.data.hydroobject = self.data.hydroobject.merge(hydroobject_deepest_points_df, on="globalid", how="left")

        # log the number of hydroobjects with computed deepest points
        num_deepest_points = self.data.hydroobject["diepstepunt"].notna().sum()
        self.logger.info(f"Computed deepest points for {num_deepest_points} hydroobjects.")

    def _compute_deepest_point_profiellijn(self) -> None:
        """Compute the deepest point per profiellijn and add as a new column to self.data.profiellijn.

        Called by: self.compute_deepest_point_hydroobjects
        """
        self.logger.info("Computing the deepest point per profiellijn...")
        self.data._ensure_loaded(layers=["profielpunt", "profiellijn"], previous_method="create_profile_tables")

        # Convert height to float
        self.data.profielpunt["hoogte"] = pd.to_numeric(self.data.profielpunt["hoogte"], errors="coerce")

        # Find the deepest point per profiellijn
        deepest_points = self.data.profielpunt.groupby("profiellijnid")["hoogte"].min()

        # Map the deepest point to the profiellijn using its globalid
        self.data.profiellijn["diepstepunt"] = self.data.profiellijn["globalid"].map(deepest_points)

    def _find_profiellijn_by_hydroobject_code(self, code: str) -> Optional[gpd.GeoDataFrame]:
        """
        Find the profiellijn GeoDataFrame by hydroobject code.

        Called by: compute_deepest_point_hydroobjects
        """
        self.data._ensure_loaded(layers=["profiellijn"], previous_method="create_profile_tables")

        # Find the hydroobjectid for the given code
        hydroobject_id = self.data.hydroobject[self.data.hydroobject["code"] == code]["globalid"].values
        if not hydroobject_id:
            self.logger.warning(f"No hydroobject found with code '{code}'.")
            return None

        # Filter profielgroep by hydroobjectid
        profielgroep = self.data.profielgroep[self.data.profielgroep["hydroobjectid"] == hydroobject_id[0]]
        if profielgroep.empty:
            self.logger.warning(f"No profielgroep found for hydroobject code '{code}'.")
            return None

        # Get the profielgroep_ids, use 'copyof' if present, otherwise use 'globalid'
        profielgroep_ids = profielgroep.apply(
            lambda row: row["copyof"] if "copyof" in row and pd.notna(row["copyof"]) else row["globalid"], axis=1
        ).tolist()

        # Get the profiellijn that matches the profielgroep_ids
        profiellijn = self.data.profiellijn[self.data.profiellijn["profielgroepid"].isin(profielgroep_ids)]

        if profiellijn.empty:
            self.logger.warning(f"No profiellijn found for hydroobject code '{code}'.")
            return None

        return profiellijn
