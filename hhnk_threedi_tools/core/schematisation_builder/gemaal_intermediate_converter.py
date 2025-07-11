# %%
import uuid
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Optional, Tuple

import geopandas as gpd
import hhnk_research_tools as hrt
import numpy as np
import pandas as pd
import pyogrio

import hhnk_threedi_tools as htt
from hhnk_threedi_tools.core.folders import Folders

# %%


def __init__(self, folder: Folders, revision=0, dem_path=None):
    self.folder = folder
    self.revision = revision


class PompIntermediateConverter:
    """
    Intermediate converter for pomp data.
    From CSO format to DAMO/intermediate format, ready for converting to HyDAMO.
    Add column "distance_validation_rule" to the gemaal layer.
    Parameters
    ----------
    damo_file_path : Path
        Path to the DAMO geopackage file.
    poler_polygon :  Path
        Path to the poler_polygon
    logger : logging.Logger, optional
        Logger for logging messages. If not provided, a default logger will be used.
    """

    def __init__(self, damo_file_path: Path, polder_polygon_path: Path, logger=None):
        self.damo_file_path = Path(damo_file_path)
        self.polder_polygon_path = Path(polder_polygon_path)

        if logger:
            self.logger = logger
        else:
            self.logger = hrt.logging.get_logger(__name__)

        self.pomp: Optional[gpd.GeoDataFrame] = None
        self.gemaal: Optional[gpd.GeoDataFrame] = None
        self.combinatiepeilgebied: Optional[gpd.GeoDataFrame] = None
        self.polder_polygon: Optional[gpd.GeoDataFrame] = None

        self.logger.info(f"Initialized GemaalIntermediateConverter with DAMO file: {self.damo_file_path}")

    def load_layers(self):
        """Load the necessary pomp layer from the DAMO geopackage file and DAMO schema."""
        self.logger.info("Loading pomp layer from DAMO file...")
        self.pomp = gpd.read_file(self.damo_file_path, layer="POMP")
        # self.logger(self.pomp.head())

        self.gemaal = gpd.read_file(self.damo_file_path, layer="GEMAAL")
        self.logger.info("Pomp layer loaded successfully.")

        self.combinatiepeilgebied = gpd.read_file(self.damo_file_path, layer="COMBINATIEPEILGEBIED")
        self.logger.info("Combinatiepeilgebied layer loaded successfully.")

        self.polder_polygon = gpd.read_file(self.polder_polygon_path)
        self.logger.info("Poler polygon layer loaded successfully.")

    def add_column_gemaalid(self):
        """Add gemaalid column to the pomp layer."""
        self.logger.info("Link pomp to gemaal and pick global_id as to define gemaalid in pomp layer")
        # Check if gemaal and pomp layer are loaded
        if self.gemaal is None:
            self.logger.error("Gemaal layer is not loaded. Please load the gemaal layer first.")
            return
        if self.pomp is None:
            self.logger.error("Pomp layer is not loaded. Please load the pomp layer first.")
            return

        # Merge pomp with gemaal based on CODEBEHEEROBJECT (pomp) and code (gemaal)
        merged = self.pomp.merge(
            self.gemaal[["code", "globalid"]], left_on="codebeheerobject", right_on="code", how="left"
        )
        # Assign the globalid from gemaal as gemaalid in pomp
        self.pomp["gemaalid"] = merged["globalid"]
        # TODO: make a check if globalid is in correct format.

        self.logger.info("gemaalid column added successfully.")

    def add_columns_to_pomp(self):
        """Add necessary columns to the pomp layer."""
        self.logger.info("Adding necessary columns to the pomp layer...")
        # Load the pomp layer if not already loaded
        if self.pomp is None or self.gemaal is None:
            self.load_layers()

        # Add 'gemaalid' column if it does not exist
        if "gemaalid" not in self.pomp.columns:
            self.add_column_gemaalid()

        # TODO: discuss if this is nessessary to add
        # Add 'globalid' column if it does not exist
        # if "globalid" not in self.pomp.columns:
        #     self.logger.info("Adding 'globalid' column to the pomp layer")
        #     # Generate a unique globalid for each pomp
        #     self.pomp["globalid"] = [str(uuid.uuid4()) for _ in range(len(self.pomp))]
        #     self.logger.info("'globalid' column added successfully.")

        # TODO: use gemaal maximalecapaciteit for pomp maximalecapaciteit if aantalpompen = 1

    def update_pomp_layer(self):
        """Update the pomp layer with gemaalid and save it to the DAMO geopackage."""
        self.logger.info("Updating pomp layer with gemaalid...")
        # Load the pomp layer if not already loaded
        self.load_layers()
        # Add gemaalid column to the pomp layer
        self.add_columns_to_pomp()
        # Save the updated pomp layer to the DAMO geopackage
        pyogrio.write_dataframe(self.pomp, self.damo_file_path, layer="pomp", driver="GPKG", overwrite=True)
        self.logger.info("Pomp layer updated successfully.")

    def polygon_to_polyline(self):
        # transform MULTIPOLYGON  to LINESTRING
        self.polder_polygon
        gdf_peilgebiedpraktijk_linestring = self.combinatiepeilgebied.copy()
        gdf_peilgebiedpraktijk_linestring["geometry"] = gdf_peilgebiedpraktijk_linestring["geometry"].apply(
            lambda geom: geom.boundary if geom.type == "MultiPolygon" else geom
        )
        gdf_peilgebiedpraktijk_linestring = gdf_peilgebiedpraktijk_linestring.explode(index_parts=True)
        gdf_peilgebiedpraktijk_linestring = gdf_peilgebiedpraktijk_linestring.reset_index(drop=True)

        # clip linestrings to the polder_polygon
        gdf_peilgebiedpraktijk_linestring = gpd.clip(gdf_peilgebiedpraktijk_linestring, self.polder_polygon)

        # make spatial join between gdf_gemaal and gdf_peilgebiedpraktijk_linestring distance 10 cm
        gemaal_spatial_join = gpd.sjoin_nearest(
            self.gemaal,
            gdf_peilgebiedpraktijk_linestring,
            how="inner",
            max_distance=0.01,
            distance_col="distance_validation_rule",
        )

        # Join the column 'distance_validation_rule' from gemaal_spatial_join into gdf_gemaal based on the 'code' column
        self.gdf_gemaal = self.gemaal.merge(
            gemaal_spatial_join[["code", "distance_validation_rule"]], on="code", how="left"
        )

        # save the layer in DAMO TOFIX """"SHOULD I SAVE IT IN DAMO OR HYDAMO"""
        self.gdf_gemaal.to_file(self.damo_file_path, layer="GEMAAL", driver="GPKG")


# %%
