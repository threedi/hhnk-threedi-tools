# %%
import uuid
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Optional, Tuple

import geopandas as gpd
import hhnk_research_tools as hrt
import numpy as np
import pandas as pd

import hhnk_threedi_tools as htt
from hhnk_threedi_tools.core.folders import Folders
from hhnk_threedi_tools.core.schematisation_builder.DAMO_HyDAMO_converter import DAMO_to_HyDAMO_Converter

# %%


class PompIntermediateConverter:
    """
    # TODO deprecated, remove later

    Intermediate converter for pomp data.
    From CSO format to DAMO/intermediate format, ready for converting to HyDAMO.
    Add column "distance_validation_rule" to the gemaal layer.

    Parameters
    ----------
    damo_file_path : Path
        Path to the DAMO geopackage file.
    logger : logging.Logger, optional
        Logger for logging messages. If not provided, a default logger will be used.
    """

    def __init__(self, damo_file_path: Path, damo_schema_path: Optional[Path] = None, logger=None):
        self.damo_file_path = Path(damo_file_path)
        # just to mae=ke the damo_hydamo_converter.py work for reading damo schema
        self.hydamo_file_path: Optional[Path] = damo_file_path.parent / "HyDAMO.gpkg"

        if logger:
            self.logger = logger
        else:
            self.logger = hrt.logging.get_logger(__name__)

        self.pomp: Optional[gpd.GeoDataFrame] = None
        self.gemaal: Optional[gpd.GeoDataFrame] = None
        self.combinatiepeilgebied: Optional[gpd.GeoDataFrame] = None
        self.polder_polygon: Optional[gpd.GeoDataFrame] = None
        self.peilafwijking: Optional[gpd.GeoDataFrame] = None
        self.peilgebiedpraktijk: Optional[gpd.GeoDataFrame] = None

        if damo_schema_path is None:
            self.damo_schema_path = hrt.get_pkg_resource_path(
                package_resource=htt.resources.schematisation_builder, name="DAMO_2_3.xml"
            )
        else:
            self.damo_schema_path = Path(damo_schema_path)

        self.logger.info(f"Initialized GemaalIntermediateConverter with DAMO file: {self.damo_file_path}")

    def run(self):
        """Run the converter to update the pomp layer."""
        self.logger.info("Running PompIntermediateConverter...")
        self.load_layers()  # STEP 1
        self.update_pomp_layer()  # STEP 2
        self.write_outputs()  # STEP 3
        self.logger.info("PompIntermediateConverter run completed.")

    def load_layers(self):
        """Load the necessary pomp layer from the DAMO geopackage file and DAMO schema."""
        self.logger.info("Loading pomp layer from DAMO file...")
        self.pomp = gpd.read_file(self.damo_file_path, layer="POMP")
        # self.logger(self.pomp.head())

        self.gemaal = gpd.read_file(self.damo_file_path, layer="GEMAAL")
        self.logger.info("Pomp layer loaded successfully.")

        self.combinatiepeilgebied = gpd.read_file(self.damo_file_path, layer="COMBINATIEPEILGEBIED")
        self.logger.info("Combinatiepeilgebied layer loaded successfully.")

        self.polder_polygon = gpd.read_file(self.damo_file_path.parent / "polder_polygon.shp")
        self.logger.info("Polder polygon layer loaded successfully.")

    def _add_column_gemaalid(self):
        """Add gemaalid column to the pomp layer."""
        self.logger.info("Link pomp to gemaal and pick global_id as to define gemaalid in pomp layer")
        # Check if gemaal and pomp layer are loaded
        if self.gemaal is None:
            self.logger.error(f"Layer '{self.gemaal}' not loaded. Call {self.load_layers}() first.")
            return

        # Merge pomp with gemaal based on CODEBEHEEROBJECT (pomp) and code (gemaal)
        merged = self.pomp.merge(
            self.gemaal[["code", "globalid"]], left_on="codebeheerobject", right_on="code", how="left"
        )

        # Assign the globalid from gemaal as gemaalid in pomp
        self.pomp["gemaalid"] = merged["globalid"]

        self.logger.info("gemaalid column added successfully.")

    def _add_column_globalid(self):
        """Add globalid column to the pomp layer."""
        self.logger.info("Adding 'globalid' column to the pomp layer")
        if "globalid" not in self.pomp.columns:
            # Generate a unique globalid for each pomp
            self.pomp["globalid"] = [str(uuid.uuid4()) for _ in range(len(self.pomp))]
            self.logger.info("'globalid' column added successfully.")
        elif self.pomp["globalid"].isnull().any():
            # If globalid column exists but has null values, fill them with unique globalids
            self.logger.info("Filling null values in 'globalid' column with unique globalids")
            self.pomp["globalid"] = self.pomp["globalid"].apply(lambda x: str(uuid.uuid4()) if pd.isnull(x) else x)

    # TODO: use gemaal maximalecapaciteit for pomp maximalecapaciteit if aantalpompen = 1

    def _adjust_pomp_maximalecapaciteit(self):
        """
        If the gemaal has a maximalecapaciteit and the pomp has a maximalecapaciteit, use the gemaal's value.
        This function is only used if a gemaaal is:
        - linked to a pomp (i.e. gemaalid is not null)
        - the gemaal has a maximalecapaciteit value.
        - the gemaal has only one pomp linked to it (i.e. aantalpompen = 1).
        """

        self.logger.info("Start adjusting 'maximalecapaciteit' in the pomp layer based on gemaal data...")

        # list gemaalid's which have one pomp linked to it
        gemaalid_counts = self.pomp["gemaalid"].value_counts()
        gemaalid_one_pomp = gemaalid_counts[gemaalid_counts == 1].index

        for gemaalid in gemaalid_one_pomp:
            # Get the gemaal with the gemaalid
            gemaal = self.gemaal[self.gemaal["globalid"] == gemaalid]

            if not gemaal.empty and "maximalecapaciteit" in gemaal.columns:
                # Check if the gemaal has a maximalecapaciteit value
                if not pd.isna(gemaal["maximalecapaciteit"].to_numpy()[0]):
                    # Update the pomp's maximalecapaciteit with the gemaal's value
                    self.pomp.loc[self.pomp["gemaalid"] == gemaalid, "maximalecapaciteit"] = gemaal[
                        "maximalecapaciteit"
                    ].to_numpy()[0]

                    self.logger.info(
                        f"Updated 'maximalecapaciteit' for gemaalid {gemaalid} in pomp layer with gemaal's value."
                    )
                else:
                    self.logger.warning(
                        f"Gemaal with gemaalid {gemaalid} has no 'maximalecapaciteit' value. Skipping update."
                    )
            else:
                self.logger.warning(
                    f"Gemaal with gemaalid {gemaalid} not found or has no 'maximalecapaciteit' column."
                )
                self.logger.warning(f"Skipping update for gemaalid {gemaalid} in pomp layer.")

        self.logger.info(
            f"Finished adjusting 'maximalecapaciteit' in the pomp layer for {len(gemaalid_one_pomp)} pompen."
        )

    def _make_pomp_layer(self):
        """
        Create the pomp layer with necessary columns according to the DAMO schema.
        Fill in data for at least one pomp with coupled gemaal data.
        This is needed to make the validator work properly.

        If the layer already exists and is not empty, this function is not executed.
        """
        self.logger.info("Start creating pomp layer with necessary columns according to DAMO schema...")

        # read the DAMO_2.3.xml schema in resource folder with function in damo_to_hydamo_converter.py
        converter = DAMO_to_HyDAMO_Converter(
            damo_file_path=self.damo_file_path,
            hydamo_file_path=self.hydamo_file_path,
            overwrite=True,
            damo_schema_path=self.damo_schema_path,
        )

        DAMO_schema_domains, DAMO_schema_objects = converter.retrieve_domain_mapping()
        pomp_columns = list(DAMO_schema_objects["pomp"].keys())

        # create a new GeoDataFrame with the pomp columns and geometry column
        self.pomp = gpd.GeoDataFrame(columns=pomp_columns)

        # add at least one gemaal to the pomp layer
        if self.gemaal is not None and not self.gemaal.empty:
            self.pomp["objectid"] = [1]  # Add a dummy objectid
            self.pomp["gemaalid"] = [self.gemaal["globalid"].iloc[0]]  # Use the first gemaal's globalid
            self.pomp["codebeheerobject"] = [self.gemaal["code"].iloc[0]]  # Use the first gemaal's code
            self.pomp["geometry"] = [self.gemaal["geometry"].iloc[0]]
            self.pomp["maximalecapaciteit"] = [self.gemaal["maximalecapaciteit"].iloc[0]]
            self.logger.info(f"Pomp layer created with object based on gemaal {self.gemaal['globalid'].iloc[0]}.")
        else:
            self.logger.warning("No gemaal data available to add to the pomp layer. Pomp layer will be empty.")

    def update_pomp_layer(self):
        """Update the pomp layer with gemaalid and save it to the DAMO geopackage."""
        self.logger.info("Updating pomp layer with gemaalid...")

        # Check if the gemaal layer is loaded
        if self.gemaal is None:
            self.logger.error(f"Layer '{self.gemaal}' not loaded. Call {self.load_layers}() first.")
            return

        # check if there is a pomp layer which is not empty
        if self.pomp is None or self.pomp.empty:
            # if so, create a "dummy" pomp layer based on one gemaal
            self.logger.info("Pomp layer is empty or not loaded. Creating a dummy pomp layer...")
            self._make_pomp_layer()
        else:
            self.logger.info("Pomp layer already exists. Proceeding to update with gemaalid.")
            # Add gemaalid column to the pomp layer
            self._add_column_gemaalid()

        # Add globalid column to the pomp layer if globalid does not exist
        self._add_column_globalid()

        self.logger.info("Pomp layer updated successfully.")


# %%
if __name__ == "__main__":
    project_folder = Path(r"E:\09.modellen_speeltuin\test_with_pomp_table_juan")
    folder = Folders(project_folder)
    damo = folder.source_data.path / "DAMO.gpkg"
    intermediate_convertion = PompIntermediateConverter(damo)
    pump_function = intermediate_convertion.gemaal_streefpeil_value()
