import uuid

import geopandas as gpd
import pandas as pd

from hhnk_threedi_tools.core.schematisation_builder.raw_export_to_DAMO_converter import RawExportToDAMOConverter

POMP_COLUMNS = [
    "code",
    "created_date",
    "featuretype",
    "gemaalid",
    "globalid",
    "last_edited_date",
    "maximalecapaciteit",
    "minimalecapaciteit",
    "objectid",
    "opmerking",
    "opstellingpomp",
    "pompcurve",
    "pomprichting",
    "rioolgemaalid",
    "soortaandrijving",
    "typepomp",
    "typepompschakeling",
]  # Columns according to DAMO schema 2.4.1


class GemaalConverter(RawExportToDAMOConverter):
    """Gemaal-specific converter implementation."""
    def __init__(self, raw_export_converter: RawExportToDAMOConverter):
        self.data = raw_export_converter.data
        self.logger = raw_export_converter.logger

    def run(self):
        """Run the converter to update the gemaal layer."""
        if self.has_executed():
            self.logger.debug("Skipping GemaalConverter, already executed.")
            return

        self.logger.info("Running GemaalConverter...")
        self.update_gemaal_layer()
        self.mark_executed()

    def update_gemaal_layer(self):
        self.logger.info("Updating gemaal layer...")

        # Ensure the pomp layer is loaded or create it if missing/empty
        try:
            self.data._ensure_loaded(["pomp"], previous_method="load_layers")
            pomp_empty = self.data.pomp is None or self.data.pomp.empty
        except ValueError:
            pomp_empty = True

        if pomp_empty:
            self.logger.info("Pomp layer is empty or not loaded. Creating a dummy pomp layer...")
            self._make_pomp_layer()
        else:
            self.logger.info("Pomp layer already exists. Updating with gemaalid...")
            self._add_column_gemaalid()

        # Add globalid column to the pomp layer if needed
        self._add_column_globalid()

        # Adjust maximalecapaciteit for pompen linked to a single gemaal
        self._adjust_pomp_maximalecapaciteit()

    def _add_column_gemaalid(self):
        """Add gemaalid column to the pomp layer."""
        # Merge pomp with gemaal based on CODEBEHEEROBJECT (pomp) and code (gemaal)
        merged = self.data.pomp.merge(
            self.data.gemaal[["code", "globalid"]], left_on="codebeheerobject", right_on="code", how="left"
        )

        # Assign the globalid from gemaal as gemaalid in pomp
        self.data.pomp["gemaalid"] = merged["globalid"]

    def _add_column_globalid(self):
        """Add globalid column to the pomp layer."""
        if "globalid" not in self.data.pomp.columns:
            # Generate a unique globalid for each pomp
            self.data.pomp["globalid"] = [str(uuid.uuid4()) for _ in range(len(self.data.pomp))]
        elif self.data.pomp["globalid"].isnull().any():
            # If globalid column exists but has null values, fill them with unique globalids
            self.logger.info("Filling null values in 'globalid' column with unique globalids")
            self.data.pomp["globalid"] = self.data.pomp["globalid"].apply(
                lambda x: str(uuid.uuid4()) if pd.isnull(x) else x
            )

    # TODO: use gemaal maximalecapaciteit for pomp maximalecapaciteit if aantalpompen = 1

    def _adjust_pomp_maximalecapaciteit(self):
        """
        If the gemaal has a maximalecapaciteit and the pomp has a maximalecapaciteit, use the gemaal's value.
        This function is only used if a gemaaal is:
        - linked to a pomp (i.e. gemaalid is not null)
        - the gemaal has a maximalecapaciteit value.
        - the gemaal has only one pomp linked to it (i.e. aantalpompen = 1).
        """

        # list gemaalid's which have one pomp linked to it
        gemaalid_counts = self.data.pomp["gemaalid"].value_counts()
        gemaalid_one_pomp = gemaalid_counts[gemaalid_counts == 1].index

        for gemaalid in gemaalid_one_pomp:
            # Get the gemaal with the gemaalid
            gemaal = self.data.gemaal[self.data.gemaal["globalid"] == gemaalid]

            if not gemaal.empty and "maximalecapaciteit" in gemaal.columns:
                # Check if the gemaal has a maximalecapaciteit value
                if not pd.isna(gemaal["maximalecapaciteit"].to_numpy()[0]):
                    # Update the pomp's maximalecapaciteit with the gemaal's value
                    self.data.pomp.loc[self.data.pomp["gemaalid"] == gemaalid, "maximalecapaciteit"] = gemaal[
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

    def _make_pomp_layer(self):
        """
        Create the pomp layer with necessary columns according to the DAMO schema.
        Fill in data for at least one pomp with coupled gemaal data.
        This is needed to make the validator work properly.

        If the layer already exists and is not empty, this function is not executed.
        """

        # read the DAMO_2.3.xml schema in resource folder with function in damo_to_hydamo_converter.py
        # converter = DAMO_to_HyDAMO_Converter()

        # TODO wietse retrieving the pump columns from schema will only work if we have the damo schema locally
        # TODO converter without arguments will also only work if we accept None arguments for the init function
        # TODO for now, we use the hardcoded POMP_COLUMNS list at the top of this file
        # _, DAMO_schema_objects = converter.retrieve_domain_mapping()
        # pomp_columns = list(DAMO_schema_objects["pomp"].keys())

        # create a new GeoDataFrame with the pomp columns and geometry column
        self.data.pomp = gpd.GeoDataFrame(columns=POMP_COLUMNS)

        # add at least one gemaal to the pomp layer
        if self.data.gemaal is not None and not self.data.gemaal.empty:
            self.data.pomp["objectid"] = [1]  # Add a dummy objectid
            self.data.pomp["code"] = [self.data.gemaal["code"].iloc[0]]  # Create a unique code for the pomp
            self.data.pomp["gemaalid"] = [self.data.gemaal["globalid"].iloc[0]]  # Use the first gemaal's globalid
            self.data.pomp["codebeheerobject"] = [self.data.gemaal["code"].iloc[0]]  # Use the first gemaal's code
            self.data.pomp["maximalecapaciteit"] = [self.data.gemaal["maximalecapaciteit"].iloc[0]]

            self.data.pomp = self.data.pomp.set_geometry([self.data.gemaal["geometry"].iloc[0]])
            self.data.pomp.crs = self.data.gemaal.crs

            self.logger.info(f"Pomp layer created with object based on gemaal {self.data.gemaal['globalid'].iloc[0]}.")
        else:
            self.logger.warning("No gemaal data available to add to the pomp layer. Pomp layer will be empty.")
