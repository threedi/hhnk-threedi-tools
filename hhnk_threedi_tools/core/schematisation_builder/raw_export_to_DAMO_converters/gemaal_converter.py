import uuid
import warnings

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

# TODO: Future improvement - retrieve pomp columns dynamically from DAMO schema instead of using hardcoded POMP_COLUMNS list

REQUIRED_GEMAAL_COLUMNS = ["code", "globalid", "geometry"]
OPTIONAL_GEMAAL_COLUMNS = ["maximalecapaciteit"]


class GemaalConverter(RawExportToDAMOConverter):
    """
    Convert raw export gemaal data to DAMO schema (2.4.1) format.

    Manages the parent-child relationship between gemaal (pumping station) and pomp (pump) layers:
    - Creates one pomp per gemaal with proper gemaalid foreign key relationship
    - Generates unique globalid identifiers for all records
    - Transfers capacity data for single-pump gemalen
    - Ensures referential integrity between layers

    Handles edge cases including missing/empty pomp layers and invalid data gracefully.
    """

    def __init__(self, raw_export_converter: RawExportToDAMOConverter):
        self.data = raw_export_converter.data
        self.logger = raw_export_converter.logger

    def run(self):
        """Execute the converter to create/update gemaal and pomp layers."""
        if self.has_executed():
            self.logger.debug("Skipping GemaalConverter, already executed.")
            return

        self.logger.info("Running GemaalConverter...")
        self.update_gemaal_layer()
        self.mark_executed()

    def update_gemaal_layer(self):
        """Update gemaal and pomp layers with proper relationships and IDs."""
        self.logger.info("Updating gemaal layer...")

        if not self._has_valid_gemaal_layer():
            self.logger.warning("No valid gemaal layer found. Creating empty pomp layer.")
            self._create_empty_pomp_layer()
            return

        self._validate_gemaal_columns()
        self._ensure_globalids(self.data.gemaal, "gemaal")

        if self._has_existing_pomp_layer():
            self.logger.info("Updating existing pomp layer...")
            self._update_existing_pomp_layer()
        else:
            self.logger.info("Creating pomp layer from gemaal data...")
            self._create_pomp_layer_from_gemaal()

        self._ensure_globalids(self.data.pomp, "pomp")
        self._transfer_capacity_for_single_pump_gemalen()

    def _is_valid_value(self, value):
        """Check if value is not None, NaN, or empty string."""
        return value is not None and not pd.isna(value) and (not isinstance(value, str) or value.strip())

    def _has_valid_gemaal_layer(self):
        """Check if gemaal layer exists and contains data."""
        try:
            self.data._ensure_loaded(["gemaal"], previous_method="load_layers")
            return self.data.gemaal is not None and not self.data.gemaal.empty
        except (ValueError, AttributeError) as e:
            self.logger.debug(f"Gemaal layer check failed: {e}")
            return False

    def _has_existing_pomp_layer(self):
        """Check if pomp layer already exists with data."""
        try:
            self.data._ensure_loaded(["pomp"], previous_method="load_layers")
            return self.data.pomp is not None and not self.data.pomp.empty
        except (ValueError, AttributeError):
            return False

    def _validate_gemaal_columns(self):
        """Validate required columns exist, warn for missing optional columns."""
        missing_required = [col for col in REQUIRED_GEMAAL_COLUMNS if col not in self.data.gemaal.columns]
        if missing_required:
            raise ValueError(f"Gemaal layer missing required columns: {missing_required}")

        missing_optional = [col for col in OPTIONAL_GEMAAL_COLUMNS if col not in self.data.gemaal.columns]
        if missing_optional:
            warnings.warn(
                f"Gemaal layer missing optional columns: {missing_optional}. Using defaults.",
                UserWarning,
            )

    def _ensure_globalids(self, gdf, layer_name):
        """Ensure all records in GeoDataFrame have valid globalid."""
        if "globalid" not in gdf.columns:
            gdf["globalid"] = None

        missing_mask = gdf["globalid"].isna() | (gdf["globalid"] == "")
        num_missing = missing_mask.sum()

        if num_missing > 0:
            self.logger.info(f"Generating {num_missing} globalid(s) for {layer_name}...")
            gdf.loc[missing_mask, "globalid"] = [str(uuid.uuid4()) for _ in range(num_missing)]

    def _create_empty_pomp_layer(self):
        """Create empty pomp layer with correct schema."""
        self.logger.info("Creating empty pomp layer...")
        crs = getattr(self.data.gemaal, "crs", None) if hasattr(self.data, "gemaal") else None
        self.data.pomp = gpd.GeoDataFrame(columns=POMP_COLUMNS, geometry=[], crs=crs)

    def _create_pomp_layer_from_gemaal(self):
        """Create pomp layer with one pump per gemaal."""
        self.logger.info(f"Creating {len(self.data.gemaal)} pomp record(s)...")

        pomp_records = [
            {
                "globalid": str(uuid.uuid4()),
                "gemaalid": row.get("globalid"),
                "code": row.get("code", f"pomp_{idx}"),
                "geometry": row.geometry.centroid if hasattr(row.geometry, "centroid") else row.geometry,
                "maximalecapaciteit": row.get("maximalecapaciteit")
                if self._is_valid_value(row.get("maximalecapaciteit"))
                else None,
            }
            for idx, row in self.data.gemaal.iterrows()
        ]

        pomp_gdf = gpd.GeoDataFrame(pomp_records, crs=self.data.gemaal.crs)

        # Add missing POMP_COLUMNS
        for col in POMP_COLUMNS:
            if col not in pomp_gdf.columns:
                pomp_gdf[col] = None

        self.data.pomp = pomp_gdf[POMP_COLUMNS + ["geometry"]]

    def _update_existing_pomp_layer(self):
        """Link existing pompen to gemalen via gemaalid."""
        if "gemaalid" not in self.data.pomp.columns:
            self.data.pomp["gemaalid"] = None

        if "codebeheerobject" not in self.data.pomp.columns or "code" not in self.data.gemaal.columns:
            self.logger.warning("Cannot link pompen: missing 'codebeheerobject' or 'code' column")
            return

        merged = self.data.pomp.merge(
            self.data.gemaal[["code", "globalid"]],
            left_on="codebeheerobject",
            right_on="code",
            how="left",
            suffixes=("", "_gemaal"),
        )
        self.data.pomp["gemaalid"] = merged["globalid_gemaal"]

        linked_count = self.data.pomp["gemaalid"].notna().sum()
        self.logger.info(f"Linked {linked_count}/{len(self.data.pomp)} pomp(en) to gemalen")

    def _transfer_capacity_for_single_pump_gemalen(self):
        """Copy maximalecapaciteit from gemaal to pomp for single-pump gemalen."""
        if "gemaalid" not in self.data.pomp.columns or "maximalecapaciteit" not in self.data.gemaal.columns:
            return

        single_pump_ids = self.data.pomp["gemaalid"].value_counts()
        single_pump_ids = single_pump_ids[single_pump_ids == 1].index
        single_pump_ids = [gid for gid in single_pump_ids if not pd.isna(gid)]

        if not single_pump_ids:
            return

        self.logger.info(f"Transferring capacity for {len(single_pump_ids)} single-pump gemalen...")
        transferred = 0

        for gemaalid in single_pump_ids:
            gemaal = self.data.gemaal[self.data.gemaal["globalid"] == gemaalid]
            if not gemaal.empty:
                capacity = gemaal["maximalecapaciteit"].iloc[0]
                if self._is_valid_value(capacity):
                    self.data.pomp.loc[self.data.pomp["gemaalid"] == gemaalid, "maximalecapaciteit"] = capacity
                    transferred += 1

        if transferred:
            self.logger.info(f"Transferred capacity for {transferred} pomp(en)")
