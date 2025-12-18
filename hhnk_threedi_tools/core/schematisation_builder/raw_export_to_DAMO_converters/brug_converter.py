import uuid
import warnings

import geopandas as gpd
import pandas as pd

from hhnk_threedi_tools.core.schematisation_builder.raw_export_to_DAMO_converter import RawExportToDAMOConverter

DOORSTROOMOPENING_COLUMNS = [
    "objectid",
    "code",
    "globalid",
    "brugid",
    "doorvaartnummer",
    "breedteopening",
    "indicatiedoorvaarbaarheid",
    "laagstedoorstroomhoogte",
    "hoogteopening",
    "doorstroomlengte",
    "afvoercoefficient",
]  # Columns according to DAMO schema 2.4.1

REQUIRED_BRUG_COLUMNS = ["code", "globalid", "geometry"]
OPTIONAL_BRUG_COLUMNS = []


class BrugConverter(RawExportToDAMOConverter):
    """
    Convert raw export brug (bridge) data to DAMO schema 2.4.1 format.

    Manages parent-child relationship:
    - brug → doorstroomopening (flow opening)

    Creates one doorstroomopening per brug and links them with brugid,
    ensuring each has a unique identifier.
    """

    def __init__(self, raw_export_converter: RawExportToDAMOConverter):
        self.data = raw_export_converter.data
        self.logger = raw_export_converter.logger

    def run(self):
        """Execute the converter to create/update brug and doorstroomopening layers."""
        self.logger.info("Running BrugConverter...")
        self.update_brug_layers()

    def update_brug_layers(self):
        """Update brug and doorstroomopening layers with proper relationships and IDs."""
        self.logger.info("Updating brug layers...")

        if not self._has_valid_brug_layer():
            self.logger.warning("No valid brug layer found. Creating empty doorstroomopening layer.")
            self._create_empty_doorstroomopening_layer()
            return

        self._validate_brug_columns()
        self._ensure_globalids(self.data.brug, "brug")

        # Handle doorstroomopening layer
        if self._has_existing_doorstroomopening_layer():
            self.logger.info("Updating existing doorstroomopening layer...")
            self._update_existing_doorstroomopening_layer()
            # Create missing doorstroomopeningen for bruggen that don't have them
            self._create_missing_doorstroomopeningen()
        else:
            self.logger.info("Creating doorstroomopening layer from brug data...")
            self._create_doorstroomopening_from_brug()

        self._ensure_globalids(self.data.doorstroomopening, "doorstroomopening")

    def _is_valid_value(self, value):
        """Check if value is not None, NaN, or empty string."""
        return value is not None and not pd.isna(value) and (not isinstance(value, str) or value.strip())

    def _has_valid_brug_layer(self):
        """Check if brug layer exists and contains data."""
        try:
            self.data._ensure_loaded(["brug"], previous_method="load_layers")
            return self.data.brug is not None and not self.data.brug.empty
        except (ValueError, AttributeError) as e:
            self.logger.debug(f"Brug layer check failed: {e}")
            return False

    def _has_existing_doorstroomopening_layer(self):
        """Check if doorstroomopening layer already exists with data."""
        try:
            self.data._ensure_loaded(["doorstroomopening"], previous_method="load_layers")
            return self.data.doorstroomopening is not None and not self.data.doorstroomopening.empty
        except (ValueError, AttributeError):
            return False

    def _validate_brug_columns(self):
        """Validate required columns exist, warn for missing optional columns."""
        missing_required = [col for col in REQUIRED_BRUG_COLUMNS if col not in self.data.brug.columns]
        if missing_required:
            raise ValueError(f"Brug layer missing required columns: {missing_required}")

        missing_optional = [col for col in OPTIONAL_BRUG_COLUMNS if col not in self.data.brug.columns]
        if missing_optional:
            warnings.warn(
                f"Brug layer missing optional columns: {missing_optional}. Using defaults.",
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

    def _create_empty_doorstroomopening_layer(self):
        """Create empty doorstroomopening layer with correct schema."""
        self.logger.info("Creating empty doorstroomopening layer...")
        # Use DataFrame because doorstroomopening has no geometry in DAMO schema
        self.data.doorstroomopening = pd.DataFrame(columns=DOORSTROOMOPENING_COLUMNS)

    def _create_doorstroomopening_from_brug(self):
        """Create doorstroomopening layer with one opening per brug."""
        self.logger.info(f"Creating {len(self.data.brug)} doorstroomopening record(s)...")

        doorstroomopening_records = [
            {
                "globalid": str(uuid.uuid4()),
                "brugid": row.get("globalid"),
                "code": row.get("code", f"doorstroomopening_{idx}"),
            }
            for idx, row in self.data.brug.iterrows()
        ]

        doorstroomopening_df = pd.DataFrame(doorstroomopening_records)

        # Add missing columns
        for col in DOORSTROOMOPENING_COLUMNS:
            if col not in doorstroomopening_df.columns:
                doorstroomopening_df[col] = None

        self.data.doorstroomopening = doorstroomopening_df[DOORSTROOMOPENING_COLUMNS]

    def _update_existing_doorstroomopening_layer(self):
        """Validate existing doorstroomopening brugid references."""
        if "brugid" not in self.data.doorstroomopening.columns:
            self.data.doorstroomopening["brugid"] = None

        # Validate that existing brugid values reference valid bruggen
        if self.data.doorstroomopening["brugid"].notna().any():
            valid_ids = self.data.brug["globalid"].values
            invalid_mask = (
                ~self.data.doorstroomopening["brugid"].isin(valid_ids) & self.data.doorstroomopening["brugid"].notna()
            )
            if invalid_mask.any():
                self.logger.warning(
                    f"Found {invalid_mask.sum()} doorstroomopeningen with invalid brugid, clearing them"
                )
                self.data.doorstroomopening.loc[invalid_mask, "brugid"] = None

        linked_count = self.data.doorstroomopening["brugid"].notna().sum()
        self.logger.info(
            f"Validated {linked_count}/{len(self.data.doorstroomopening)} doorstroomopening(en) linked to bruggen"
        )

    def _create_missing_doorstroomopeningen(self):
        """Create doorstroomopeningen for bruggen that don't have them yet."""
        # Get bruggen that already have doorstroomopeningen
        existing_brugids = self.data.doorstroomopening["brugid"].dropna().unique()

        # Find bruggen without doorstroomopeningen
        missing_mask = ~self.data.brug["globalid"].isin(existing_brugids)
        bruggen_without_doorstroom = self.data.brug[missing_mask]

        if bruggen_without_doorstroom.empty:
            self.logger.info("All bruggen already have doorstroomopeningen")
            return

        self.logger.info(f"Creating {len(bruggen_without_doorstroom)} missing doorstroomopening(en)...")

        # Create new doorstroomopeningen
        new_doorstroomopeningen = [
            {
                "globalid": str(uuid.uuid4()),
                "brugid": row.get("globalid"),
                "code": row.get("code", f"doorstroomopening_{idx}"),
            }
            for idx, row in bruggen_without_doorstroom.iterrows()
        ]

        new_df = pd.DataFrame(new_doorstroomopeningen)

        # Add missing columns
        for col in DOORSTROOMOPENING_COLUMNS:
            if col not in new_df.columns:
                new_df[col] = None
            if col not in self.data.doorstroomopening.columns:
                self.data.doorstroomopening[col] = None

        # Convert both to object dtype for all columns to ensure consistent dtypes
        existing_df = self.data.doorstroomopening[DOORSTROOMOPENING_COLUMNS].astype(object)
        new_df_typed = new_df[DOORSTROOMOPENING_COLUMNS].astype(object)

        # Append to existing doorstroomopeningen
        self.data.doorstroomopening = pd.concat(
            [existing_df, new_df_typed],
            ignore_index=True,
        )

        self.logger.info(f"Added {len(new_doorstroomopeningen)} new doorstroomopening(en)")
