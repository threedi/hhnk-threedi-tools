import uuid
import warnings

import geopandas as gpd
import pandas as pd

from hhnk_threedi_tools.core.schematisation_builder.raw_export_to_DAMO_converter import RawExportToDAMOConverter

KUNSTWERKOPENING_COLUMNS = [
    "objectid",
    "code",
    "globalid",
    "stuwid",
    "regelmiddelid",
    "vormopening",
    "afvoercoefficient",
    "hoogstedoorstroombreedte",
    "laagstedoorstroombreedte",
    "doorstroomlengte",
    "hoogstedoorstroomhoogte",
    "laagstedoorstroomhoogte",
]  # Columns according to DAMO schema 2.4.1

REGELMIDDEL_COLUMNS = [
    "objectid",
    "code",
    "globalid",
    "kunstwerkopeningid",
    "duikersifonhevelid",
    "gemaalid",
    "soortregelmiddel",
    "soortregelbaarheid",
    "typemateriaalregelmiddel",
    "overlaatonderlaat",
    "hoogte",
    "breedte",
    "minimalehoogtebovenkant",
    "maximalehoogtebovenkant",
    "hoogteopening",
    "stroomrichting",
    "afvoercoefficient",
    "functieregelmiddel",
]  # Columns according to DAMO schema 2.4.1

REQUIRED_STUW_COLUMNS = ["code", "globalid", "geometry"]
OPTIONAL_STUW_COLUMNS = []


class StuwConverter(RawExportToDAMOConverter):
    """
    Convert raw export stuw (weir) data to DAMO schema 2.4.1 format.

    Manages parent-child relationships:
    - stuw → kunstwerkopening (structure opening)
    - kunstwerkopening → regelmiddel (control device)

    Creates one kunstwerkopening per stuw and one regelmiddel per kunstwerkopening,
    linking them together with unique identifiers.
    """

    def __init__(self, raw_export_converter: RawExportToDAMOConverter):
        self.data = raw_export_converter.data
        self.logger = raw_export_converter.logger

    def run(self):
        """Execute the converter to create/update stuw, kunstwerkopening, and regelmiddel layers."""
        self.logger.info("Running StuwConverter...")
        self.update_stuw_layers()

    def update_stuw_layers(self):
        """Update stuw, kunstwerkopening, and regelmiddel layers with proper relationships and IDs."""
        self.logger.info("Updating stuw layers...")

        if not self._has_valid_stuw_layer():
            self.logger.warning("No valid stuw layer found. Creating empty child layers.")
            self._create_empty_kunstwerkopening_layer()
            self._create_empty_regelmiddel_layer()
            return

        self._validate_stuw_columns()
        self._ensure_globalids(self.data.stuw, "stuw")

        # Handle kunstwerkopening layer
        if self._has_existing_kunstwerkopening_layer():
            self.logger.info("Updating existing kunstwerkopening layer...")
            self._update_existing_kunstwerkopening_layer()
            # Create missing kunstwerkopeningen for stuwen that don't have them
            self._create_missing_kunstwerkopeningen()
        else:
            self.logger.info("Creating kunstwerkopening layer from stuw data...")
            self._create_kunstwerkopening_from_stuw()

        self._ensure_globalids(self.data.kunstwerkopening, "kunstwerkopening")

        # Handle regelmiddel layer
        if self._has_existing_regelmiddel_layer():
            self.logger.info("Updating existing regelmiddel layer...")
            self._update_existing_regelmiddel_layer()
            # Create missing regelmiddelen for kunstwerkopeningen that don't have them
            self._create_missing_regelmiddelen()
        else:
            self.logger.info("Creating regelmiddel layer from kunstwerkopening data...")
            self._create_regelmiddel_from_kunstwerkopening()

        self._ensure_globalids(self.data.regelmiddel, "regelmiddel")

    def _is_valid_value(self, value):
        """Check if value is not None, NaN, or empty string."""
        return value is not None and not pd.isna(value) and (not isinstance(value, str) or value.strip())

    def _has_valid_stuw_layer(self):
        """Check if stuw layer exists and contains data."""
        try:
            self.data._ensure_loaded(["stuw"], previous_method="load_layers")
            return self.data.stuw is not None and not self.data.stuw.empty
        except (ValueError, AttributeError) as e:
            self.logger.debug(f"Stuw layer check failed: {e}")
            return False

    def _has_existing_kunstwerkopening_layer(self):
        """Check if kunstwerkopening layer already exists with data."""
        try:
            self.data._ensure_loaded(["kunstwerkopening"], previous_method="load_layers")
            return self.data.kunstwerkopening is not None and not self.data.kunstwerkopening.empty
        except (ValueError, AttributeError):
            return False

    def _has_existing_regelmiddel_layer(self):
        """Check if regelmiddel layer already exists with data."""
        try:
            self.data._ensure_loaded(["regelmiddel"], previous_method="load_layers")
            return self.data.regelmiddel is not None and not self.data.regelmiddel.empty
        except (ValueError, AttributeError):
            return False

    def _validate_stuw_columns(self):
        """Validate required columns exist, warn for missing optional columns."""
        missing_required = [col for col in REQUIRED_STUW_COLUMNS if col not in self.data.stuw.columns]
        if missing_required:
            raise ValueError(f"Stuw layer missing required columns: {missing_required}")

        missing_optional = [col for col in OPTIONAL_STUW_COLUMNS if col not in self.data.stuw.columns]
        if missing_optional:
            warnings.warn(
                f"Stuw layer missing optional columns: {missing_optional}. Using defaults.",
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

    def _create_empty_kunstwerkopening_layer(self):
        """Create empty kunstwerkopening layer with correct schema."""
        self.logger.info("Creating empty kunstwerkopening layer...")
        # Use DataFrame because kunstwerkopening has no geometry in DAMO schema
        self.data.kunstwerkopening = pd.DataFrame(columns=KUNSTWERKOPENING_COLUMNS)

    def _create_empty_regelmiddel_layer(self):
        """Create empty regelmiddel layer with correct schema."""
        self.logger.info("Creating empty regelmiddel layer...")
        crs = getattr(self.data.stuw, "crs", None) if hasattr(self.data, "stuw") else None
        # Use GeoDataFrame because regelmiddel has geometry (inherited from stuw)
        self.data.regelmiddel = gpd.GeoDataFrame(columns=REGELMIDDEL_COLUMNS, geometry="geometry", crs=crs)

    def _create_kunstwerkopening_from_stuw(self):
        """Create kunstwerkopening layer with one opening per stuw."""
        self.logger.info(f"Creating {len(self.data.stuw)} kunstwerkopening record(s)...")

        kunstwerkopening_records = [
            {
                "globalid": str(uuid.uuid4()),
                "stuwid": row.get("globalid"),
                "code": row.get("code", f"kunstwerkopening_{idx}"),
            }
            for idx, row in self.data.stuw.iterrows()
        ]

        kunstwerkopening_df = pd.DataFrame(kunstwerkopening_records)

        # Add missing columns
        for col in KUNSTWERKOPENING_COLUMNS:
            if col not in kunstwerkopening_df.columns:
                kunstwerkopening_df[col] = None

        self.data.kunstwerkopening = kunstwerkopening_df[KUNSTWERKOPENING_COLUMNS]

    def _update_existing_kunstwerkopening_layer(self):
        """Validate existing kunstwerkopening stuwid references."""
        if "stuwid" not in self.data.kunstwerkopening.columns:
            self.data.kunstwerkopening["stuwid"] = None

        # Validate that existing stuwid values reference valid stuwen
        if self.data.kunstwerkopening["stuwid"].notna().any():
            valid_ids = self.data.stuw["globalid"].values
            invalid_mask = (
                ~self.data.kunstwerkopening["stuwid"].isin(valid_ids) & self.data.kunstwerkopening["stuwid"].notna()
            )
            if invalid_mask.any():
                self.logger.warning(
                    f"Found {invalid_mask.sum()} kunstwerkopeningen with invalid stuwid, clearing them"
                )
                self.data.kunstwerkopening.loc[invalid_mask, "stuwid"] = None

        linked_count = self.data.kunstwerkopening["stuwid"].notna().sum()
        self.logger.info(
            f"Validated {linked_count}/{len(self.data.kunstwerkopening)} kunstwerkopening(en) linked to stuwen"
        )

    def _create_regelmiddel_from_kunstwerkopening(self):
        """Create regelmiddel layer with one control device per kunstwerkopening, using stuw geometry."""
        self.logger.info(f"Creating {len(self.data.kunstwerkopening)} regelmiddel record(s)...")

        # Merge with stuw to get original geometry
        merged = self.data.kunstwerkopening.merge(
            self.data.stuw[["globalid", "geometry"]],
            left_on="stuwid",
            right_on="globalid",
            how="left",
            suffixes=("", "_stuw"),
        )

        regelmiddel_records = [
            {
                "globalid": str(uuid.uuid4()),
                "kunstwerkopeningid": row.get("globalid"),
                "code": row.get("code", f"regelmiddel_{idx}"),
                "geometry": row.geometry_stuw
                if hasattr(row, "geometry_stuw") and row.geometry_stuw is not None
                else row.geometry,
            }
            for idx, row in merged.iterrows()
        ]

        regelmiddel_gdf = gpd.GeoDataFrame(regelmiddel_records, crs=self.data.stuw.crs)

        # Add missing columns
        for col in REGELMIDDEL_COLUMNS:
            if col not in regelmiddel_gdf.columns:
                regelmiddel_gdf[col] = None

        self.data.regelmiddel = regelmiddel_gdf[REGELMIDDEL_COLUMNS + ["geometry"]]

    def _update_existing_regelmiddel_layer(self):
        """Validate existing regelmiddel kunstwerkopeningid references."""
        if "kunstwerkopeningid" not in self.data.regelmiddel.columns:
            self.data.regelmiddel["kunstwerkopeningid"] = None

        # Validate that existing kunstwerkopeningid values reference valid kunstwerkopeningen
        if self.data.regelmiddel["kunstwerkopeningid"].notna().any():
            valid_ids = self.data.kunstwerkopening["globalid"].values
            invalid_mask = (
                ~self.data.regelmiddel["kunstwerkopeningid"].isin(valid_ids)
                & self.data.regelmiddel["kunstwerkopeningid"].notna()
            )
            if invalid_mask.any():
                self.logger.warning(
                    f"Found {invalid_mask.sum()} regelmiddelen with invalid kunstwerkopeningid, clearing them"
                )
                self.data.regelmiddel.loc[invalid_mask, "kunstwerkopeningid"] = None

        linked_count = self.data.regelmiddel["kunstwerkopeningid"].notna().sum()
        self.logger.info(
            f"Validated {linked_count}/{len(self.data.regelmiddel)} regelmiddel(en) linked to kunstwerkopeningen"
        )

    def _create_missing_kunstwerkopeningen(self):
        """Create kunstwerkopeningen for stuwen that don't have them yet."""
        # Get stuwen that already have kunstwerkopeningen
        existing_stuwids = self.data.kunstwerkopening["stuwid"].dropna().unique()

        # Find stuwen without kunstwerkopeningen
        missing_mask = ~self.data.stuw["globalid"].isin(existing_stuwids)
        stuwen_without_kunstwerk = self.data.stuw[missing_mask]

        if stuwen_without_kunstwerk.empty:
            self.logger.info("All stuwen already have kunstwerkopeningen")
            return

        self.logger.info(f"Creating {len(stuwen_without_kunstwerk)} missing kunstwerkopening(en)...")

        # Create new kunstwerkopeningen
        new_kunstwerkopeningen = [
            {
                "globalid": str(uuid.uuid4()),
                "stuwid": row.get("globalid"),
                "code": row.get("code", f"kunstwerkopening_{idx}"),
            }
            for idx, row in stuwen_without_kunstwerk.iterrows()
        ]

        new_df = pd.DataFrame(new_kunstwerkopeningen)

        # Add missing columns to both DataFrames to ensure consistent dtypes
        for col in KUNSTWERKOPENING_COLUMNS:
            if col not in new_df.columns:
                new_df[col] = None
            if col not in self.data.kunstwerkopening.columns:
                self.data.kunstwerkopening[col] = None

        # Convert both to object dtype for all columns to ensure consistent dtypes
        existing_df = self.data.kunstwerkopening[KUNSTWERKOPENING_COLUMNS].astype(object)
        new_df_typed = new_df[KUNSTWERKOPENING_COLUMNS].astype(object)

        # Append to existing kunstwerkopeningen
        self.data.kunstwerkopening = pd.concat([existing_df, new_df_typed], ignore_index=True)

        self.logger.info(f"Added {len(new_kunstwerkopeningen)} new kunstwerkopening(en)")

    def _create_missing_regelmiddelen(self):
        """Create regelmiddelen for kunstwerkopeningen that don't have them yet."""
        import uuid

        # Get kunstwerkopeningen that already have regelmiddelen
        existing_kunstwerkopeningids = self.data.regelmiddel["kunstwerkopeningid"].dropna().unique()

        # Find kunstwerkopeningen without regelmiddelen
        missing_mask = ~self.data.kunstwerkopening["globalid"].isin(existing_kunstwerkopeningids)
        kunstwerk_without_regelmiddel = self.data.kunstwerkopening[missing_mask]

        if kunstwerk_without_regelmiddel.empty:
            self.logger.info("All kunstwerkopeningen already have regelmiddelen")
            return

        self.logger.info(f"Creating {len(kunstwerk_without_regelmiddel)} missing regelmiddel(en)...")

        # Merge with stuw to get original geometry
        kunstwerk_with_stuw = kunstwerk_without_regelmiddel.merge(
            self.data.stuw[["globalid", "geometry"]],
            left_on="stuwid",
            right_on="globalid",
            how="left",
            suffixes=("", "_stuw"),
        )

        # Create new regelmiddelen
        new_regelmiddelen = [
            {
                "globalid": str(uuid.uuid4()),
                "kunstwerkopeningid": row.get("globalid"),
                "code": row.get("code", f"regelmiddel_{idx}"),
                "geometry": row.geometry_stuw if hasattr(row, "geometry_stuw") else row.get("geometry_stuw"),
            }
            for idx, row in kunstwerk_with_stuw.iterrows()
        ]

        new_gdf = gpd.GeoDataFrame(new_regelmiddelen, crs=self.data.stuw.crs)

        # Add missing columns with consistent object dtype to avoid FutureWarning
        for col in REGELMIDDEL_COLUMNS:
            if col not in new_gdf.columns:
                new_gdf[col] = pd.Series(dtype=object)
            if col not in self.data.regelmiddel.columns:
                self.data.regelmiddel[col] = pd.Series(dtype=object)

        # Append to existing regelmiddelen
        self.data.regelmiddel = pd.concat(
            [self.data.regelmiddel[REGELMIDDEL_COLUMNS + ["geometry"]], new_gdf[REGELMIDDEL_COLUMNS + ["geometry"]]],
            ignore_index=True,
        )
        # Ensure result is a GeoDataFrame
        self.data.regelmiddel = gpd.GeoDataFrame(self.data.regelmiddel, geometry="geometry", crs=self.data.stuw.crs)

        self.logger.info(f"Added {len(new_regelmiddelen)} new regelmiddel(en)")
