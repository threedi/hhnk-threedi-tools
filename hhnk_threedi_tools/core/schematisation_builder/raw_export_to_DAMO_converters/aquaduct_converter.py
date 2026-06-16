import uuid
import warnings

import geopandas as gpd
import pandas as pd

from hhnk_threedi_tools.core.schematisation_builder.raw_export_to_DAMO_converter import RawExportToDAMOConverter

# Mapping from aquaduct attributes to duikersifonhevel attributes
ATTRIBUTE_MAPPING = {
    # Direct mappings
    "soortmateriaal": "soortmateriaal",
    "typemethoderuwheid": "typemethoderuwheid",
    "ruwheid": "ruwheid",
    "hoogteconstructie": "hoogteopening",
    "breedte": "breedteopening",
    "bodemhoogtebenedenstrooms": "hoogtebinnenonderkantbene",
    "bodemhoogtebovenstrooms": "hoogtebinnenonderkantbov",
}

DUIKERSIFONHEVEL_COLUMNS = [
    "objectid",
    "code",
    "globalid",
    "indicatiewaterkerend",
    "kerendehoogte",
    "typewaterkerendeconstructie",
    "indpeilregulpeilscheidend",
    "categorie",
    "lengte",
    "hoogteopening",
    "breedteopening",
    "hoogtebinnenonderkantbene",
    "drempelpeil",
    "hoogtebinnenonderkantbov",
    "signaleringspeil",
    "vormkoker",
    "sluitpeil",
    "soortmateriaal",
    "openkeerpeil",
    "openingspeil",
    "typekruising",
    "ontwerpbuitenwaterstand",
    "afvoercoefficient",
    "aantaldoorstroomopeningen",
    "typemethoderuwheid",
    "ruwheid",
    "intreeverlies",
    "uittreeverlies",
    "bochtenknilverlies",
    "waterkeringid",
]  # Columns according to DAMO schema 2.4.1

REQUIRED_AQUADUCT_COLUMNS = ["code", "globalid", "geometry"]
OPTIONAL_AQUADUCT_COLUMNS = []


class AquaductConverter(RawExportToDAMOConverter):
    """
    Convert raw export aquaduct (aqueduct) data to DAMO duikersifonhevel layer.

    Aquaducts are treated as a special type of duikersifonhevel for validation purposes.
    This converter:
    - Loads aquaduct data from raw export
    - Maps aquaduct attributes to duikersifonhevel schema
    - Appends aquaduct records to duikersifonhevel layer (or creates if missing)
    - Generates unique identifiers for all records
    """

    def __init__(self, raw_export_converter: RawExportToDAMOConverter):
        self.data = raw_export_converter.data
        self.logger = raw_export_converter.logger

    def run(self):
        """Execute the converter to add aquaduct data to duikersifonhevel layer."""
        self.convert_aquaduct_to_duikersifonhevel()

    def convert_aquaduct_to_duikersifonhevel(self):
        """Convert aquaduct records and add them to duikersifonhevel layer."""
        if not self._has_valid_aquaduct_layer():
            self.logger.info("No valid aquaduct layer found. Skipping conversion.")
            return

        self._validate_aquaduct_columns()
        self._ensure_globalids(self.data.aquaduct, "aquaduct")

        # Create duikersifonhevel records from aquaduct
        converted_records = self._create_duikersifonhevel_from_aquaduct()

        # Add to existing duikersifonhevel or create new layer
        if self._has_existing_duikersifonhevel_layer():
            self._append_to_duikersifonhevel(converted_records)
        else:
            self.data.duikersifonhevel = converted_records

        self._ensure_globalids(self.data.duikersifonhevel, "duikersifonhevel")
        self.logger.info(
            f"Added {len(converted_records)} aquaduct(s) to duikersifonhevel (total: {len(self.data.duikersifonhevel)})"
        )

    def _is_valid_value(self, value):
        """Check if value is not None, NaN, or empty string."""
        return value is not None and not pd.isna(value) and (not isinstance(value, str) or value.strip())

    def _has_valid_aquaduct_layer(self):
        """Check if aquaduct layer exists and contains data."""
        try:
            self.data._ensure_loaded(["aquaduct"], previous_method="load_layers")
            return self.data.aquaduct is not None and not self.data.aquaduct.empty
        except (ValueError, AttributeError) as e:
            self.logger.debug(f"Aquaduct layer check failed: {e}")
            return False

    def _has_existing_duikersifonhevel_layer(self):
        """Check if duikersifonhevel layer already exists with data."""
        if not hasattr(self.data, "duikersifonhevel"):
            return False
        return self.data.duikersifonhevel is not None and not self.data.duikersifonhevel.empty

    def _validate_aquaduct_columns(self):
        """Validate required columns exist, warn for missing optional columns."""
        missing_required = [col for col in REQUIRED_AQUADUCT_COLUMNS if col not in self.data.aquaduct.columns]
        if missing_required:
            raise ValueError(f"Aquaduct layer missing required columns: {missing_required}")

        missing_optional = [col for col in OPTIONAL_AQUADUCT_COLUMNS if col not in self.data.aquaduct.columns]
        if missing_optional:
            warnings.warn(
                f"Aquaduct layer missing optional columns: {missing_optional}. Using defaults.",
                UserWarning,
            )

    def _ensure_globalids(self, gdf, layer_name):
        """Ensure all records in GeoDataFrame have valid globalid."""
        if "globalid" not in gdf.columns:
            gdf["globalid"] = None

        missing_mask = gdf["globalid"].isna() | (gdf["globalid"] == "")
        num_missing = missing_mask.sum()

        if num_missing > 0:
            gdf.loc[missing_mask, "globalid"] = [str(uuid.uuid4()) for _ in range(num_missing)]

    def _create_duikersifonhevel_from_aquaduct(self):
        """Create duikersifonhevel records from aquaduct data with attribute mapping."""
        # Create new records with mapped attributes
        converted_records = []
        for idx, row in self.data.aquaduct.iterrows():
            record = {
                "globalid": row.get("globalid") if self._is_valid_value(row.get("globalid")) else str(uuid.uuid4()),
                "code": row.get("code", f"aquaduct_{idx}"),
                "geometry": row.geometry,
                "typekruising": "Aquaduct",  # Always set to Aquaduct to identify these records
            }

            # Map attributes from aquaduct to duikersifonhevel
            for aqua_attr, duiker_attr in ATTRIBUTE_MAPPING.items():
                if aqua_attr in self.data.aquaduct.columns:
                    value = row.get(aqua_attr)
                    if self._is_valid_value(value):
                        record[duiker_attr] = value

            converted_records.append(record)

        # Create GeoDataFrame with proper CRS
        duikersifonhevel_gdf = gpd.GeoDataFrame(converted_records, crs=self.data.aquaduct.crs)

        # Add all missing columns from DUIKERSIFONHEVEL_COLUMNS
        for col in DUIKERSIFONHEVEL_COLUMNS:
            if col not in duikersifonhevel_gdf.columns:
                duikersifonhevel_gdf[col] = None

        return duikersifonhevel_gdf[DUIKERSIFONHEVEL_COLUMNS + ["geometry"]]

    def _append_to_duikersifonhevel(self, new_records):
        """Append converted aquaduct records to existing duikersifonhevel layer."""
        # Ensure both DataFrames have the same columns
        for col in DUIKERSIFONHEVEL_COLUMNS + ["geometry"]:
            if col not in self.data.duikersifonhevel.columns:
                self.data.duikersifonhevel[col] = None
            if col not in new_records.columns:
                new_records[col] = None

        # Convert both to consistent column order
        existing_gdf = self.data.duikersifonhevel[DUIKERSIFONHEVEL_COLUMNS + ["geometry"]].copy()
        new_gdf = new_records[DUIKERSIFONHEVEL_COLUMNS + ["geometry"]].copy()

        # Concatenate
        self.data.duikersifonhevel = pd.concat(
            [existing_gdf, new_gdf],
            ignore_index=True,
        )
        # Ensure result is a GeoDataFrame
        self.data.duikersifonhevel = gpd.GeoDataFrame(
            self.data.duikersifonhevel, geometry="geometry", crs=self.data.aquaduct.crs
        )
