import uuid
from pathlib import Path
from typing import Optional

import geopandas as gpd
import hhnk_research_tools as hrt
import numpy as np
import pandas as pd
from shapely.geometry import MultiLineString
from shapely.validation import make_valid


class ProfileIntermediateConverter:
    """
    Intermediate converter for profile data.
    From source (DAMO and ODS/CSO) to intermediate format, ready for converting to HyDAMO.

    Functionalities
    ---------------
    - Read and validate layers
    - Linemerge hydroobjects on peilvakken
    - Create profielgroep, profiellijn, and profielpunt
    - Write output

    Parameters
    ----------
    damo_file_path : Path
        Path to the DAMO geopackage file.
    ods_cso_file_path : Path
        Path to the ODS/CSO geopackage file.
    logger : logging.Logger, optional
        Logger for logging messages. If not provided, a default logger will be used.
    """

    def __init__(self, damo_file_path: Path, ods_cso_file_path: Path, logger=None):
        self.damo_file_path = Path(damo_file_path)
        self.ods_cso_file_path = Path(ods_cso_file_path)

        if logger:
            self.logger = logger
        else:
            self.logger = hrt.logging.get_logger(__name__)

        self.hydroobject: Optional[gpd.GeoDataFrame] = None
        self.hydroobject_linemerged: Optional[gpd.GeoDataFrame] = None

        self.gw_pro: Optional[gpd.GeoDataFrame] = None
        self.gw_prw: Optional[gpd.GeoDataFrame] = None
        self.gw_pbp: Optional[gpd.GeoDataFrame] = None
        self.iws_geo_beschr_profielpunten: Optional[gpd.GeoDataFrame] = None

        self.gecombineerde_peilen: Optional[gpd.GeoDataFrame] = None

    def load_layers(self):
        """Load and preprocess necessary layers."""
        self.logger.info("Loading layers into ProfileIntermediateConverter...")
        self.hydroobject = self._load_and_validate(self.damo_file_path, "hydroobject")

        self.gw_pro = self._load_and_validate(self.damo_file_path, "gw_pro")
        self.gw_prw = self._load_and_validate(self.damo_file_path, "gw_prw")
        self.gw_pbp = self._load_and_validate(self.damo_file_path, "gw_pbp")
        self.iws_geo_beschr_profielpunten = self._load_and_validate(
            self.damo_file_path, "iws_geo_beschr_profielpunten"
        )

        gecombineerde_peilen = self._load_and_validate(self.ods_cso_file_path, "gecombineerde_peilen")
        self.gecombineerde_peilen = gecombineerde_peilen.explode(index_parts=False).reset_index(drop=True)

    def _load_and_validate(self, source_path, layer_name: str) -> gpd.GeoDataFrame:
        """Load a layer from the DAMO geopackage and apply make_valid to its geometries."""
        gdf = gpd.read_file(source_path, layer=layer_name)
        if "geometry" in gdf.columns:
            gdf["geometry"] = gdf["geometry"].apply(make_valid)
        return gdf

    def create_profile_tables(self):
        """
        Create profielgroep, profiellijn, and profielpunten.
        Based on the layers 'gw_pro', 'gw_prw', 'gw_pbp' and 'iws_geo_beschr_profielpunten'.
        """
        self.logger.info("Creating profile tables...")
        if (
            self.gw_pro is None
            or self.gw_prw is None
            or self.gw_pbp is None
            or self.iws_geo_beschr_profielpunten is None
        ):
            raise ValueError("Layers not loaded. Call load_layers() first.")

        # Create profielgroep and profiellijn, both are a copy of gw_pro
        self.profielgroep = self.gw_pro.copy()
        self.profielgroep["GlobalID"] = [str(uuid.uuid4()) for _ in range(len(self.profielgroep))]

        # Rename
        self.profielgroep = self.profielgroep.rename(columns={"PRO_ID": "code"})

        # Attach profielgroep to hydroObjectID based on the geometry
        self.assign_hydroobject_ids()

        self.profielgroep["geometry"] = None  # Profielgroep has no geometry

        # Drop unnecessary columns
        self.profielgroep = self.profielgroep[["GlobalID", "code", "geometry", "hydroobjectID"]]

        self.profiellijn = self.gw_pro.copy()
        self.profiellijn["GlobalID"] = [str(uuid.uuid4()) for _ in range(len(self.profiellijn))]
        self.profiellijn["profielgroepID"] = self.profielgroep["GlobalID"]

        # Rename columns in profiellijn and drop unnecessary columns
        self.profiellijn = self.profiellijn.rename(
            columns={"OPRDATOP": "datumInwinning", "OSMOMSCH": "namespace", "PRO_ID": "code"}
        )
        self.profiellijn = self.profiellijn[
            ["GlobalID", "code", "datumInwinning", "namespace", "geometry", "profielgroepID"]
        ]

        # Create profielpunt
        # Geometry is in iws_geo_beschr_profielpunten
        # Change column name PBP_PBP_ID to code
        # Create uuid for profielpunt, in profielpunt this is GlobalID
        self.profielpunt = self.iws_geo_beschr_profielpunten.copy()
        self.profielpunt = self.profielpunt.rename(columns={"PBP_PBP_ID": "code"})
        self.profielpunt["GlobalID"] = [str(uuid.uuid4()) for _ in range(len(self.profielpunt))]

        # Other information is in gw_pbp
        # We create new columns in profielpunt for the information from gw_pbp
        # We can use the PBP_ID in gw_pbp to join the two tables to code in profielpunt
        self.profielpunt = self.profielpunt.merge(
            self.gw_pbp[["PBP_ID", "PRW_PRW_ID", "PBPSOORT", "IWS_VOLGNR", "IWS_HOOGTE", "IWS_AFSTAND"]],
            left_on="code",
            right_on="PBP_ID",
            how="left",
        )
        self.profielpunt = self.profielpunt.rename(
            columns={
                "PBPSOORT": "typeProfielPunt",
                "IWS_VOLGNR": "codeVolgnummer",
                "IWS_HOOGTE": "hoogte",
                "IWS_AFSTAND": "afstand",
            }
        )

        # We filter profielpunt to keep only 'vaste bodem'
        # 'vaste bodem' is denoted as Z1 in OSMOMSCH column from GW_PRW
        #  GW_PRW can be mapped based on PRW_ID in GW_PRW and PRW_PRW_ID in profielpunt
        # We join, then filter
        self.profielpunt = self.profielpunt.merge(
            self.gw_prw[["PRW_ID", "PRO_PRO_ID", "OSMOMSCH"]],
            left_on="PRW_PRW_ID",
            right_on="PRW_ID",
            how="left",
        )
        self.profielpunt = self.profielpunt[self.profielpunt["OSMOMSCH"] == "Z1"]

        # Log warning if there are still duplicates in profielpunt (based on geometry) # TODO temp validation here
        duplicates = self.profielpunt[self.profielpunt.duplicated(subset=["geometry"], keep=False)]
        if not duplicates.empty:
            self.logger.warning(
                f"Found {len(duplicates)} duplicate geometries in profielpunt. "
                # f"Codes: {duplicates['code'].unique()}"
            )

        # Now we need to add the profielLijnID to every profielpunt
        # We can use the PRO_PRO_ID in gw_prw to find the globalID in profiellijn with a matching PRO_ID (which was renamed to code)
        self.profielpunt = self.profielpunt.merge(
            self.profiellijn[["code", "GlobalID"]],
            left_on="PRO_PRO_ID",
            right_on="code",
            how="left",
            suffixes=("", "_profiellijn"),
        )
        self.profielpunt = self.profielpunt.rename(columns={"GlobalID_profiellijn": "profielLijnID"})
        self.profielpunt = self.profielpunt.drop(columns=["code_profiellijn"])

        # Drop columns that are not needed
        self.profielpunt = self.profielpunt.drop(columns=["PRW_ID", "PBP_ID", "PRO_PRO_ID", "PRW_PRW_ID"])

    def assign_hydroobject_ids(self):
        if not "GlobalID" in self.hydroobject.columns:
            self.hydroobject["GlobalID"] = [str(uuid.uuid4()) for _ in range(len(self.hydroobject))]

        no_match_codes = []
        multiple_match_codes = []

        for idx, row in self.profielgroep.iterrows():
            intersecting = self.hydroobject[self.hydroobject.intersects(row.geometry)]

            gids_list = intersecting["GlobalID"].tolist()

            if not gids_list:
                no_match_codes.append(row.get("code"))
                self.profielgroep.at[idx, "hydroobjectID"] = None
                continue

            self.profielgroep.at[idx, "hydroobjectID"] = gids_list[0]

            if len(gids_list) > 1:
                multiple_match_codes.append((row.get("code"), len(gids_list)))
                for i, gid in enumerate(gids_list[1:], start=2):
                    self.profielgroep.at[idx, f"hydroobjectID{i}"] = gid

        warnings_list = []
        if no_match_codes:
            warnings_list.append(f"No intersection for profielgroep with code(s): {no_match_codes}")
        if multiple_match_codes:
            details = ", ".join([f"{idx} ({count})" for idx, count in multiple_match_codes])
            warnings_list.append(f"Multiple intersections for profielgroep with code(s): {details}")

        if warnings_list:
            self.logger.warning("\n".join(warnings_list))

    def process_linemerge(self):
        """Run line merge algorithm and store result."""
        self.logger.info("ProfileIntermediateConverter is linemerging hydroobjects for peilgebieden...")
        if self.hydroobject is None or self.gecombineerde_peilen is None:
            raise ValueError("Layers not loaded. Call load_damo_layers() first.")
        self.hydroobject_linemerged = self._linemerge_hydroobjects(self.gecombineerde_peilen, self.hydroobject)

    def _linemerge_hydroobjects(self, gecombineerde_peilen, hydroobject):
        """Merges hydroobjects within a peilgebied, split by primary and secondary categories."""
        merged_hydroobjects = []
        hydroobject_map = {}

        for _, peilgebied in gecombineerde_peilen.iterrows():
            hydroobjects = hydroobject[hydroobject.intersects(peilgebied.geometry)]
            if hydroobjects.empty:
                continue

            hydroobjects = hydroobjects.clip(peilgebied.geometry)
            if hydroobjects.empty:
                continue

            # Split into primary and secondary
            is_primary = hydroobjects["CATEGORIEOPPWATERLICHAAM"] == 1
            primary = hydroobjects[is_primary]
            secondary = hydroobjects[~is_primary | hydroobjects["CATEGORIEOPPWATERLICHAAM"].isna()]

            for category_label, group in [("primary", primary), ("secondary", secondary)]:
                if group.empty:
                    continue

                merged_geom = group.geometry.union_all()

                if merged_geom.geom_type == "GeometryCollection":
                    self.logger.info(
                        f"GeometryCollection in peilgebied {peilgebied['PeilgebiedPraktijk_ID']} ({category_label}). Keeping only (Multi)LineStrings."
                    )
                    merged_geom = geometrycollection_to_linestring(merged_geom)

                if merged_geom.geom_type == "LineString":
                    merged_geom = MultiLineString([merged_geom])

                codes = group["CODE"].unique()
                linemerge_id = str(uuid.uuid4())

                for code in codes:
                    hydroobject_map[code] = linemerge_id

                merged_hydroobjects.append({
                    "geometry": merged_geom,
                    "hydroobjectCODE": codes,
                    "linemergeID": linemerge_id,
                    "peilgebiedID": peilgebied["PeilgebiedPraktijk_ID"],
                    "categorie": category_label
                })

        merged_hydroobjects_gdf = gpd.GeoDataFrame(merged_hydroobjects, crs=hydroobject.crs)
        if merged_hydroobjects_gdf.empty:
            return None
        else:
            self.hydroobject_map = hydroobject_map
            return merged_hydroobjects_gdf
        
    def find_linemerge_id_by_hydroobject_code(self, code: str):
        """
        Find the linemergeID by hydroobject code.
        """
        if code in self.hydroobject_map:
            return self.hydroobject_map[code]
        else:
            self.logger.warning(f"No linemerge found for hydroobject with code {code}.")
            return None

    def find_peilgebied_id_by_hydroobject_code(self, code: str):
        """
        Find the peilgebiedID by hydroobject code.
        """
        if not hasattr(self, "hydroobject_linemerged"):
            self.logger.error("Linemerged hydroobjects not yet processed.")
            return None

        # First, find the linemergeID for this hydroobject code
        if code in self.hydroobject_map:
            linemerge_id = self.hydroobject_map[code]

            # Now, lookup the peilgebiedID in the linemerged GeoDataFrame
            match = self.hydroobject_linemerged[self.hydroobject_linemerged["linemergeID"] == linemerge_id]

            if match.empty:
                self.logger.warning(f"No matching linemerge found for hydroobject code '{code}'.")
                return None
            else:
                peilgebied_id = match.iloc[0]["peilgebiedID"]
                return peilgebied_id
        else:
            self.logger.warning(f"Hydroobject code '{code}' not found in the linemerged map.")
            return None

    def write_outputs(self, output_path: Path):
        """
        Write all GeoDataFrame attributes of this class to a GeoPackage or individual files.
        """
        output_path = Path(output_path)
        geodf_attrs = {name: val for name, val in self.__dict__.items() if isinstance(val, gpd.GeoDataFrame)}

        if not geodf_attrs:
            self.logger.warning("No GeoDataFrames found to write.")
            return

        if output_path.suffix == ".gpkg":
            for name, gdf in geodf_attrs.items():
                gdf.to_file(output_path, layer=name, driver="GPKG")
                self.logger.info(f"Wrote layer '{name}' to {output_path}")
        else:
            output_path.mkdir(parents=True, exist_ok=True)
            for name, gdf in geodf_attrs.items():
                gdf.to_file(output_path / f"{name}.gpkg", driver="GPKG")
                self.logger.info(f"Wrote file '{name}.gpkg' to {output_path}")


# General functions
def geometrycollection_to_linestring(geometry):
    """
    Filter a geometrycollection to keep only LineString or MultiLineString
    """
    if geometry.geom_type == "GeometryCollection":
        fixed_geometry = [geom for geom in geometry.geoms if geom.geom_type in ["LineString", "MultiLineString"]]
        if fixed_geometry:
            merged_geometry = gpd.GeoSeries(fixed_geometry).union_all()
            return merged_geometry
        else:
            return geometry

    return geometry
