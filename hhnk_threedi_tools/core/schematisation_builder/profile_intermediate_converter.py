# %%
import logging
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import geopandas as gpd
import hhnk_research_tools as hrt
import numpy as np
import pandas as pd
from shapely.geometry import LineString, MultiLineString, Point
from shapely.validation import make_valid


@dataclass
class _Data:
    """Available as .data under ProfileIntermediateConverter"""

    # DAMO tables
    # Tables are not Optional[gpd.GeoDataFrame] because it causes issues with mypy typechecking.
    hydroobject: gpd.GeoDataFrame = field(default_factory=gpd.GeoDataFrame)
    hydroobject_linemerged: gpd.GeoDataFrame = field(default_factory=gpd.GeoDataFrame)
    gw_pro: gpd.GeoDataFrame = field(default_factory=gpd.GeoDataFrame)
    gw_prw: gpd.GeoDataFrame = field(default_factory=gpd.GeoDataFrame)
    gw_pbp: gpd.GeoDataFrame = field(default_factory=gpd.GeoDataFrame)
    iws_geo_beschr_profielpunten: gpd.GeoDataFrame = field(default_factory=gpd.GeoDataFrame)

    # CSO tables
    gecombineerde_peilen: gpd.GeoDataFrame = field(default_factory=gpd.GeoDataFrame)

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


class ProfileIntermediateConverter:
    """
    Intermediate converter for profile data.
    From source (DAMO and ODS/CSO) to intermediate format, ready for converting to HyDAMO.

    Functionalities
    ---------------
    - Read and validate layers
    - Create profielgroep, profiellijn, and profielpunt
    - Linemerge primary hydroobjects on peilvakken
    - Connect primary hydroobjects without profiles to the nearest profile
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

    def __init__(self, damo_file_path: Path, ods_cso_file_path: Path, logger: logging.Logger = None):
        self.damo_file_path = Path(damo_file_path)
        self.ods_cso_file_path = Path(ods_cso_file_path)

        if logger:
            self.logger = logger
        else:
            self.logger = hrt.logging.get_logger(__name__)

        self.data = _Data()

    def load_layers(self):
        """Step 1: Load and preprocess necessary layers. All output is stored under self.gdf.xx"""
        self.logger.info("Loading layers into ProfileIntermediateConverter...")
        self.data.hydroobject = self._load_and_validate(self.damo_file_path, "hydroobject")

        self.data.gw_pro = self._load_and_validate(self.damo_file_path, "gw_pro")
        self.data.gw_prw = self._load_and_validate(self.damo_file_path, "gw_prw")
        self.data.gw_pbp = self._load_and_validate(self.damo_file_path, "gw_pbp")
        self.data.iws_geo_beschr_profielpunten = self._load_and_validate(
            self.damo_file_path, "iws_geo_beschr_profielpunten"
        )

        gecombineerde_peilen = self._load_and_validate(self.ods_cso_file_path, "gecombineerde_peilen")
        self.data.gecombineerde_peilen = gecombineerde_peilen.explode(index_parts=False).reset_index(drop=True)

    @staticmethod
    def _load_and_validate(source_path: Path, layer_name: str) -> gpd.GeoDataFrame:
        """Load a layer from the DAMO geopackage and apply make_valid to its geometries."""
        gdf = gpd.read_file(source_path, layer=layer_name)
        if "geometry" in gdf.columns:
            gdf["geometry"] = gdf["geometry"].apply(make_valid)
        return gdf

    def process_linemerge(self):
        """
        Step 2:
        Run line merge algorithm and store result.
        """
        self.logger.info("ProfileIntermediateConverter is linemerging hydroobjects for peilgebieden...")

        self.data._ensure_loaded(["hydroobject", "gecombineerde_peilen"], previous_method="load_damo_layers")

        self.data.hydroobject_linemerged = self._linemerge_hydroobjects(
            self.data.gecombineerde_peilen, self.data.hydroobject
        )

    def _linemerge_hydroobjects(self, gecombineerde_peilen, hydroobject) -> gpd.GeoDataFrame:
        """
        Merge hydroobjects within a peilgebied, assigning each hydroobject to the peilgebied
        in which the largest part of its geometry lies. Hydroobjects are not clipped.
        """
        merged_hydroobjects = []
        hydroobject_map = {}

        # Assign each hydroobject to the peilgebied where the largest part of its geometry lies
        self.logger.info("Assigning hydroobjects to peilgebieden.")
        # Build spatial index for peilgebieden for efficiency
        peilgebieden_sindex = gecombineerde_peilen.sindex

        # For each hydroobject, find intersecting peilgebieden and assign to the one with largest overlap
        hydroobject_to_peilgebied = {}
        for idx, ho_row in hydroobject.iterrows():
            ho_geom = ho_row.geometry
            possible_peil_idx = list(peilgebieden_sindex.query(ho_geom))
            if not possible_peil_idx:
                self.logger.warning(f"No peilgebied found for hydroobject with GlobalID {ho_row['GlobalID']}.")
                continue
            # Find the peilgebied with the largest intersection length
            max_length = 0
            best_peilgebied_id = None
            for peil_idx in possible_peil_idx:
                peil_row = gecombineerde_peilen.iloc[peil_idx]
                intersection = ho_geom.intersection(peil_row.geometry)
                if intersection.is_empty:
                    continue
                # Use length for lines
                overlap = intersection.length
                if overlap > max_length:
                    max_length = overlap
                    best_peilgebied_id = peil_row["PeilgebiedPraktijk_ID"]
            if best_peilgebied_id is not None:
                hydroobject_to_peilgebied[idx] = best_peilgebied_id

        # Add peilgebied assignment to hydroobject
        hydroobject = hydroobject.copy()
        hydroobject["assigned_peilgebied"] = hydroobject.index.map(hydroobject_to_peilgebied)

        # Only keep hydroobjects that have an assigned peilgebied
        hydroobject = hydroobject[hydroobject["assigned_peilgebied"].notna()]

        # Split into groups by peilgebied and category
        for peilgebied_id, group_peilgebied in hydroobject.groupby("assigned_peilgebied"):
            for category_label, group in [
                ("primary", group_peilgebied[group_peilgebied["CATEGORIEOPPWATERLICHAAM"] == 1]),
                (
                    "secondary",
                    group_peilgebied[
                        (group_peilgebied["CATEGORIEOPPWATERLICHAAM"] != 1)
                        | group_peilgebied["CATEGORIEOPPWATERLICHAAM"].isna()
                    ],
                ),
            ]:
                if category_label == "secondary":
                    continue  # skip secondary
                if group.empty:
                    continue

                merged_geom = group.geometry.union_all()

                if merged_geom.geom_type == "GeometryCollection":
                    merged_geom = self.geometrycollection_to_linestring(merged_geom)

                if merged_geom.geom_type == "LineString":
                    merged_geom = MultiLineString([merged_geom])

                if merged_geom.is_empty or merged_geom.geom_type != "MultiLineString":
                    continue

                codes = group["CODE"].unique()
                linemerge_id = str(uuid.uuid4())

                for code in codes:
                    hydroobject_map[code] = linemerge_id

                merged_hydroobjects.append(
                    {
                        "geometry": merged_geom,
                        "hydroobjectCODE": codes,
                        "linemergeID": linemerge_id,
                        "peilgebiedID": peilgebied_id,
                        "categorie": category_label,
                    }
                )

        merged_hydroobjects_gdf = gpd.GeoDataFrame(merged_hydroobjects, crs=hydroobject.crs)
        if merged_hydroobjects_gdf.empty:
            return None
        else:
            self.hydroobject_map = hydroobject_map
            return merged_hydroobjects_gdf

    @staticmethod
    def _geometrycollection_to_linestring(geometry):
        """Filter a geometrycollection to keep only LineString or MultiLineString"""
        if geometry.geom_type == "GeometryCollection":
            fixed_geometry = [geom for geom in geometry.geoms if geom.geom_type in ["LineString", "MultiLineString"]]
            if fixed_geometry:
                merged_geometry = gpd.GeoSeries(fixed_geometry).union_all()
                return merged_geometry
            else:
                return geometry

        return geometry

    def create_profile_tables(self):
        """
        Step 3: Create profielgroep, profiellijn, and profielpunt.
        Based on the layers 'gw_pro', 'gw_prw', 'gw_pbp' and 'iws_geo_beschr_profielpunten'.
        """
        self.logger.info("Creating profile tables...")
        self.data._ensure_loaded(
            layers=["gw_pro", "gw_prw", "gw_pbp", "iws_geo_beschr_profielpunten"], previous_method="load_layers"
        )

        # Create profielgroep and profiellijn, both are a copy of gw_pro
        self.data.profielgroep = self.data.gw_pro.copy()
        self.data.profielgroep["GlobalID"] = [str(uuid.uuid4()) for _ in range(len(self.data.profielgroep))]

        # Rename
        self.data.profielgroep = self.data.profielgroep.rename(columns={"PRO_ID": "code"})

        # Attach profielgroep to hydroObjectID based on the geometry
        self._assign_hydroobject_ids()

        self.data.profielgroep["geometry"] = None  # Profielgroep has no geometry

        # Drop unnecessary columns
        self.data.profielgroep = self.data.profielgroep[["GlobalID", "code", "geometry", "hydroobjectID"]]

        self.data.profiellijn = self.data.gw_pro.copy()
        self.data.profiellijn["GlobalID"] = [str(uuid.uuid4()) for _ in range(len(self.data.profiellijn))]
        self.data.profiellijn["profielgroepID"] = self.data.profielgroep["GlobalID"]

        # Rename columns in profiellijn and drop unnecessary columns
        self.data.profiellijn = self.data.profiellijn.rename(
            columns={"OPRDATOP": "datumInwinning", "OSMOMSCH": "namespace", "PRO_ID": "code"}
        )
        self.data.profiellijn = self.data.profiellijn[
            ["GlobalID", "code", "datumInwinning", "namespace", "geometry", "profielgroepID"]
        ]

        # Create profielpunt
        # Geometry is in iws_geo_beschr_profielpunten
        # Change column name PBP_PBP_ID to code
        # Create uuid for profielpunt, in profielpunt this is GlobalID
        self.data.profielpunt = self.data.iws_geo_beschr_profielpunten.copy()
        self.data.profielpunt = self.data.profielpunt.rename(columns={"PBP_PBP_ID": "code"})
        self.data.profielpunt["GlobalID"] = [str(uuid.uuid4()) for _ in range(len(self.data.profielpunt))]

        # Other information is in gw_pbp
        # We create new columns in profielpunt for the information from gw_pbp
        # We can use the PBP_ID in gw_pbp to join the two tables to code in profielpunt
        self.data.profielpunt = self.data.profielpunt.merge(
            self.data.gw_pbp[["PBP_ID", "PRW_PRW_ID", "PBPSOORT", "IWS_VOLGNR", "IWS_HOOGTE", "IWS_AFSTAND"]],
            left_on="code",
            right_on="PBP_ID",
            how="left",
        )
        self.data.profielpunt = self.data.profielpunt.rename(
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
        self.data.profielpunt = self.data.profielpunt.merge(
            self.data.gw_prw[["PRW_ID", "PRO_PRO_ID", "OSMOMSCH"]],
            left_on="PRW_PRW_ID",
            right_on="PRW_ID",
            how="left",
        )
        self.data.profielpunt = self.data.profielpunt[self.data.profielpunt["OSMOMSCH"] == "Z1"]

        # Log warning if there are still duplicates in profielpunt (based on geometry) # TODO temp validation here
        duplicates = self.data.profielpunt[self.data.profielpunt.duplicated(subset=["geometry"], keep=False)]
        if not duplicates.empty:
            self.logger.warning(
                f"Found {len(duplicates)} duplicate geometries in profielpunt. "
                # f"Codes: {duplicates['code'].unique()}"
            )

        # Now we need to add the profielLijnID to every profielpunt
        # We can use the PRO_PRO_ID in gw_prw to find the globalID in profiellijn with a matching PRO_ID (which was renamed to code)
        self.data.profielpunt = self.data.profielpunt.merge(
            self.data.profiellijn[["code", "GlobalID"]],
            left_on="PRO_PRO_ID",
            right_on="code",
            how="left",
            suffixes=("", "_profiellijn"),
        )
        self.data.profielpunt = self.data.profielpunt.rename(columns={"GlobalID_profiellijn": "profielLijnID"})
        self.data.profielpunt = self.data.profielpunt.drop(columns=["code_profiellijn"])

        # Drop columns that are not needed
        self.data.profielpunt = self.data.profielpunt.drop(columns=["PRW_ID", "PBP_ID", "PRO_PRO_ID", "PRW_PRW_ID"])

    def _assign_hydroobject_ids(self) -> None:
        if "GlobalID" not in self.data.hydroobject.columns:
            self.data.hydroobject["GlobalID"] = [str(uuid.uuid4()) for _ in range(len(self.data.hydroobject))]

        no_match_codes = []
        multiple_match_codes = []

        for idx, row in self.data.profielgroep.iterrows():
            intersecting = self.data.hydroobject[self.data.hydroobject.intersects(row.geometry)]

            gids_list = intersecting["GlobalID"].tolist()

            if not gids_list:
                no_match_codes.append(row.get("code"))
                self.data.profielgroep.loc[idx, "hydroobjectID"] = None
                continue

            self.data.profielgroep.loc[idx, "hydroobjectID"] = gids_list[0]

            if len(gids_list) > 1:
                multiple_match_codes.append((row.get("code"), len(gids_list)))
                for i, gid in enumerate(gids_list[1:], start=2):
                    self.data.profielgroep.loc[idx, f"hydroobjectID{i}"] = gid

        warnings_list = []
        if no_match_codes:
            warnings_list.append(f"No intersection for profielgroep with code(s): {no_match_codes}")
        if multiple_match_codes:
            details = ", ".join([f"{idx} ({count})" for idx, count in multiple_match_codes])
            warnings_list.append(f"Multiple intersections for profielgroep with code(s): {details}")

        if warnings_list:
            self.logger.warning("\n".join(warnings_list))

    def connect_profiles_to_hydroobject_without_profiles(self, max_distance: int = 250) -> None:
        """
        Step 4: Find hydroobjects that are not connected to profiles.
        Connect them to the nearest profile on the same hydroobject linemerge.

        Parameters
        ----------
        max_distance : int [meters]
            max search distance to connect a profile to another hydroobject.
        """
        self.logger.info("Finding hydroobjects without profiles...")
        self.data._ensure_loaded(layers=["hydroobject_linemerged"], previous_method="process_linemerge")

        # Find primary hydroobjects that are not connected to profiles
        hydroobject_ids_with_profiles = set(self.data.profielgroep["hydroobjectID"].unique())
        primary_hydroobjects = self.data.hydroobject[self.data.hydroobject["CATEGORIEOPPWATERLICHAAM"] == 1]
        hydroobject_without_profiles = primary_hydroobjects[
            ~primary_hydroobjects["GlobalID"].isin(hydroobject_ids_with_profiles)
        ].copy()

        if hydroobject_without_profiles.empty:
            self.logger.info("No primary hydroobjects without profiles found. Skipping connection step.")
            return

        self.logger.info(
            f"Found {len(hydroobject_without_profiles)} from {len(primary_hydroobjects)} primary hydroobjects without profiles."
        )

        # Mappings
        code_to_linemerge_id = self.hydroobject_map.copy()
        linemergeid_to_codes = (
            self.data.hydroobject_linemerged.explode("hydroobjectCODE")
            .groupby("linemergeID")["hydroobjectCODE"]
            .apply(list)
            .to_dict()
        )
        code_to_globalid = dict(zip(self.data.hydroobject["CODE"], self.data.hydroobject["GlobalID"]))

        # Build spatial index for profiellijn, grouped by linemergeID
        no_profiellijn_for_linemerge = []
        profiellijn_by_linemerge = {}
        for linemerge_id, codes in linemergeid_to_codes.items():
            gids = [code_to_globalid.get(code) for code in codes if code in code_to_globalid]
            profielgroep_ids = self.data.profielgroep[self.data.profielgroep["hydroobjectID"].isin(gids)][
                "GlobalID"
            ].tolist()
            profiellijn = self.data.profiellijn[
                self.data.profiellijn["profielgroepID"].isin(profielgroep_ids)
            ].reset_index(drop=True)
            if not profiellijn.empty:
                profiellijn_by_linemerge[linemerge_id] = (profiellijn, profiellijn.sindex)
            else:
                no_profiellijn_for_linemerge.append(linemerge_id)

        if no_profiellijn_for_linemerge:
            self.logger.warning(
                f"Found {len(no_profiellijn_for_linemerge)} primairy linemerges without profiellijn. "
                f"This means that these hydroobjects cannot be connected to profiles. "
                f"These linemerges are: {no_profiellijn_for_linemerge}"
            )

        # Prepare list for concat
        profielgroep_new = []

        for _, row in hydroobject_without_profiles.iterrows():
            linemerge_id = code_to_linemerge_id.get(row["CODE"])
            if not linemerge_id or linemerge_id not in profiellijn_by_linemerge:
                continue

            profiellijn, sindex = profiellijn_by_linemerge[linemerge_id]
            if profiellijn.empty:
                continue

            # Find nearest profile line
            # 1. Buffer geometry to get nearby candidates
            buffered_geom = row.geometry.buffer(max_distance)

            # 2. Use sindex to find intersecting geometries
            possible_idx = list(sindex.query(buffered_geom))
            nearest_idx = None
            if not possible_idx:
                # fallback: brute force if no candidates found
                profiellijn["distance"] = profiellijn.geometry.distance(row.geometry)
                min_distance = profiellijn["distance"].min()
                if min_distance < max_distance:
                    nearest_idx = profiellijn["distance"].idxmin()
            else:
                candidates = profiellijn.iloc[possible_idx].copy()
                candidates["distance"] = candidates.geometry.distance(row.geometry)
                min_distance = candidates["distance"].min()
                if min_distance < max_distance:
                    nearest_idx = candidates["distance"].idxmin()

            if nearest_idx is None:
                continue

            # 3. Select nearest profile line
            nearest_profile_line = profiellijn.loc[nearest_idx]

            profielgroep_id = nearest_profile_line["profielgroepID"]

            # Copy rows
            profielgroep_copy = self.data.profielgroep[self.data.profielgroep["GlobalID"] == profielgroep_id].copy()
            profielgroep_copy["GlobalID"] = str(uuid.uuid4())  # New GlobalID for the copy
            profielgroep_copy["hydroobjectID"] = row["GlobalID"]
            profielgroep_copy["copyOf"] = profielgroep_id

            profielgroep_new.append(profielgroep_copy)

        # Concat
        if profielgroep_new:
            self.data.profielgroep = pd.concat([self.data.profielgroep] + profielgroep_new, ignore_index=True)

        self.logger.info("Connected hydroobjects to profiles.")

    def compute_deepest_point_hydroobjects(self) -> None:
        """Step 5: Compute the deepest point and add as a new column to self.hydroobject."""
        self.logger.info("Computing the deepest point for hydroobjects...")

        self.data._ensure_loaded(layers=["hydroobject"], previous_method="load_layers")
        self.data._ensure_loaded(layers=["profielgroep"], previous_method="create_profile_tables")

        # filter the hydroobject on reference in profielgroep
        # these are the hydroobjects that have a profile
        hydroobject_with_profiles = self.data.hydroobject[
            self.data.hydroobject["GlobalID"].isin(self.data.profielgroep["hydroobjectID"])
        ]
        if hydroobject_with_profiles.empty:
            self.logger.warning("No hydroobjects with profiles found. Skipping deepest point computation.")
            return

        # Check if "diepstePunt" is already computed
        if "diepstePunt" not in self.data.profiellijn.columns:
            self._compute_deepest_point_profiellijn()

        # collect the profiellijn features per hydroobject
        hydroobject_deepest_points = []
        for _, hydro_row in hydroobject_with_profiles.iterrows():
            hydro_code = hydro_row["CODE"]

            # Find the corresponding profiellijn
            profiellijn = self._find_profiellijn_by_hydroobject_code(hydro_code)
            if profiellijn is None or profiellijn.empty:
                continue

            # Find the deepest point in the profiellijn
            diepste_punt = profiellijn["diepstePunt"].min()
            if pd.isna(diepste_punt):
                continue

            # Append to results
            hydroobject_deepest_points.append(
                {"GlobalID": hydro_row["GlobalID"], "diepstePunt": round(diepste_punt, 2)}
            )

        # Create a DataFrame from the results
        hydroobject_deepest_points_df = pd.DataFrame(hydroobject_deepest_points)
        if hydroobject_deepest_points_df.empty:
            self.logger.warning("No deepest points computed for hydroobjects.")
            return

        # Merge the deepest points back to the hydroobject GeoDataFrame
        self.data.hydroobject = self.data.hydroobject.merge(hydroobject_deepest_points_df, on="GlobalID", how="left")

        # log the number of hydroobjects with computed deepest points
        num_deepest_points = self.data.hydroobject["diepstePunt"].notna().sum()
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
        deepest_points = self.data.profielpunt.groupby("profielLijnID")["hoogte"].min()

        # Map the deepest point to the profiellijn using its GlobalID
        self.data.profiellijn["diepstePunt"] = self.data.profiellijn["GlobalID"].map(deepest_points)

    def _find_profiellijn_by_hydroobject_code(self, code: str) -> Optional[gpd.GeoDataFrame]:
        """
        Find the profiellijn GeoDataFrame by hydroobject code.

        Called by: compute_deepest_point_hydroobjects
        """
        self.data._ensure_loaded(layers=["profiellijn"], previous_method="create_profile_tables")

        # Find the hydroobjectID for the given code
        hydroobject_id = self.data.hydroobject[self.data.hydroobject["CODE"] == code]["GlobalID"].values
        if not hydroobject_id:
            self.logger.warning(f"No hydroobject found with code '{code}'.")
            return None

        # Filter profielgroep by hydroobjectID
        profielgroep = self.data.profielgroep[self.data.profielgroep["hydroobjectID"] == hydroobject_id[0]]
        if profielgroep.empty:
            self.logger.warning(f"No profielgroep found for hydroobject code '{code}'.")
            return None

        # Get the profielgroep_ids, use 'copyOf' if present, otherwise use 'GlobalID'
        profielgroep_ids = profielgroep.apply(
            lambda row: row["copyOf"] if "copyOf" in row and pd.notna(row["copyOf"]) else row["GlobalID"], axis=1
        ).tolist()

        # Get the profiellijn that matches the profielgroep_ids
        profiellijn = self.data.profiellijn[self.data.profiellijn["profielgroepID"].isin(profielgroep_ids)]

        if profiellijn.empty:
            self.logger.warning(f"No profiellijn found for hydroobject code '{code}'.")
            return None

        return profiellijn

    def compute_distance_and_depth_wet_profile(self) -> None:
        """
        Step 6:
        Computes the distance for the wet profile.
        It is the distance between the profielpunt features with attribute "typeProfielPunt" = 22
        We do not actually compute the topological distance between points,
        but we use the distance in attribute "afstand" to compute difference.

        Also collects all profielpunt features between the features with typeProfielPunt = 22
        and calculates the difference between the max and min value from "hoogte" column.
        This is the depth of the wet profile.
        """
        self.logger.info("Computing distance for wet profile...")

        self.data._ensure_loaded(layers=["profielpunt"], previous_method="create_profile_tables")
        if "afstand" not in self.data.profielpunt.columns:
            raise ValueError("afstand column is not present in profielpunt.")
        if "typeProfielPunt" not in self.data.profielpunt.columns:
            raise ValueError("typeProfielPunt column is not present in profielpunt.")

        # we need to group the profielpunt by profielLijnID
        # for each group we compute the distance as abs difference between the first and last point (with typeProfielPunt = 22)
        wet_profile_distances = (
            self.data.profielpunt[self.data.profielpunt["typeProfielPunt"] == 22]
            .groupby("profielLijnID")["afstand"]
            .agg(lambda x: round(abs(x.iloc[-1] - x.iloc[0]), 2) if len(x) > 1 else np.nan)
            .reset_index()
        )

        # Rename the column to "afstandNatProfiel"
        wet_profile_distances = wet_profile_distances.rename(columns={"afstand": "afstandNatProfiel"})
        # Merge the distances back to the profiellijn GeoDataFrame (profiellijn globalID is profielLijnID)
        self.data.profiellijn = self.data.profiellijn.merge(
            wet_profile_distances, left_on="GlobalID", right_on="profielLijnID", how="left"
        )
        # Drop the profielLijnID column as it is no longer needed
        self.data.profiellijn = self.data.profiellijn.drop(columns=["profielLijnID"])

        # log the number of profielLijnID with computed wet profile distances
        num_wet_profile_distances = self.data.profiellijn["afstandNatProfiel"].notna().sum()
        self.logger.info(f"Succesfully computed wet profile distances for {num_wet_profile_distances} profielLijns")
        if len(self.data.profiellijn) - num_wet_profile_distances > 0:
            self.logger.warning(
                f"Failed to compute wet profile distances for {len(self.data.profiellijn) - num_wet_profile_distances} profielLijns"
            )

        # Compute the depth of the wet profile
        self.logger.info("Computing depth for wet profile...")

        # Compute the depth of the wet profile between the profielpunt features with typeProfielPunt == 22
        wet_profile_depths = []
        for pl_id, group in self.data.profielpunt.groupby("profielLijnID"):
            wet_points = group[group["typeProfielPunt"] == 22]
            if len(wet_points) > 1:
                min_afstand = wet_points["afstand"].min()
                max_afstand = wet_points["afstand"].max()
                # Select points between min_afstand and max_afstand (inclusive)
                in_range = group[(group["afstand"] >= min_afstand) & (group["afstand"] <= max_afstand)]
                if not in_range.empty:
                    min_hoogte = in_range["hoogte"].min()
                    max_hoogte = in_range["hoogte"].max()
                    wet_profile_depths.append(
                        {"profielLijnID": pl_id, "diepteNatProfiel": round(max_hoogte - min_hoogte, 2)}
                    )
                else:
                    wet_profile_depths.append({"profielLijnID": pl_id, "diepteNatProfiel": np.nan})
            else:
                wet_profile_depths.append({"profielLijnID": pl_id, "diepteNatProfiel": np.nan})

        # Merge the computed depths back to the profiellijn GeoDataFrame
        wet_profile_depths_df = pd.DataFrame(wet_profile_depths)
        self.data.profiellijn = self.data.profiellijn.merge(
            wet_profile_depths_df, left_on="GlobalID", right_on="profielLijnID", how="left"
        )
        self.data.profiellijn = self.data.profiellijn.drop(columns=["profielLijnID"])

        num_wet_profile_depths = self.data.profiellijn["diepteNatProfiel"].notna().sum()
        self.logger.info(f"Succesfully computed wet profile depths for {num_wet_profile_depths} profielLijns")
        if len(self.data.profiellijn) - num_wet_profile_depths > 0:
            self.logger.warning(
                f"Failed to compute wet profile depths for {len(self.data.profiellijn) - num_wet_profile_depths} profielLijns"
            )

    def add_z_to_point_geometry_based_on_column(self, column_name: str) -> None:
        """
        Step 7:
        Add Z coordinate to the geometry of profielpunt based on a specified column.
        The column should contain height values that will be used as Z coordinates.
        """
        self.logger.info(f"Adding Z coordinate to profielpunt geometry based on column '{column_name}'...")
        self.data._ensure_loaded(layers=["profielpunt"], previous_method="create_profile_tables")
        if column_name not in self.data.profielpunt.columns:
            raise ValueError(f"Column '{column_name}' is not present in profielpunt.")

        # Ensure the column contains numeric values
        self.data.profielpunt[column_name] = pd.to_numeric(self.data.profielpunt[column_name], errors="coerce")

        # Add Z coordinate to the geometry
        self.data.profielpunt["geometry"] = self.data.profielpunt.apply(
            lambda row: Point(row.geometry.x, row.geometry.y, row[column_name])
            if pd.notna(row[column_name])
            else row.geometry,
            axis=1,
        )

        self.logger.info("Z coordinate added to profielpunt geometry.")

    def add_breedte_value_from_hydroobject(self):
        """
        Step 8:
        Add the 'BREEDTE' value from hydroobject to profiellijn.
        The profielgroep is connected to the hydroobject by hydroobjectID.
        The profiellijn is connected to the profielgroep by profielgroepID.
        """
        self.logger.info("Adding 'BREEDTE' value from hydroobject to profiellijn...")
        self.data._ensure_loaded(layers=["profiellijn", "hydroobject"], previous_method="load_layers")

        # Ensure 'BREEDTE' column exists in hydroobject
        if "BREEDTE" not in self.data.hydroobject.columns:
            raise ValueError("Column 'BREEDTE' is not present in hydroobject.")

        # Merge profiellijn with profielgroep to get hydroobjectID
        profiellijn_with_pg = self.data.profiellijn.merge(
            self.data.profielgroep[["GlobalID", "hydroobjectID"]],
            left_on="profielgroepID",
            right_on="GlobalID",
            how="left",
            suffixes=("", "_profielgroep"),
        )
        # Merge with hydroobject to get BREEDTE
        profiellijn_with_breedte = profiellijn_with_pg.merge(
            self.data.hydroobject[["GlobalID", "BREEDTE"]],
            left_on="hydroobjectID",
            right_on="GlobalID",
            how="left",
            suffixes=("", "_hydroobject"),
        )
        # Assign the BREEDTE column to profiellijn as hydroobject_breedte
        self.data.profiellijn["hydroobject_breedte"] = profiellijn_with_breedte["BREEDTE"]

    def compute_jaarinwinning(self):
        """
        Step 9: Compute the year of inwinning of the profiellijn based on the datumInwinning
        and add as a new column to self.profiellijn.
        """
        self.logger.info("Computing the year of inwinning for profiellijn...")
        self.data._ensure_loaded(layers=["profiellijn"], previous_method="create_profile_tables")
        if "datumInwinning" not in self.data.profiellijn.columns:
            raise ValueError("datumInwinning column is not present in profiellijn.")

        # Convert datumInwinning to datetime
        self.data.profiellijn["datumInwinning"] = pd.to_datetime(
            self.data.profiellijn["datumInwinning"], errors="coerce"
        )

        # Extract the year from datumInwinning
        self.data.profiellijn["jaarinwinning"] = self.data.profiellijn["datumInwinning"].dt.year

        self.logger.info("Year of inwinning computed and added to profiellijn.")

    def compute_number_of_profielpunt_features_per_profiellijn(self):
        """
        # TODO WVG: wat moet hiermee? moet ie wel draaien?
        Compute the number of profielpunt features per profiellijn and add as a new column to self.profiellijn.
        NOTE: This function is also implement in the validation module
        """
        self.logger.info("Computing the number of profielpunt features per profiellijn...")
        self.data._ensure_loaded(layers=["profielpunt", "profiellijn"], previous_method="create_profile_tables")
        if "profielLijnID" not in self.data.profielpunt.columns:
            raise ValueError("profielLijnID column is not present in profielpunt.")

        # Count the number of profielpunt features per profiellijn
        count_per_profiellijn = (
            self.data.profielpunt.groupby("profielLijnID").size().reset_index(name="aantalProfielPunten")
        )

        # Map the count to the profiellijn using its GlobalID
        self.data.profiellijn["aantalProfielPunten"] = self.data.profiellijn["GlobalID"].map(
            count_per_profiellijn.set_index("profielLijnID")["aantalProfielPunten"]
        )

        # If NaN values are present, set them to 0
        self.data.profiellijn["aantalProfielPunten"] = (
            self.data.profiellijn["aantalProfielPunten"].fillna(0).astype(int)
        )

        self.logger.info(
            f"Computed the number of profielpunt features for {self.data.profiellijn['aantalProfielPunten'].notna().sum()} profiellijns."
        )

    def add_maxcross_to_profiellijn(self):
        """
        Step 10:
        Add the maximum cross product of the segments of the LineString to the profiellijn.
        This is used to check if the LineString is straight (enough).
        """
        self.logger.info("Adding maximum cross product to profiellijn...")
        self.data._ensure_loaded(layers=["profiellijn"], previous_method="create_profile_tables")

        # Ensure geometry is a LineString
        if not all(
            self.data.profiellijn.geometry.apply(lambda geom: geom.geom_type in ["LineString", "MultiLineString"])
        ):
            raise ValueError("All geometries in profiellijn must be LineString or MultiLineString.")

        self.logger.info(f"{self.data.profiellijn.geometry}")

        # Apply the function to compute max cross product
        self.data.profiellijn["max_cross_product"] = self.data.profiellijn["geometry"].apply(
            self._is_linestring_straight
        )

        self.logger.info("Maximum cross product added to profiellijn.")

    def _is_linestring_straight(self, line: LineString) -> float:
        """
        Check if a LineString is straight by checking the collinearity of each segment.
        A LineString is considered straight if all segments are collinear, but we allow a small tolerance.
        This tolerance is determined in the validation module (validationrules.json).
        In this function, the maximum cross product of the segments is computed.
        """

        coords = list(line.coords)
        if len(coords) < 3:
            return True  # Two points always form a straight line

        # Use the first segment as the reference vector
        x0, y0 = coords[0]
        x1, y1 = coords[1]
        dx_ref = x1 - x0
        dy_ref = y1 - y0

        max_cross = 0
        for i in range(1, len(coords) - 1):
            x1, y1 = coords[i]
            x2, y2 = coords[i + 1]
            dx = x2 - x1
            dy = y2 - y1
            # Use cross product to check colinearity (should be zero if vectors are colinear)
            cross = dx_ref * dy - dy_ref * dx

            if abs(cross) > max_cross:
                max_cross = abs(cross)

        return max_cross

    def compute_if_ascending(self):
        """
        Step 11:
        Compute if the profielpunt features are in ascending order based on the 'hoogte' column.
        We determine the point with the lowest height
        And check if ascending both to the left and right of this point, based on the 'afstand' column.
        This is used to check if the profile is ascending.
        """
        self.logger.info("Computing if profielpunt features are in ascending order...")
        self.data._ensure_loaded(layers=["profielpunt"], previous_method="create_profile_tables")
        if "hoogte" not in self.data.profielpunt.columns:
            raise ValueError("hoogte column is not present in profielpunt.")
        if "afstand" not in self.data.profielpunt.columns:
            raise ValueError("afstand column is not present in profielpunt.")

        # Ensure the hoogte column is numeric
        self.data.profielpunt["hoogte"] = pd.to_numeric(self.data.profielpunt["hoogte"], errors="coerce")

        ascending_per_lijn = self.data.profielpunt.groupby("profielLijnID").apply(self._is_ascending)
        ascending_per_lijn.name = "isAscending"

        # Convert to DataFrame for merge
        ascending_df = ascending_per_lijn.reset_index()

        # Merge on GlobalID (in profiellijn) = profielLijnID (in profielpunt)
        self.data.profiellijn = self.data.profiellijn.merge(
            ascending_df, how="left", left_on="GlobalID", right_on="profielLijnID"
        )

    # General functions
    @staticmethod
    def _is_ascending(group: pd.DataFrame) -> int:
        """
        Check if the 'hoogte' values in a group form a V-shape based on 'afstand' order.

        The function determines whether the 'hoogte' (height) values first decrease (to a minimum)
        and then increase, forming a V-shape when sorted by 'afstand' (distance).

        Parameters
        ----------
        group : pd.DataFrame
            DataFrame with at least two columns: 'afstand' and 'hoogte'.

        Returns
        -------
        int
            1: if the group is V-shaped (descending to minimum then ascending),
            0: otherwise
        Returns False if the group is empty.
        """
        if group.empty:
            return False
        group = group.sort_values(by="afstand").reset_index(drop=True)

        min_index = group["hoogte"].idxmin()
        left = group.loc[:min_index, "hoogte"]
        right = group.loc[min_index:, "hoogte"]

        left = left.reset_index(drop=True)
        right = right.reset_index(drop=True)

        left_descending = all(left.iloc[i] >= left.iloc[i + 1] for i in range(len(left) - 1))
        right_ascending = all(right.iloc[i] <= right.iloc[i + 1] for i in range(len(right) - 1))

        if left_descending and right_ascending:
            return 1
        else:
            return 0

    def write_outputs(self, output_path: Path):
        """
        Step 12:
        Write all GeoDataFrame attributes of this class to a GeoPackage or individual files.
        """
        output_path = Path(output_path)
        geodf_attrs = {name: val for name, val in self.data.__dict__.items() if isinstance(val, gpd.GeoDataFrame)}

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

    def find_linemerge_id_by_hydroobject_code(self, code: str) -> str:
        """
        Utility/debug function and not used in the main class workflow.

        Find the linemergeID by hydroobject code.
        """
        if code in self.hydroobject_map:
            return self.hydroobject_map[code]
        else:
            self.logger.warning(f"No linemerge found for hydroobject with code {code}.")
            return None

    def find_peilgebied_id_by_hydroobject_code(self, code: str):
        """
        Utility/debug function and not used in the main class workflow.

        Find the peilgebiedID by hydroobject code.
        """

        self.data._ensure_loaded(layers=["hydroobject_linemerged"], previous_method="process_linemerge")

        # First, find the linemergeID for this hydroobject code
        if code in self.hydroobject_map:
            linemerge_id = self.hydroobject_map[code]

            # Now, lookup the peilgebiedID in the linemerged GeoDataFrame
            match = self.data.hydroobject_linemerged[self.data.hydroobject_linemerged["linemergeID"] == linemerge_id]

            if match.empty:
                self.logger.warning(f"No matching linemerge found for hydroobject code '{code}'.")
                return None
            else:
                peilgebied_id = match.iloc[0]["peilgebiedID"]
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

        if "diepstePunt" not in self.data.hydroobject.columns:
            raise ValueError(
                "diepstePunt column is not present in hydroobject. Call compute_deepest_point_hydroobjects() first."
            )

        # Find the hydroobject by code
        hydroobject = self.data.hydroobject[self.data.hydroobject["CODE"] == code]
        if hydroobject.empty:
            self.logger.warning(f"No hydroobject found with code '{code}'.")
            return None

        # Get the deepest point
        diepste_punt = hydroobject["diepstePunt"].values[0]
        return diepste_punt
