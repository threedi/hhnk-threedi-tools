import uuid
from pathlib import Path
from typing import Optional

import geopandas as gpd
import hhnk_research_tools as hrt
import numpy as np
import pandas as pd
from shapely.geometry import MultiLineString, Point
from shapely.validation import make_valid


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
        """
        Merges hydroobjects within a peilgebied, assigning each hydroobject to the peilgebied
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
                    merged_geom = geometrycollection_to_linestring(merged_geom)

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

    def find_profiellijn_by_hydroobject_code(self, code: str):
        """
        Find the profiellijn GeoDataFrame by hydroobject code.
        """
        if self.profiellijn is None:
            raise ValueError("profiellijn is not loaded. Call create_profile_tables() first.")

        # Find the hydroobjectID for the given code
        hydroobject_id = self.hydroobject[self.hydroobject["CODE"] == code]["GlobalID"].values
        if not hydroobject_id:
            self.logger.warning(f"No hydroobject found with code '{code}'.")
            return None

        # Filter profielgroep by hydroobjectID
        profielgroep = self.profielgroep[self.profielgroep["hydroobjectID"] == hydroobject_id[0]]
        if profielgroep.empty:
            self.logger.warning(f"No profielgroep found for hydroobject code '{code}'.")
            return None

        # Get the profielgroep_ids, use 'copyOf' if present, otherwise use 'GlobalID'
        profielgroep_ids = profielgroep.apply(
            lambda row: row["copyOf"] if "copyOf" in row and pd.notna(row["copyOf"]) else row["GlobalID"], axis=1
        ).tolist()

        # Get the profiellijn that matches the profielgroep_ids
        profiellijn = self.profiellijn[self.profiellijn["profielgroepID"].isin(profielgroep_ids)]

        if profiellijn.empty:
            self.logger.warning(f"No profiellijn found for hydroobject code '{code}'.")
            return None

        return profiellijn

    def find_deepest_point_by_hydroobject_code(self, code: str):
        """
        Find the deepest point for a hydroobject by its code.
        """
        if self.hydroobject is None:
            raise ValueError("hydroobject is not loaded. Call load_layers() first.")
        if self.profielgroep is None:
            raise ValueError("profielgroep is not loaded. Call create_profile_tables() first.")
        if "diepstePunt" not in self.hydroobject.columns:
            raise ValueError(
                "diepstePunt column is not present in hydroobject. Call compute_deepest_point_hydroobjects() first."
            )

        # Find the hydroobject by code
        hydroobject = self.hydroobject[self.hydroobject["CODE"] == code]
        if hydroobject.empty:
            self.logger.warning(f"No hydroobject found with code '{code}'.")
            return None

        # Get the deepest point
        diepste_punt = hydroobject["diepstePunt"].values[0]
        return diepste_punt

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

    def compute_deepest_point_profiellijn(self):
        """
        Compute the deepest point per profiellijn and add as a new column to self.profiellijn.
        """
        self.logger.info("Computing the deepest point per profiellijn...")
        if self.profielpunt is None:
            raise ValueError("profielpunt is not loaded. Call create_profile_tables() first.")
        if "hoogte" not in self.profielpunt.columns:
            raise ValueError("hoogte column is not present in profielpunt.")
        if "profielLijnID" not in self.profielpunt.columns:
            raise ValueError("profielLijnID column is not present in profielpunt.")
        if self.profiellijn is None:
            raise ValueError("profiellijn is not loaded. Call create_profile_tables() first.")

        # Convert height to float
        self.profielpunt["hoogte"] = pd.to_numeric(self.profielpunt["hoogte"], errors="coerce")

        # Find the deepest point per profiellijn
        deepest_points = self.profielpunt.groupby("profielLijnID")["hoogte"].min()

        # Map the deepest point to the profiellijn using its GlobalID
        self.profiellijn["diepstePunt"] = self.profiellijn["GlobalID"].map(deepest_points)

    def connect_profiles_to_hydroobject_without_profiles(self, max_distance=250):
        """
        Find hydroobjects that are not connected to profiles.
        Connect them to the nearest profile on the same hydroobject linemerge.
        """
        self.logger.info("Finding hydroobjects without profiles...")
        if self.hydroobject_linemerged is None:
            raise ValueError("Linemerged hydroobjects not yet processed. Call process_linemerge() first.")

        # Find primary hydroobjects that are not connected to profiles
        hydroobject_ids_with_profiles = set(self.profielgroep["hydroobjectID"].unique())
        primary_hydroobjects = self.hydroobject[self.hydroobject["CATEGORIEOPPWATERLICHAAM"] == 1]
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
        code_to_linemerge_id = {code: lm_id for code, lm_id in self.hydroobject_map.items()}
        linemergeid_to_codes = (
            self.hydroobject_linemerged.explode("hydroobjectCODE")
            .groupby("linemergeID")["hydroobjectCODE"]
            .apply(list)
            .to_dict()
        )
        code_to_globalid = dict(zip(self.hydroobject["CODE"], self.hydroobject["GlobalID"]))

        # Build spatial index for profiellijn, grouped by linemergeID
        no_profiellijn_for_linemerge = []
        profiellijn_by_linemerge = {}
        for linemerge_id, codes in linemergeid_to_codes.items():
            gids = [code_to_globalid.get(code) for code in codes if code in code_to_globalid]
            profielgroep_ids = self.profielgroep[self.profielgroep["hydroobjectID"].isin(gids)]["GlobalID"].tolist()
            profiellijn = self.profiellijn[self.profiellijn["profielgroepID"].isin(profielgroep_ids)].reset_index(
                drop=True
            )
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
            profielgroep_copy = self.profielgroep[self.profielgroep["GlobalID"] == profielgroep_id].copy()
            profielgroep_copy["GlobalID"] = str(uuid.uuid4())  # New GlobalID for the copy
            profielgroep_copy["hydroobjectID"] = row["GlobalID"]
            profielgroep_copy["copyOf"] = profielgroep_id

            profielgroep_new.append(profielgroep_copy)

        # Concat
        if profielgroep_new:
            self.profielgroep = pd.concat([self.profielgroep] + profielgroep_new, ignore_index=True)

        self.logger.info("Connected hydroobjects to profiles.")

    def compute_deepest_point_hydroobjects(self):
        """
        Compute the deepest point and add as a new column to self.hydroobject.
        """
        self.logger.info("Computing the deepest point for hydroobjects...")

        if self.hydroobject is None:
            raise ValueError("hydroobject is not loaded. Call load_layers() first.")
        if self.profielgroep is None:
            raise ValueError("profielgroep is not loaded. Call create_profile_tables() first.")
        if "hydroobjectID" not in self.profielgroep.columns:
            raise ValueError("hydroobjectID column is not present in profielgroep.")

        # filter the hydroobject on reference in profielgroep
        # these are the hydroobjects that have a profile
        hydroobject_with_profiles = self.hydroobject[
            self.hydroobject["GlobalID"].isin(self.profielgroep["hydroobjectID"])
        ]
        if hydroobject_with_profiles.empty:
            self.logger.warning("No hydroobjects with profiles found. Skipping deepest point computation.")
            return

        # Check if "diepstePunt" is already computed
        if "diepstePunt" not in self.profiellijn.columns:
            self.compute_deepest_point_profiellijn()

        # collect the profiellijn features per hydroobject
        hydroobject_deepest_points = []
        for _, hydro_row in hydroobject_with_profiles.iterrows():
            hydro_code = hydro_row["CODE"]

            # Find the corresponding profiellijn
            profiellijn = self.find_profiellijn_by_hydroobject_code(hydro_code)
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
        self.hydroobject = self.hydroobject.merge(hydroobject_deepest_points_df, on="GlobalID", how="left")

        # log the number of hydroobjects with computed deepest points
        num_deepest_points = self.hydroobject["diepstePunt"].notnull().sum()
        self.logger.info(f"Computed deepest points for {num_deepest_points} hydroobjects.")

    def compute_distance_wet_profile(self):
        """
        Compute the distance for the wet profile
        It is the distance between the profielpunt features with attribute "typeProfielPunt" = 22
        We do not actually compute distance between points,
        but use the distance in attribute "afstand" to compute difference
        """
        self.logger.info("Computing distance for wet profile...")
        if self.profielpunt is None:
            raise ValueError("profielpunt is not loaded. Call create_profile_tables() first.")
        if "afstand" not in self.profielpunt.columns:
            raise ValueError("afstand column is not present in profielpunt.")
        if "typeProfielPunt" not in self.profielpunt.columns:
            raise ValueError("typeProfielPunt column is not present in profielpunt.")

        # we need to group the profielpunt by profielLijnID
        # for each group we compute the distance as abs difference between the first and last point (with typeProfielPunt = 22)
        wet_profile_distances = (
            self.profielpunt[self.profielpunt["typeProfielPunt"] == 22]
            .groupby("profielLijnID")["afstand"]
            .agg(lambda x: round(abs(x.iloc[-1] - x.iloc[0]), 2) if len(x) > 1 else np.nan)
            .reset_index()
        )

        # Rename the column to "afstandNatProfiel"
        wet_profile_distances = wet_profile_distances.rename(columns={"afstand": "afstandNatProfiel"})
        # Merge the distances back to the profiellijn GeoDataFrame (profiellijn globalID is profielLijnID)
        self.profiellijn = self.profiellijn.merge(
            wet_profile_distances, left_on="GlobalID", right_on="profielLijnID", how="left"
        )
        # Drop the profielLijnID column as it is no longer needed
        self.profiellijn = self.profiellijn.drop(columns=["profielLijnID"])

        # log the number of profielLijnID with computed wet profile distances
        num_wet_profile_distances = self.profiellijn["afstandNatProfiel"].notnull().sum()
        self.logger.info(f"Succesfully computed wet profile distances for {num_wet_profile_distances} profielLijns")
        self.logger.warning(
            f"Failed to compute wet profile distances for {len(self.profiellijn) - num_wet_profile_distances} profielLijns"
        )

    def compute_number_of_profielpunt_features_per_profiellijn(self):
        """
        Compute the number of profielpunt features per profiellijn and add as a new column to self.profiellijn.
        NOTE: This function is also implement in the validation module
        """
        self.logger.info("Computing the number of profielpunt features per profiellijn...")
        if self.profielpunt is None:
            raise ValueError("profielpunt is not loaded. Call create_profile_tables() first.")
        if "profielLijnID" not in self.profielpunt.columns:
            raise ValueError("profielLijnID column is not present in profielpunt.")
        if self.profiellijn is None:
            raise ValueError("profiellijn is not loaded. Call create_profile_tables() first.")

        # Count the number of profielpunt features per profiellijn
        count_per_profiellijn = (
            self.profielpunt.groupby("profielLijnID").size().reset_index(name="aantalProfielPunten")
        )

        # Map the count to the profiellijn using its GlobalID
        self.profiellijn["aantalProfielPunten"] = self.profiellijn["GlobalID"].map(
            count_per_profiellijn.set_index("profielLijnID")["aantalProfielPunten"]
        )

        # If NaN values are present, set them to 0
        self.profiellijn["aantalProfielPunten"] = self.profiellijn["aantalProfielPunten"].fillna(0).astype(int)

        self.logger.info(
            f"Computed the number of profielpunt features for {self.profiellijn['aantalProfielPunten'].notnull().sum()} profiellijns."
        )

    def add_z_to_point_geometry_based_on_column(self, column_name: str):
        """
        Add Z coordinate to the geometry of profielpunt based on a specified column.
        The column should contain height values that will be used as Z coordinates.
        """
        self.logger.info(f"Adding Z coordinate to profielpunt geometry based on column '{column_name}'...")
        if self.profielpunt is None:
            raise ValueError("profielpunt is not loaded. Call create_profile_tables() first.")
        if column_name not in self.profielpunt.columns:
            raise ValueError(f"Column '{column_name}' is not present in profielpunt.")

        # Ensure the column contains numeric values
        self.profielpunt[column_name] = pd.to_numeric(self.profielpunt[column_name], errors="coerce")

        # Add Z coordinate to the geometry
        self.profielpunt["geometry"] = self.profielpunt.apply(
            lambda row: Point(row.geometry.x, row.geometry.y, row[column_name])
            if pd.notna(row[column_name])
            else row.geometry,
            axis=1,
        )

        self.logger.info("Z coordinate added to profielpunt geometry.")


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
