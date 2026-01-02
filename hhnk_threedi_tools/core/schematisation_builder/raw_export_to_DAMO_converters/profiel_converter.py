import uuid
from typing import Optional

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import LineString, MultiLineString, Point

from hhnk_threedi_tools.core.schematisation_builder.raw_export_to_DAMO_converter import RawExportToDAMOConverter


class ProfielConverter(RawExportToDAMOConverter):
    """Profile-specific converter implementation."""

    def __init__(self, raw_export_converter: RawExportToDAMOConverter):
        self.data = raw_export_converter.data
        self.logger = raw_export_converter.logger

    def run(self):
        self.logger.info("Starting ProfielConverter...")
        self.process_linemerge()
        self.create_profile_tables()
        self.connect_profiles_to_hydroobject_without_profiles()
        self.compute_deepest_point_hydroobjects()

    def process_linemerge(self):
        self.logger.info("ProfielConverter is linemerging hydroobjects for peilgebieden...")
        self.data._ensure_loaded(["hydroobject", "peilgebiedpraktijk"], previous_method="load_damo_layers")
        self.data.hydroobject_linemerged = self._linemerge_hydroobjects(
            self.data.peilgebiedpraktijk, self.data.hydroobject
        )

    def create_profile_tables(self):
        """
        Create profielgroep, profiellijn, and profielpunt.
        Based on the layers 'gw_pro', 'gw_prw', 'gw_pbp' and 'iws_geo_beschr_profielpunten'.
        """
        self.logger.info("Creating profile tables...")
        self.data._ensure_loaded(
            layers=["gw_pro", "gw_prw", "gw_pbp", "iws_geo_beschr_profielpunten"], previous_method="load_layers"
        )

        # Create profielgroep and profiellijn, both are a copy of gw_pro
        self.data.profielgroep = self.data.gw_pro.copy()
        # only add if needed # TODO
        self.data.profielgroep["globalid"] = [str(uuid.uuid4()) for _ in range(len(self.data.profielgroep))]

        # Rename
        self.data.profielgroep = self.data.profielgroep.rename(columns={"pro_id": "code"})

        # Attach profielgroep to hydroobjectid based on the geometry
        no_match_codes = self._assign_hydroobject_ids()

        self.data.profielgroep["geometry"] = None  # Profielgroep has no geometry

        # Drop unnecessary columns
        self.data.profielgroep = self.data.profielgroep[["globalid", "code", "geometry", "hydroobjectid"]]

        self.data.profiellijn = self.data.gw_pro.copy()
        self.data.profiellijn["globalid"] = [str(uuid.uuid4()) for _ in range(len(self.data.profiellijn))]
        self.data.profiellijn["profielgroepid"] = self.data.profielgroep["globalid"]

        # Rename columns in profiellijn and drop unnecessary columns
        self.data.profiellijn = self.data.profiellijn.rename(
            columns={"oprdatop": "datuminwinning", "osmomsch": "namespace", "pro_id": "code"}
        )
        self.data.profiellijn = self.data.profiellijn[
            ["globalid", "code", "datuminwinning", "namespace", "geometry", "profielgroepid"]
        ]

        # Create profielpunt
        # Geometry is in iws_geo_beschr_profielpunten
        # Change column name PBP_PBP_ID to code
        # Create uuid for profielpunt, in profielpunt this is globalid
        self.data.profielpunt = self.data.iws_geo_beschr_profielpunten.copy()
        self.data.profielpunt = self.data.profielpunt.rename(columns={"pbp_pbp_id": "code"})
        self.data.profielpunt["globalid"] = [str(uuid.uuid4()) for _ in range(len(self.data.profielpunt))]

        # Other information is in gw_pbp
        # We create new columns in profielpunt for the information from gw_pbp
        # We can use the PBP_ID in gw_pbp to join the two tables to code in profielpunt
        self.data.profielpunt = self.data.profielpunt.merge(
            self.data.gw_pbp[["pbp_id", "prw_prw_id", "pbpsoort", "iws_volgnr", "iws_hoogte", "iws_afstand"]],
            left_on="code",
            right_on="pbp_id",
            how="left",
        )
        self.data.profielpunt = self.data.profielpunt.rename(
            columns={
                "pbpsoort": "typeprofielpunt",
                "iws_volgnr": "codevolgnummer",
                "iws_hoogte": "hoogte",
                "iws_afstand": "afstand",
            }
        )

        # We filter profielpunt to keep only 'vaste bodem'
        # 'vaste bodem' is denoted as Z1 in OSMOMSCH column from GW_PRW
        #  GW_PRW can be mapped based on PRW_ID in GW_PRW and PRW_PRW_ID in profielpunt
        # We join, then filter
        self.data.profielpunt = self.data.profielpunt.merge(
            self.data.gw_prw[["prw_id", "pro_pro_id", "osmomsch"]],
            left_on="prw_prw_id",
            right_on="prw_id",
            how="left",
        )
        self.data.profielpunt = self.data.profielpunt[self.data.profielpunt["osmomsch"] == "Z1"]

        # Log warning if there are still duplicates in profielpunt (based on geometry) # TODO temp validation here
        duplicates = self.data.profielpunt[self.data.profielpunt.duplicated(subset=["geometry"], keep=False)]
        if not duplicates.empty:
            self.logger.warning(
                f"Found {len(duplicates)} duplicate geometries in profielpunt. "
                # f"codes: {duplicates['code'].unique()}"
            )

        # Now we need to add the profiellijnid to every profielpunt
        # We can use the PRO_PRO_ID in gw_prw to find the globalid in profiellijn with a matching PRO_ID (which was renamed to code)
        self.data.profielpunt = self.data.profielpunt.merge(
            self.data.profiellijn[["code", "globalid"]],
            left_on="pro_pro_id",
            right_on="code",
            how="left",
            suffixes=("", "_profiellijn"),
        )
        self.data.profielpunt = self.data.profielpunt.rename(columns={"globalid_profiellijn": "profiellijnid"})
        self.data.profielpunt = self.data.profielpunt.drop(columns=["code_profiellijn"])

        # Drop columns that are not needed
        self.data.profielpunt = self.data.profielpunt.drop(columns=["prw_id", "pbp_id", "pro_pro_id", "prw_prw_id"])

        # Geometry in correct format
        self._add_z_to_point_geometry_based_on_column("hoogte")
        self._drop_z_from_linestringz_geometry()

        # Drop profielgroep and profiellijn features with no match with hydroobjects based on no_match_codes
        self.data.profielgroep = self.data.profielgroep[~self.data.profielgroep["code"].isin(no_match_codes)]
        self.data.profiellijn = self.data.profiellijn[~self.data.profiellijn["code"].isin(no_match_codes)]

    def connect_profiles_to_hydroobject_without_profiles(self, max_distance: int = 250) -> None:
        """
        Find hydroobjects that are not connected to profiles.
        Connect them to the nearest profile on the same hydroobject linemerge.

        Parameters
        ----------
        max_distance : int [meters]
            max search distance to connect a profile to another hydroobject.
        """
        self.logger.info("Finding hydroobjects without profiles...")
        self.data._ensure_loaded(layers=["hydroobject_linemerged"], previous_method="process_linemerge")

        # Find primary hydroobjects that are not connected to profiles
        hydroobject_ids_with_profiles = set(self.data.profielgroep["hydroobjectid"].unique())
        primary_hydroobjects = self.data.hydroobject[self.data.hydroobject["categorieoppwaterlichaamcode"] == str(1)]
        hydroobject_without_profiles = primary_hydroobjects[
            ~primary_hydroobjects["globalid"].isin(hydroobject_ids_with_profiles)
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
            self.data.hydroobject_linemerged.explode("hydroobjectcode")
            .groupby("linemergeid")["hydroobjectcode"]
            .apply(list)
            .to_dict()
        )
        code_to_globalid = dict(zip(self.data.hydroobject["code"], self.data.hydroobject["globalid"]))

        # Build spatial index for profiellijn, grouped by linemergeid
        no_profiellijn_for_linemerge = []
        profiellijn_by_linemerge = {}
        for linemerge_id, codes in linemergeid_to_codes.items():
            gids = [code_to_globalid.get(code) for code in codes if code in code_to_globalid]
            profielgroep_ids = self.data.profielgroep[self.data.profielgroep["hydroobjectid"].isin(gids)][
                "globalid"
            ].tolist()
            profiellijn = self.data.profiellijn[
                self.data.profiellijn["profielgroepid"].isin(profielgroep_ids)
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
            linemerge_id = code_to_linemerge_id.get(row["code"])
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

            profielgroep_id = nearest_profile_line["profielgroepid"]

            # Copy rows
            profielgroep_copy = self.data.profielgroep[self.data.profielgroep["globalid"] == profielgroep_id].copy()
            profielgroep_copy["globalid"] = str(uuid.uuid4())  # New globalid for the copy
            profielgroep_copy["hydroobjectid"] = row["globalid"]
            profielgroep_copy["copyof"] = profielgroep_id

            profielgroep_new.append(profielgroep_copy)

        # Concat
        if profielgroep_new:
            self.data.profielgroep = pd.concat([self.data.profielgroep] + profielgroep_new, ignore_index=True)

        self.logger.info("Connected hydroobjects to profiles.")

    def _assign_hydroobject_ids(self) -> list[str]:
        """
        Assign hydroobject globalids to profielgroep features based on spatial intersection.

        Returns
        -------
        list[str]
            Codes of profielgroep features with no intersecting hydroobject.
        """
        profielgroep = self.data.profielgroep
        hydroobject = self.data.hydroobject

        if "globalid" not in hydroobject.columns:
            hydroobject["globalid"] = [str(uuid.uuid4()) for _ in range(len(hydroobject))]

        no_match_codes = []
        multiple_match_codes = []

        for idx, row in profielgroep.iterrows():
            intersecting = hydroobject[hydroobject.intersects(row.geometry)]
            gids_list = intersecting["globalid"].tolist()

            if not gids_list:
                no_match_codes.append(row.get("code"))
                profielgroep.loc[idx, "hydroobjectid"] = np.nan
                continue

            profielgroep.loc[idx, "hydroobjectid"] = gids_list[0]

            if len(gids_list) > 1:
                multiple_match_codes.append((row.get("code"), len(gids_list)))
                for i, gid in enumerate(gids_list[1:], start=2):
                    profielgroep.loc[idx, f"hydroobjectid{i}"] = gid

        if no_match_codes:
            self.logger.warning(f"No intersection for profielgroep with code(s): {no_match_codes}")
        if multiple_match_codes:
            details = ", ".join([f"{idx} ({count})" for idx, count in multiple_match_codes])
            self.logger.warning(f"Multiple intersections for profielgroep with code(s): {details}")

        return no_match_codes

    def _add_z_to_point_geometry_based_on_column(self, column_name: str) -> None:
        self.logger.info(f"Adding Z coordinate to profielpunt geometry based on column '{column_name}'...")
        if column_name not in self.data.profielpunt.columns:
            raise ValueError(f"Column '{column_name}' is not present in profielpunt.")

        self.data.profielpunt[column_name] = pd.to_numeric(self.data.profielpunt[column_name], errors="coerce")
        self.data.profielpunt["geometry"] = self.data.profielpunt.apply(
            lambda row: Point(row.geometry.x, row.geometry.y, row[column_name])
            if pd.notna(row[column_name])
            else row.geometry,
            axis=1,
        )

    def _drop_z_from_linestringz_geometry(self) -> None:
        self.logger.info("Dropping Z coordinate from linestringZ geometry...")
        self.data.profiellijn["geometry"] = self.data.profiellijn["geometry"].apply(
            lambda geom: LineString([(x, y) for x, y, z in geom.coords]) if geom.has_z else geom
        )

    def _linemerge_hydroobjects(self, peilgebiedpraktijk, hydroobject) -> gpd.GeoDataFrame:
        merged_hydroobjects = []
        hydroobject_map = {}
        peilgebieden_sindex = peilgebiedpraktijk.sindex

        hydroobject_to_peilgebied = {}
        for idx, ho_row in hydroobject.iterrows():
            ho_geom = ho_row.geometry
            possible_peil_idx = list(peilgebieden_sindex.query(ho_geom))
            if not possible_peil_idx:
                self.logger.warning(f"No peilgebied found for hydroobject with globalid {ho_row['globalid']}.")
                continue
            max_length = 0
            best_peilgebied_id = None
            for peil_idx in possible_peil_idx:
                peil_row = peilgebiedpraktijk.iloc[peil_idx]
                intersection = ho_geom.intersection(peil_row.geometry)
                if intersection.is_empty:
                    continue
                overlap = intersection.length
                if overlap > max_length:
                    max_length = overlap
                    best_peilgebied_id = peil_row["objectid"]
            if best_peilgebied_id is not None:
                hydroobject_to_peilgebied[idx] = best_peilgebied_id

        hydroobject = hydroobject.copy()
        hydroobject["assigned_peilgebied"] = hydroobject.index.map(hydroobject_to_peilgebied)
        hydroobject = hydroobject[hydroobject["assigned_peilgebied"].notna()]

        for peilgebied_id, group_peilgebied in hydroobject.groupby("assigned_peilgebied"):
            primary_group = group_peilgebied[group_peilgebied["categorieoppwaterlichaamcode"].isin([str(1), 1])]
            if primary_group.empty:
                continue

            merged_geom = primary_group.geometry.union_all()
            if merged_geom.geom_type == "GeometryCollection":
                merged_geom = self._geometrycollection_to_linestring(merged_geom)
            if merged_geom.geom_type == "LineString":
                merged_geom = MultiLineString([merged_geom])
            if merged_geom.is_empty or merged_geom.geom_type != "MultiLineString":
                continue

            codes = primary_group["code"].unique()
            linemerge_id = str(uuid.uuid4())
            for code in codes:
                hydroobject_map[code] = linemerge_id

            merged_hydroobjects.append(
                {
                    "geometry": merged_geom,
                    "hydroobjectcode": codes,
                    "linemergeid": linemerge_id,
                    "peilgebiedid": peilgebied_id,
                    "categorie": "primary",
                }
            )

        if not merged_hydroobjects:
            return None
        else:
            merged_hydroobjects_gdf = gpd.GeoDataFrame(merged_hydroobjects, crs=hydroobject.crs)

        if merged_hydroobjects_gdf.empty:
            return None
        else:
            self.hydroobject_map = hydroobject_map
            return merged_hydroobjects_gdf

    def find_linemerge_id_by_hydroobject_code(self, code: str) -> str:
        """
        Utility/debug function and not used in the main class workflow.

        Find the linemergeid by hydroobject code.
        """
        if code in self.hydroobject_map:
            return self.hydroobject_map[code]
        else:
            self.logger.warning(f"No linemerge found for hydroobject with code {code}.")
            return None

    def find_peilgebied_id_by_hydroobject_code(self, code: str):
        """
        Utility/debug function and not used in the main class workflow.

        Find the peilgebiedid by hydroobject code.
        """

        self.data._ensure_loaded(layers=["hydroobject_linemerged"], previous_method="process_linemerge")

        # First, find the linemergeid for this hydroobject code
        if code in self.hydroobject_map:
            linemerge_id = self.hydroobject_map[code]

            # Now, lookup the peilgebiedid in the linemerged GeoDataFrame
            match = self.data.hydroobject_linemerged[self.data.hydroobject_linemerged["linemergeid"] == linemerge_id]

            if match.empty:
                self.logger.warning(f"No matching linemerge found for hydroobject code '{code}'.")
                return None
            else:
                peilgebied_id = match.iloc[0]["peilgebiedid"]
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

        if "diepstepunt" not in self.data.hydroobject.columns:
            raise ValueError(
                "diepstepunt column is not present in hydroobject. Call compute_deepest_point_hydroobjects() first."
            )

        # Find the hydroobject by code
        hydroobject = self.data.hydroobject[self.data.hydroobject["code"] == code]
        if hydroobject.empty:
            self.logger.warning(f"No hydroobject found with code '{code}'.")
            return None

        # Get the deepest point
        diepste_punt = hydroobject["diepstepunt"].values[0]
        return diepste_punt

    def compute_deepest_point_hydroobjects(self) -> None:
        """Compute the deepest point for each hydroobject that has a profile."""
        self.logger.info("Computing the deepest point for hydroobjects...")

        self.data._ensure_loaded(layers=["hydroobject"], previous_method="load_layers")
        self.data._ensure_loaded(layers=["profielgroep"], previous_method="create_profile_tables")

        # filter the hydroobject on reference in profielgroep
        # these are the hydroobjects that have a profile
        hydroobject_with_profiles = self.data.hydroobject[
            self.data.hydroobject["globalid"].isin(self.data.profielgroep["hydroobjectid"])
        ]
        if hydroobject_with_profiles.empty:
            self.logger.warning("No hydroobjects with profiles found. Skipping deepest point computation.")
            return

        # Check if "diepstepunt" is already computed
        if "diepstepunt" not in self.data.profiellijn.columns:
            self._compute_deepest_point_profiellijn()

        # collect the profiellijn features per hydroobject
        hydroobject_deepest_points = []
        for _, hydro_row in hydroobject_with_profiles.iterrows():
            hydro_code = hydro_row["code"]

            # Find the corresponding profiellijn
            profiellijn = self._find_profiellijn_by_hydroobject_code(hydro_code)
            if profiellijn is None or profiellijn.empty:
                continue

            # Find the deepest point in the profiellijn
            diepste_punt = profiellijn["diepstepunt"].min()
            if pd.isna(diepste_punt):
                continue

            # Append to results
            hydroobject_deepest_points.append(
                {"globalid": hydro_row["globalid"], "diepstepunt": round(diepste_punt, 2)}
            )

        # Create a DataFrame from the results
        hydroobject_deepest_points_df = pd.DataFrame(hydroobject_deepest_points)
        if hydroobject_deepest_points_df.empty:
            self.logger.warning("No deepest points computed for hydroobjects.")
            return

        # Merge the deepest points back to the hydroobject GeoDataFrame
        self.data.hydroobject = self.data.hydroobject.merge(hydroobject_deepest_points_df, on="globalid", how="left")

        # log the number of hydroobjects with computed deepest points
        num_deepest_points = self.data.hydroobject["diepstepunt"].notna().sum()
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
        deepest_points = self.data.profielpunt.groupby("profiellijnid")["hoogte"].min()

        # Map the deepest point to the profiellijn using its globalid
        self.data.profiellijn["diepstepunt"] = self.data.profiellijn["globalid"].map(deepest_points)

    def _find_profiellijn_by_hydroobject_code(self, code: str) -> Optional[gpd.GeoDataFrame]:
        """
        Find the profiellijn GeoDataFrame by hydroobject code.

        Called by: compute_deepest_point_hydroobjects
        """
        self.data._ensure_loaded(layers=["profiellijn"], previous_method="create_profile_tables")

        # Find the hydroobjectid for the given code
        hydroobject_id = self.data.hydroobject[self.data.hydroobject["code"] == code]["globalid"].values
        if not hydroobject_id:
            self.logger.warning(f"No hydroobject found with code '{code}'.")
            return None

        # Filter profielgroep by hydroobjectid
        profielgroep = self.data.profielgroep[self.data.profielgroep["hydroobjectid"] == hydroobject_id[0]]
        if profielgroep.empty:
            self.logger.warning(f"No profielgroep found for hydroobject code '{code}'.")
            return None

        # Get the profielgroep_ids, use 'copyof' if present, otherwise use 'globalid'
        profielgroep_ids = profielgroep.apply(
            lambda row: row["copyof"] if "copyof" in row and pd.notna(row["copyof"]) else row["globalid"], axis=1
        ).tolist()

        # Get the profiellijn that matches the profielgroep_ids
        profiellijn = self.data.profiellijn[self.data.profiellijn["profielgroepid"].isin(profielgroep_ids)]

        if profiellijn.empty:
            self.logger.warning(f"No profiellijn found for hydroobject code '{code}'.")
            return None

        return profiellijn
