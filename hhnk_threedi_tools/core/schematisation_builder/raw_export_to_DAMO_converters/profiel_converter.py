import uuid

import pandas as pd

from hhnk_threedi_tools.core.schematisation_builder.raw_export_to_DAMO_converter import RawExportToDAMOConverter


class ProfielConverter(RawExportToDAMOConverter):
    """Profile-specific converter implementation."""

    def run(self):
        self.logger.info("Starting ProfielConverter...")
        self.load_layers()
        self.process_linemerge()
        self.create_profile_tables()
        self.connect_profiles_to_hydroobject_without_profiles()
        self.write_outputs()

    def load_layers(self):
        self.logger.info("Loading profile-specific layers...")
        self.data.hydroobject = self._load_and_validate(self.raw_export_file_path, "hydroobject")
        self.data.gw_pro = self._load_and_validate(self.raw_export_file_path, "gw_pro")
        self.data.gw_prw = self._load_and_validate(self.raw_export_file_path, "gw_prw")
        self.data.gw_pbp = self._load_and_validate(self.raw_export_file_path, "gw_pbp")
        self.data.iws_geo_beschr_profielpunten = self._load_and_validate(
            self.raw_export_file_path, "iws_geo_beschr_profielpunten"
        )
        peilgebiedpraktijk = self._load_and_validate(self.raw_export_file_path, "peilgebiedpraktijk")
        self.data.peilgebiedpraktijk = peilgebiedpraktijk.explode(index_parts=False).reset_index(drop=True)

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
