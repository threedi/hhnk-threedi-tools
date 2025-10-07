from functools import cached_property

import geopandas as gpd
import numpy as np
import pandas as pd
from hhnk_research_tools import logging

from hhnk_threedi_tools.core.folders import Folders

logger = logging.get_logger(name=__name__)


class StructureRelations:
    """
    Class that adds properties from the schematisation to structure table.
     - maximum and minimum crest_level from table control
     - minimum and reference_level and bank_level at start and end side from cross_section_locations linked to structure, reference and bank level may originate from different cross_section_locations

     # TODO
    """

    def __init__(
        self,
        folder: Folders,
        structure_table: str = "",
    ):
        self.folder = folder
        self.database = folder.model.schema_base.database

        if structure_table not in ["weir", "culvert", "pump", "orifice"]:
            raise ValueError("Provide structure table weir, culvert, pump or orifice")
        else:
            self.structure_table = structure_table

    def concat_channel_ids(self) -> gpd.GeoDataFrame:
        """Concatenate channel_id and connection_node_id from channels_gdf for both start and end side.

        Returns
        -------
        gpd.GeoDataFrame
            DataFrame with columns channel_id and connection_node_id
        """

        channels_gdf = self.database.load(layer="channel", index_column="id")
        channels_gdf["channel_id"] = channels_gdf.index

        channel_join_df = pd.concat(
            [
                channels_gdf[["channel_id", "connection_node_id_start"]].rename(
                    columns={"connection_node_id_start": "connection_node_id"}
                ),
                channels_gdf[["channel_id", "connection_node_id_end"]].rename(
                    columns={"connection_node_id_end": "connection_node_id"}
                ),
            ]
        )
        return channel_join_df

    def join_channel(
        self, structure_gdf: gpd.GeoDataFrame, channel_join_df: gpd.GeoDataFrame, side: str
    ) -> gpd.GeoDataFrame:
        """Join channel_id to structure table at start or end node.

        Parameters
        ----------
        structure_gdf : gpd.GeoDataFrame
            Structure GeoDataFrame with added column channel_id_{side}
        channel_join_df : gpd.GeoDataFrame
            DataFrame with columns channel_id and connection_node_id concatednated from channels table
        side : str
            "start" or "end" side of the structure

        Raises
        ------
        ValueError
            If side is not "start" or "end"

        Returns
        -------
        gpd.GeoDataFrame
            Structure GeoDataFrame with added column channel_id_{side}

        """
        if side not in ["start", "end"]:
            raise ValueError("side should be 'start' or 'end'")

        # Join channel_id to structure table at start node
        structure_gdf = (
            structure_gdf.merge(
                channel_join_df,
                left_on=f"connection_node_id_{side}",
                right_on="connection_node_id",
                how="left",
            )
            .rename(columns={"channel_id": f"channel_id_{side}"})
            .drop(columns=["connection_node_id"])
        )

        return structure_gdf

    def join_cross_sections(
        self, structure_gdf: gpd.GeoDataFrame, cross_section_gdf: gpd.GeoDataFrame, side: str
    ) -> gpd.GeoDataFrame:
        """Join cross section location to structure table at start or end node.

        Use only closest cross section location to channel_id_{side}.

        Parameters
        ----------
        structure_gdf : gpd.GeoDataFrame
            Structure GeoDataFrame with added column channel_id_{side}
        cross_section_gdf : gpd.GeoDataFrame
            Cross section location GeoDataFrame with columns cs_id, channel_id, reference_level, bank_level, geometry
        side : str
            "start" or "end" side of the structure

        Raises
        ------
        ValueError
            If side is not "start" or "end"

        Returns
        -------
        gpd.GeoDataFrame
            Structure GeoDataFrame with added columns from cross_section_location

        """
        if side not in ["start", "end"]:
            raise ValueError("side should be 'start' or 'end'")

        # Join cross section locations on channel_id
        structure_gdf = (
            structure_gdf.merge(
                cross_section_gdf[
                    [
                        "cs_id",
                        "channel_id",
                        "reference_level",
                        "bank_level",
                        "geometry",
                    ]
                ].rename(columns={"geometry": f"geometry_cs_{side}"}),
                left_on=f"channel_id_{side}",
                right_on="channel_id",
                how="left",
            )
            .rename(
                columns={
                    "cs_id": f"cs_id_{side}",
                    "reference_level": f"ref_level_{side}",
                    "bank_level": f"bank_level_{side}",
                }
            )
            .drop(columns=["channel_id"])
        )

        # Calculate distance between structure and cross section location
        structure_gdf[f"dist_cs_{side}"] = structure_gdf.apply(
            lambda row: row.geometry.distance(row[f"geometry_cs_{side}"]), axis=1
        )

        return structure_gdf

    def get_min_max_levels(self, structure_gdf: gpd.GeoDataFrame, side: str) -> gpd.GeoDataFrame:
        """Get minimum and maximum reference level and bank level at start or end side of structure.

        Parameters
        ----------
        structure_gdf : gpd.GeoDataFrame
            Structure GeoDataFrame with added columns from cross_section_location
        structure_table : str
            "weir", "culvert", "pump" or "orifice"
        side : str
            "start" or "end" side of the structure

        Raises
        ------
        ValueError
            If side is not "start" or "end"

        Returns
        -------
        gpd.GeoDataFrame
            Structure GeoDataFrame with added columns min and max _ref_level_{side} and _bank_level_{side}

        """
        if side not in ["start", "end"]:
            raise ValueError("side should be 'start' or 'end'")

        # Get channel join df
        channel_join_df = self.concat_channel_ids()

        # Join channel and cross section location to structure table
        cross_section_gdf = self.database.load(layer="cross_section_location", index_column="id")
        cross_section_gdf["cs_id"] = cross_section_gdf.index

        # Create temporary dataframe to join to main structure table later
        struct_clean_gdf = structure_gdf[
            ["geometry", f"{self.structure_table}_id", f"connection_node_id_{side}"]
        ].copy()
        # Join multiple channel ids
        struct_join_gdf = self.join_channel(structure_gdf=struct_clean_gdf, channel_join_df=channel_join_df, side=side)
        # Join multiple cross section locations
        struct_join_gdf = self.join_cross_sections(
            structure_gdf=struct_join_gdf, cross_section_gdf=cross_section_gdf, side=side
        )
        # Filter on closest cross section location to structure at side with same channel_id
        struct_join_csl_gdf = (
            struct_join_gdf.sort_values(by=[f"dist_cs_{side}"])
            .drop_duplicates(subset=[f"{self.structure_table}_id", f"channel_id_{side}"], keep="first")
            .drop(
                columns=[
                    # f"geometry_cs_{side}",
                    f"dist_cs_{side}",
                    f"connection_node_id_{side}",
                    f"channel_id_{side}",
                ]
            )
        )

        # Get minimum and maximum reference level at start or end side of structure
        struct_join_csl_sorted_gdf = struct_join_csl_gdf.sort_values(by=[f"ref_level_{side}"]).drop(
            columns=[f"bank_level_{side}"]
        )
        struct_min_ref_level_gdf = struct_join_csl_sorted_gdf.drop_duplicates(
            subset=[
                f"{self.structure_table}_id",
            ],
            keep="first",
        ).rename(
            columns={
                f"ref_level_{side}": f"min_ref_level_{side}",
                f"cs_id_{side}": f"cs_id_min_ref_level_{side}",
                f"geometry_cs_{side}": f"geometry_cs_min_ref_{side}",
            }
        )
        struct_max_ref_level_gdf = struct_join_csl_sorted_gdf.drop_duplicates(
            subset=[
                f"{self.structure_table}_id",
            ],
            keep="last",
        ).rename(
            columns={
                f"ref_level_{side}": f"max_ref_level_{side}",
                f"cs_id_{side}": f"cs_id_max_ref_level_{side}",
                f"geometry_cs_{side}": f"geometry_cs_max_ref_{side}",
            }
        )

        # Get minimum and maximum bank level at start or end side of structure
        struct_join_csl_sorted_gdf = struct_join_csl_gdf.sort_values(by=[f"bank_level_{side}"]).drop(
            columns=[f"ref_level_{side}"]
        )
        struct_min_bank_level_gdf = struct_join_csl_sorted_gdf.drop_duplicates(
            subset=[
                f"{self.structure_table}_id",
            ],
            keep="first",
        ).rename(
            columns={
                f"bank_level_{side}": f"min_bank_level_{side}",
                f"cs_id_{side}": f"cs_id_min_bank_level_{side}",
                f"geometry_cs_{side}": f"geometry_cs_min_bank_{side}",
            }
        )
        struct_max_bank_level_gdf = struct_join_csl_sorted_gdf.drop_duplicates(
            subset=[
                f"{self.structure_table}_id",
            ],
            keep="last",
        ).rename(
            columns={
                f"bank_level_{side}": f"max_bank_level_{side}",
                f"cs_id_{side}": f"cs_id_max_bank_level_{side}",
                f"geometry_cs_{side}": f"geometry_cs_max_bank_{side}",
            }
        )

        # Join min and max of ref and bank levels to structure table
        structure_gdf = (
            structure_gdf.merge(
                struct_min_ref_level_gdf[
                    [
                        f"{self.structure_table}_id",
                        f"min_ref_level_{side}",
                        f"cs_id_min_ref_level_{side}",
                        f"geometry_cs_min_ref_{side}",
                    ]
                ],
                left_on=f"{self.structure_table}_id",
                right_on=f"{self.structure_table}_id",
                how="left",
            )
            .merge(
                struct_max_ref_level_gdf[
                    [
                        f"{self.structure_table}_id",
                        f"max_ref_level_{side}",
                        f"cs_id_max_ref_level_{side}",
                        f"geometry_cs_max_ref_{side}",
                    ]
                ],
                left_on=f"{self.structure_table}_id",
                right_on=f"{self.structure_table}_id",
                how="left",
            )
            .merge(
                struct_min_bank_level_gdf[
                    [
                        f"{self.structure_table}_id",
                        f"min_bank_level_{side}",
                        f"cs_id_min_bank_level_{side}",
                        f"geometry_cs_min_bank_{side}",
                    ]
                ],
                left_on=f"{self.structure_table}_id",
                right_on=f"{self.structure_table}_id",
                how="left",
            )
            .merge(
                struct_max_bank_level_gdf[
                    [
                        f"{self.structure_table}_id",
                        f"max_bank_level_{side}",
                        f"cs_id_max_bank_level_{side}",
                        f"geometry_cs_max_bank_{side}",
                    ]
                ],
                left_on=f"{self.structure_table}_id",
                right_on=f"{self.structure_table}_id",
                how="left",
            )
        )

        return structure_gdf

    def get_manhole_data_at_connection_node(self, structure_gdf: gpd.GeoDataFrame, side: str) -> gpd.GeoDataFrame:
        """Join manhole data (from connection node) on start and end node of structure

        Parameters
        ----------
        structure_gdf : gpd.GeoDataFrame
            Structure GeoDataFrame with added columns from cross_section_location
        side : str
            "start" or "end" side of the structure


        Returns
        -------
        gpd.GeoDataFrame
            Structure GeoDataFrame with added data from manholes
        """
        if side not in ["start", "end"]:
            raise ValueError("side should be 'start' or 'end'")

        # Load connection nodes
        connection_node_gdf = self.database.load(layer="connection_node", index_column="id")
        connection_node_gdf["conn_id"] = connection_node_gdf.index

        # Join connection nodes
        structure_gdf = structure_gdf.merge(
            connection_node_gdf[
                [
                    "conn_id",
                    "storage_area",
                    "initial_water_level",
                    "bottom_level",
                    "exchange_level",
                    "exchange_type",
                ]
            ].rename(
                columns={
                    "cs_id": f"cs_id_{side}",
                    "storage_area": f"storage_area_{side}",
                    "initial_water_level": f"initial_water_level_{side}",
                    "bottom_level": f"bottom_level_{side}",
                    "exchange_level": f"exchange_level_{side}",
                    "exchange_type": f"exchange_type_{side}",
                }
            ),
            left_on=f"connection_node_id_{side}",
            right_on="conn_id",
            how="left",
        ).drop(columns=["conn_id"])

        return structure_gdf

    def join_table_control(
        self,
        structure_gdf: gpd.GeoDataFrame,
    ) -> gpd.GeoDataFrame:
        """Join table control to structure table on structure id.

        Parameters
        ----------
        structure_gdf : gpd.GeoDataFrame
            Structure GeoDataFrame with added column channel_id_{side}
        structure_table : str
            "weir", "culvert", "pump" or "orifice"

        Returns
        -------
        gpd.GeoDataFrame
            Structure GeoDataFrame with added columns from table_control

        """
        if self.structure_table not in ["weir", "culvert", "pump", "orifice"]:
            raise ValueError("Provide structure table weir, culvert, pump or orifice")

        table_control_gdf = self.database.load(layer="table_control", index_column="id")
        table_control_gdf["control_id"] = table_control_gdf.index

        # Add prperties from table controle to structure table
        if self.structure_table in ["weir", "orifice"]:
            # filter control table on weir
            table_control_gdf = table_control_gdf[table_control_gdf["target_type"] == self.structure_table]
            # filter control table on action type
            set_crest_level_gdf = table_control_gdf[table_control_gdf["action_type"] == "set_crest_level"]

            # Check if there are any other table controles
            if len(table_control_gdf[table_control_gdf["action_type"] != "set_crest_level"]) > 0:
                logger.warning(
                    f"Found table control other than 'set_crest_level' for table {self.structure_table}, but this is not joined to structure as this is not implemented for any action type other than set_crest_level"
                )
                # TODO andere action types nog toevoegen

            if len(set_crest_level_gdf) > 0:
                # Bepaal de minimale kruinhoogte uit de action table
                for index, row in set_crest_level_gdf.iterrows():
                    # convert action table string an array on new line as seperator
                    action_table_array = row["action_table"].split("\n")
                    # split each item in array on , and get second line as float
                    action_values = [float(line.split(",")[1]) for line in action_table_array]
                    # get min and max of action values
                    set_crest_level_gdf.at[index, "min_crest_level_control"] = min(action_values)  # noqa: PD008
                    set_crest_level_gdf.at[index, "max_crest_level_control"] = max(action_values)  # noqa: PD008

                structure_gdf = structure_gdf.merge(
                    set_crest_level_gdf[
                        [
                            "target_id",
                            "control_id",
                            "min_crest_level_control",
                            "max_crest_level_control",
                            "action_table",
                            "measure_operator",
                        ]
                    ],
                    left_on=f"{self.structure_table}_id",
                    right_on="target_id",
                    how="left",
                ).drop(columns=["target_id"])
        else:
            logger.warning(
                f"Join table control not implemented for structure table {self.structure_table}, only available for weir and orifice"
            )  # TODO

        return structure_gdf

    @cached_property
    def gdf(self) -> gpd.GeoDataFrame:
        # Load table
        structure_gdf = self.database.load(layer=self.structure_table, index_column="id")
        structure_gdf[f"{self.structure_table}_id"] = structure_gdf.index

        for side in ["start", "end"]:
            structure_gdf = self.get_manhole_data_at_connection_node(structure_gdf=structure_gdf, side=side)
            structure_gdf = self.get_min_max_levels(structure_gdf=structure_gdf, side=side)

        # Join table control to structure table
        structure_gdf = self.join_table_control(structure_gdf)

        return structure_gdf

    def get_wrong_profile(self, side: str) -> gpd.GeoDataFrame:
        """Filter structures where the reference level of the cross section location reference level is below the orifice crest level or the culvert inlet level of the adjacent channels.

        If there is a manhole on the start or end node, profiles are not checked.

        Parameters
        ----------
        side : str
            "start" or "end" side of the structure


        Returns
        -------
        gpd.GeoDataFrame
            Cross section locations that are wrong
        """
        if side not in ["start", "end"]:
            raise ValueError("side should be 'start' or 'end'")

        struct_gdf = self.gdf

        if self.structure_table == "culvert":
            struct_gdf["minimal_level"] = struct_gdf[f"invert_level_{side}"]
        elif self.structure_table in ["weir", "orifice"]:
            if "min_crest_level_control" in struct_gdf.columns:
                # Get lowest value from min_crest_level_control and crest_level if not nan
                struct_gdf["minimal_level"] = np.nanmin(
                    [struct_gdf["min_crest_level_control"], struct_gdf["crest_level"]], axis=0
                )
            else:
                struct_gdf["minimal_level"] = struct_gdf["crest_level"]

        # Filter sunk structures on side start or end
        struct_sunk_gdf = struct_gdf[
            (struct_gdf["minimal_level"] < struct_gdf[f"min_ref_level_{side}"])
            & (struct_gdf[f"bottom_level_{side}"].isna())
        ].copy()
        struct_sunk_gdf["cs_id_min_ref_level"] = struct_sunk_gdf[f"cs_id_min_ref_level_{side}"]
        # Set geometry to crs geometry
        struct_sunk_gdf["geometry"] = struct_sunk_gdf[f"geometry_cs_min_ref_{side}"].drop(
            columns=[f"geometry_cs_min_ref_{side}"]
        )
        # Select relevant columns
        wrong_profiles = struct_sunk_gdf[
            [
                "cs_id_min_ref_level",
                f"min_ref_level_{side}",
                "code",
                "minimal_level",
                "geometry",
            ]
        ].rename(
            columns={
                "cs_id_min_ref_level": "cross_section_location_id",
                f"min_ref_level_{side}": "reference_level",
                "code": "structure_code",
                "minimal_level": "proposed_reference_level",
            }
        )

        return wrong_profiles
