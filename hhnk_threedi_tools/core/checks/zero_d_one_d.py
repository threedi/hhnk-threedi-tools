# -*- coding: utf-8 -*-
"""
Created on Fri Aug 20 16:09:34 2021

@author: chris.kerklaan
"""
# Third-party imports
import pandas as pd
import hhnk_research_tools as hrt


from hhnk_research_tools.threedi.geometry_functions import coordinates_to_points
import hhnk_threedi_tools.core.checks.grid_result_metadata as grid_result_metadata

from hhnk_research_tools.variables import (
    t_end_sum_col,
    t_end_rain_col,
    t_end_rain_min_one_col,
    t_0_col,
    t_start_rain_col,
    t_index_col,
    t_end_rain_col,
    all_1d,
    DEF_TRGT_CRS,
)

# Local imports
from hhnk_threedi_tools.core.folders import Folders
from hhnk_threedi_tools.variables.zero_d_one_d import (
    lvl_start_col,
    lvl_end_col,
    lvl_end_rain_col,
    lvl_rain_col,
    res_orifices,
    res_culverts,
    res_channels,
    start_node_col,
    end_node_col,
    code_col,
    flow_direction_col,
    slope_col,
    water_lvl_diff_col,
    q_col,
    u_var_col,
    slope_abs_cm_km_col,
    map_id_col,
    zoom_cat_col,
    upstream_id_col,
    downstream_id_col,
    waterlevel_up_end_col,
    waterlevel_down_end_col,
    waterlevel_up_start_col,
    waterlevel_down_start_col,
    waterlevel_diff_abs_m_col,
    struct_on_lvl_limit_col,
    id_col,
    index_col,
    primary_col,
    waterlevel_t_end_col,
    waterlevel_t_0_col,
)


class ZeroDOneDTest:
    def __init__(self, folder: Folders, revision=0):
        self.fenv = folder
        self.grid_result = folder.threedi_results.zero_d_one_d[revision].grid
        (
            rain,
            detected_rain,
            timestep,
            days_dry_start,
            days_dry_end,
            self.timestep_df,
        ) = grid_result_metadata.construct_scenario(self.grid_result)

    @classmethod
    def from_path(cls, path_to_polder, revision=0):
        return cls(Folders(path_to_polder), revision)

    def run(self):

        cols = [
            t_end_sum_col,
            t_end_rain_col,
            t_end_rain_min_one_col,
            t_start_rain_col,
            t_0_col,
        ]
        try:
            # Get subset of nodes (1D)
            subset_1d = self.grid_result.nodes.subset(all_1d)

            # Create waterlevels dataframe
            waterlevel_lst = subset_1d.timeseries(
                indexes=list(self.timestep_df.values[0])
            ).s1
            waterlevel_df = pd.DataFrame(
                waterlevel_lst.T, columns=self.timestep_df.columns
            )

            # Gather information into dataframe
            results_df = waterlevel_df[cols].copy()
            results_df.rename(
                columns={
                    t_start_rain_col: f"wlvl_{t_start_rain_col}",
                    t_0_col: f"wlvlv_{t_0_col}",
                    t_end_rain_min_one_col: f"wvlv_{t_end_rain_min_one_col}",
                    t_end_rain_col: f"wlvlv_{t_end_rain_col}",
                    t_end_sum_col: f"wlvl_{t_end_sum_col}",
                },
                inplace=True,
            )
            # Peilverschillen tussen aantal timesteps
            # verschil in waterstand van start regen tov start berekening
            results_df.insert(
                0,
                lvl_start_col,
                waterlevel_df[t_start_rain_col] - waterlevel_df[t_0_col],
            )
            # peilstijging tijdens neerslagperiode
            results_df.insert(
                0,
                lvl_rain_col,
                waterlevel_df[t_end_rain_col] - waterlevel_df[t_start_rain_col],
            )
            # check of er een evenwicht is aan het einde van de neerslagperiode
            results_df.insert(
                0,
                lvl_end_rain_col,
                waterlevel_df[t_end_rain_col] - waterlevel_df[t_end_rain_min_one_col],
            )
            # keert de waterstand weer terug naar peil begin regen
            results_df.insert(
                0, lvl_end_col, waterlevel_df[t_end_sum_col] - waterlevel_df[t_0_col]
            )

            # Add timesteps
            for col in cols:
                results_df.insert(
                    results_df.shape[1], col, self.timestep_df[col].values[0]
                )

            # Get point geometry from nodes
            crds = coordinates_to_points(nodes=subset_1d)

            results_gdf = hrt.df_add_geometry_to_gdf(df=results_df, geometry_col=crds)

            self.results = results_gdf

            return results_gdf
        except Exception as e:
            raise e from None

    def run_hydraulic(self):
        try:
            T_0 = self.timestep_df.loc[t_index_col, t_0_col]
            T_end = self.timestep_df.loc[t_index_col, t_end_rain_col]

            wtrlvl_nodes_at_timesteps = get_nodes_1d(self.grid_result, T_0, T_end)

            channels_gdf = create_structure_gdf(
                threedi_result=self.grid_result,
                structure_name=res_channels,
                wtrlvl_nodes_at_timesteps=wtrlvl_nodes_at_timesteps,
                t_end=T_end,
            )

            # Find all connection nodes from channels that are primary and combine in list without duplicates
            primary_nodes_series = channels_gdf.loc[
                channels_gdf.zoom_cat == 4, start_node_col
            ].append(channels_gdf.loc[channels_gdf.zoom_cat == 4, end_node_col])
            primary_nodes = primary_nodes_series.unique().tolist()

            culvert_gdf = create_structure_gdf(
                threedi_result=self.grid_result,
                structure_name=res_culverts,
                wtrlvl_nodes_at_timesteps=wtrlvl_nodes_at_timesteps,
                t_end=T_end,
                primary_nodes=primary_nodes,
            )
            orifice_gdf = create_structure_gdf(
                threedi_result=self.grid_result,
                structure_name=res_orifices,
                wtrlvl_nodes_at_timesteps=wtrlvl_nodes_at_timesteps,
                t_end=T_end,
                primary_nodes=primary_nodes,
            )

            # combine orifices and culverts into one dataframe
            structures_gdf = pd.concat([orifice_gdf, culvert_gdf])

            self.hydraulic_results = {
                "channels": channels_gdf,
                "structures": structures_gdf,
            }
            return channels_gdf, structures_gdf
        except Exception as e:
            raise e from None


def add_code(table, structure_lines, structure_name):
    # Conversion to string is because otherwise these are objects, which isn't valid for geopackage format
    # Ophalen code die in damo staat (dit werkt nog niet lekker in de grid)
    if (
        structure_name != res_orifices
    ):  # Voor orifices staat dit niet in de code, maar in de display_name
        table[code_col] = structure_lines.code.astype("U13")
    else:
        table[code_col] = structure_lines.display_name.astype("U13")


def add_waterlevel_info(structure, structure_lines, wtrlvl_nodes_at_timesteps, t_end):
    # Bepalen waterstanden -----------------------------------------------------------
    # Upstream en downstream id van de connection nodes van het structure
    up_ids = list(structure_lines.line_nodes[:, 0])
    down_ids = list(
        structure_lines.line_nodes[:, 1]
    )  # Downstream id van de connection nodes

    # Get upstream and downstream id per structure
    ids_df = pd.DataFrame(up_ids, columns=[upstream_id_col])
    ids_df[downstream_id_col] = down_ids

    # Koppel deze aan de water levels die in de nodes_1d dataframe staan om de waterstanden te koppelen
    # Add water levels based on upstream and downstream id's
    up_waterlevel = pd.merge(
        ids_df,
        wtrlvl_nodes_at_timesteps,
        left_on=upstream_id_col,
        right_on=id_col,
        how="left",
    )
    down_waterlevel = pd.merge(
        ids_df,
        wtrlvl_nodes_at_timesteps,
        left_on=downstream_id_col,
        right_on=id_col,
        how="left",
    )

    # Zet deze informatie in de structuretabel
    structure[waterlevel_up_end_col] = up_waterlevel[
        waterlevel_t_end_col
    ]  # Waterlevel_end
    structure[waterlevel_down_end_col] = down_waterlevel[waterlevel_t_end_col]

    structure[waterlevel_up_start_col] = up_waterlevel[waterlevel_t_0_col]
    structure[waterlevel_down_start_col] = down_waterlevel[waterlevel_t_0_col]

    # Water level difference in METER
    structure[water_lvl_diff_col] = [
        (u - d)
        for u, d in zip(
            structure[waterlevel_up_end_col], structure[waterlevel_down_end_col]
        )
    ]
    structure[q_col] = structure_lines.timeseries(indexes=[t_end]).q.tolist()[
        0
    ]  # Discharge
    structure[u_var_col] = structure_lines.timeseries(indexes=[t_end]).u1.tolist()[
        0
    ]  # Velocity
    return up_waterlevel, down_waterlevel


def check_primary(structure, structure_lines, structure_name, primary_nodes):
    """
    Based on what type of structure we are dealing with
    If channels, we check zoom cat
    If kunstwerk, we use the list of primary nodes (channels connection nodes)
    """
    structure[start_node_col] = structure_lines.line_nodes[:, 0]
    structure[end_node_col] = structure_lines.line_nodes[
        :, 1
    ]  # Downstream id of connection nodes

    if structure_name == res_channels:
        # Classify parts of channels as primary if zoom category is 4
        structure[primary_col] = structure[zoom_cat_col].apply(
            lambda x: True if x == 4 else False
        )
    else:
        # We use nodes qualified as primary by above method for other structure types
        structure[primary_col] = structure.apply(
            lambda row: row[start_node_col] in primary_nodes
            and row[end_node_col] in primary_nodes,
            axis=1,
        )


def add_slope_info(structure, structure_name, up_waterlevel, down_waterlevel):
    """
    Bepaal verhang bij kunstwerken
    """
    # Bepaal de richting van de stroming en neem absolute waarden aan voor afvoerwaarden
    structure[flow_direction_col] = structure[q_col].apply(lambda x: -1 if x < 0 else 1)

    # bereken het verhang over het structure
    structure[slope_col] = (structure[water_lvl_diff_col] * 100) / (
        structure.length.values * 0.001
    )  # CM/KM structure.length.values is de lengte van het segment

    # Absolute waarden meenemen
    structure[q_col] = structure[
        q_col
    ].abs()  # Absoluut debiet, richting staat in structure['richting']
    structure[u_var_col] = structure[u_var_col].abs()  # Absolute snelheid
    structure[slope_abs_cm_km_col] = structure[
        slope_col
    ].abs()  # structure.length.values is de lengte van het segment
    structure[waterlevel_diff_abs_m_col] = structure[water_lvl_diff_col].abs()

    # Bepalen of een structure op een peilgrens ligt, en daardoor is het verhang niet interessant.
    if structure_name in [res_orifices, res_culverts]:
        structure[struct_on_lvl_limit_col] = False
        # Dit testen we door te kijken of de bovenstroomse en benedenstrooms INITIELE waterstand gelijk zijn.
        structure.loc[
            (round(up_waterlevel[waterlevel_t_0_col], 2))
            != (round(down_waterlevel[waterlevel_t_0_col], 2)),
            struct_on_lvl_limit_col,
        ] = True


def create_structure_gdf(
    threedi_result, structure_name, wtrlvl_nodes_at_timesteps, t_end, primary_nodes=[]
):
    """Lees de netCDF uit voor de verschillende structure typen
        - channel
        - orifice
        - culvert
    TODO: Koppelen aan Sqlite om de structurecodes goed uit te lezen.
    """
    structure = pd.DataFrame()
    structure_lines = getattr(threedi_result.lines, structure_name)

    # Add identifying codes to structs
    add_code(
        table=structure, structure_lines=structure_lines, structure_name=structure_name
    )

    # get geometry from 3di results and convert to shapely LineString
    lines_geometry = hrt.threedi.line_geometries_to_coords(
        structure_lines.line_geometries
    )

    # De 3di-id van het structure
    structure[map_id_col] = structure_lines.content_pk.tolist()

    # Zoom category (indicatie primair)
    structure[zoom_cat_col] = structure_lines.zoom_category.tolist()
    up_waterlevel, down_waterlevel = add_waterlevel_info(
        structure=structure,
        structure_lines=structure_lines,
        wtrlvl_nodes_at_timesteps=wtrlvl_nodes_at_timesteps,
        t_end=t_end,
    )
    # Add information about primary or not
    check_primary(
        structure=structure,
        structure_lines=structure_lines,
        structure_name=structure_name,
        primary_nodes=primary_nodes,
    )

    structure = hrt.df_add_geometry_to_gdf(df=structure, geometry_col=lines_geometry)

    # Add information about 'verhang'
    add_slope_info(structure, structure_name, up_waterlevel, down_waterlevel)
    return structure


def get_nodes_1d(result, T_0, T_end):
    """
    Creates dataframe with water level on timestamp and corresponding connection node
    """
    id_1d_nodes = result.nodes.subset(
        all_1d
    ).id.tolist()  # De content_pk van de 1d nodes.

    waterlevel_1d_nodes_t0 = (
        result.nodes.subset(all_1d).timeseries(indexes=[T_0]).s1.tolist()[0]
    )
    waterlevel_1d_nodes_t_end_rain = (
        result.nodes.subset(all_1d).timeseries(indexes=[T_end]).s1.tolist()[0]
    )

    # Create dataframe
    nodes_1d = pd.DataFrame(waterlevel_1d_nodes_t0, columns=[waterlevel_t_0_col])
    nodes_1d[waterlevel_t_end_col] = waterlevel_1d_nodes_t_end_rain
    nodes_1d[index_col] = nodes_1d.index
    nodes_1d[id_col] = id_1d_nodes

    nodes_1d = nodes_1d.round(5)
    return nodes_1d
