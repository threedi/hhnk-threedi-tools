import pandas as pd
from ...threedi.variables.rain_dataframe import t_index_col, t_0_col, t_end_rain_col
from ...wsa.conversion_functions import line_geometries_to_coords
from ...dataframe_functions.conversion import create_gdf_from_df
from ...threedi.variables.gridadmin import all_1d
from .variables.dataframe_mapping import res_orifices, res_culverts, res_channels, start_node_col, end_node_col, \
    code_col, flow_direction_col, slope_col, water_lvl_diff_col, q_col, u_var_col, slope_abs_cm_km_col, map_id_col, \
    zoom_cat_col, upstream_id_col, downstream_id_col, waterlevel_up_end_col, waterlevel_down_end_col, \
    waterlevel_up_start_col, waterlevel_down_start_col, waterlevel_diff_abs_m_col, struct_on_lvl_limit_col, id_col, \
    index_col, primary_col, waterlevel_t_end_col, waterlevel_t_0_col

def add_code(table, structure_lines, structure_name):
    # Conversion to string is because otherwise these are objects, which isn't valid for geopackage format
    # Ophalen code die in damo staat (dit werkt nog niet lekker in de grid)
    if structure_name != res_orifices:  # Voor orifices staat dit niet in de code, maar in de display_name
        table[code_col] = structure_lines.code.astype('U13')
    else:
        table[code_col] = structure_lines.display_name.astype('U13')

def add_waterlevel_info(structure, structure_lines, wtrlvl_nodes_at_timesteps, t_end):
    # Bepalen waterstanden -----------------------------------------------------------
    # Upstream en downstream id van de connection nodes van het structure
    up_ids = list(structure_lines.line_nodes[:, 0])
    down_ids = list(structure_lines.line_nodes[:, 1])  # Downstream id van de connection nodes

    # Get upstream and downstream id per structure
    ids_df = pd.DataFrame(up_ids, columns=[upstream_id_col])
    ids_df[downstream_id_col] = down_ids

    # Koppel deze aan de water levels die in de nodes_1d dataframe staan om de waterstanden te koppelen
    # Add water levels based on upstream and downstream id's
    up_waterlevel = pd.merge(ids_df, wtrlvl_nodes_at_timesteps, left_on=upstream_id_col, right_on=id_col, how='left')
    down_waterlevel = pd.merge(ids_df, wtrlvl_nodes_at_timesteps, left_on=downstream_id_col, right_on=id_col, how='left')

    # Zet deze informatie in de structuretabel
    structure[waterlevel_up_end_col] = up_waterlevel[waterlevel_t_end_col]  # Waterlevel_end
    structure[waterlevel_down_end_col] = down_waterlevel[waterlevel_t_end_col]

    structure[waterlevel_up_start_col] = up_waterlevel[waterlevel_t_0_col]
    structure[waterlevel_down_start_col] = down_waterlevel[waterlevel_t_0_col]

    # Water level difference in METER
    structure[water_lvl_diff_col] = [(u - d) for u, d in
                                     zip(structure[waterlevel_up_end_col], structure[waterlevel_down_end_col])]
    structure[q_col] = structure_lines.timeseries(indexes=[t_end]).q.tolist()[0]  # Discharge
    structure[u_var_col] = structure_lines.timeseries(indexes=[t_end]).u1.tolist()[0]  # Velocity
    return up_waterlevel, down_waterlevel

def check_primary(structure, structure_lines, structure_name, primary_nodes):
    """
    Based on what type of structure we are dealing with
    If channels, we check zoom cat
    If kunstwerk, we use the list of primary nodes (channels connection nodes)
    """
    structure[start_node_col] = structure_lines.line_nodes[:, 0]
    structure[end_node_col] = structure_lines.line_nodes[:, 1]  # Downstream id of connection nodes

    if structure_name == res_channels:
        # Classify parts of channels as primary if zoom category is 4
        structure[primary_col] = structure[zoom_cat_col].apply(lambda x: True if x == 4 else False)
    else:
        # We use nodes qualified as primary by above method for other structure types
        structure[primary_col] = structure.apply(
            lambda row: row[start_node_col] in primary_nodes and row[end_node_col] in primary_nodes, axis=1)

def add_slope_info(structure, structure_name, up_waterlevel, down_waterlevel):
    """
    Bepaal verhang bij kunstwerken
    """
    # Bepaal de richting van de stroming en neem absolute waarden aan voor afvoerwaarden
    structure[flow_direction_col] = structure[q_col].apply(lambda x: -1 if x < 0 else 1)

    # bereken het verhang over het structure
    structure[slope_col] = (structure[water_lvl_diff_col] * 100) / (
            structure.length.values * 0.001)  # CM/KM structure.length.values is de lengte van het segment

    # Absolute waarden meenemen
    structure[q_col] = structure[q_col].abs()  # Absoluut debiet, richting staat in structure['richting']
    structure[u_var_col] = structure[u_var_col].abs()  # Absolute snelheid
    structure[slope_abs_cm_km_col] = structure[slope_col].abs()  # structure.length.values is de lengte van het segment
    structure[waterlevel_diff_abs_m_col] = structure[water_lvl_diff_col].abs()

    # Bepalen of een structure op een peilgrens ligt, en daardoor is het verhang niet interessant.
    if structure_name in [res_orifices, res_culverts]:
        structure[struct_on_lvl_limit_col] = False
        # Dit testen we door te kijken of de bovenstroomse en benedenstrooms INITIELE waterstand gelijk zijn.
        structure.loc[(round(up_waterlevel[waterlevel_t_0_col], 2)) != (
            round(down_waterlevel[waterlevel_t_0_col], 2)), struct_on_lvl_limit_col] = True

def create_structure_gdf(threedi_result, structure_name, wtrlvl_nodes_at_timesteps, t_end, primary_nodes=[]):
    """Lees de netCDF uit voor de verschillende structure typen
        - channel
        - orifice
        - culvert
    TODO: Koppelen aan Sqlite om de structurecodes goed uit te lezen.
    """
    structure = pd.DataFrame()
    structure_lines = getattr(threedi_result.lines, structure_name)

    # Add identifying codes to structs
    add_code(table=structure,
             structure_lines=structure_lines,
             structure_name=structure_name)

    # get geometry from 3di results and convert to shapely LineString
    lines_geometry = line_geometries_to_coords(structure_lines.line_geometries)

    # De 3di-id van het structure
    structure[map_id_col] = structure_lines.content_pk.tolist()

    # Zoom category (indicatie primair)
    structure[zoom_cat_col] = structure_lines.zoom_category.tolist()
    up_waterlevel, down_waterlevel = add_waterlevel_info(structure=structure,
                                                         structure_lines=structure_lines,
                                                         wtrlvl_nodes_at_timesteps=wtrlvl_nodes_at_timesteps,
                                                         t_end=t_end)
    # Add information about primary or not
    check_primary(structure=structure,
                  structure_lines=structure_lines,
                  structure_name=structure_name,
                  primary_nodes=primary_nodes)

    structure = create_gdf_from_df(df=structure, geometry_col=lines_geometry)

    # Add information about 'verhang'
    add_slope_info(structure, structure_name, up_waterlevel, down_waterlevel)
    return structure

def get_nodes_1d(result, T_0, T_end):
    """
    Creates dataframe with water level on timestamp and corresponding connection node
    """
    id_1d_nodes = result.nodes.subset(all_1d).id.tolist()  # De content_pk van de 1d nodes.

    waterlevel_1d_nodes_t0 = result.nodes.subset(all_1d).timeseries(indexes=[T_0]).s1.tolist()[0]
    waterlevel_1d_nodes_t_end_rain = result.nodes.subset(all_1d).timeseries(indexes=[T_end]).s1.tolist()[0]

    # Create dataframe
    nodes_1d = pd.DataFrame(waterlevel_1d_nodes_t0, columns=[waterlevel_t_0_col])
    nodes_1d[waterlevel_t_end_col] = waterlevel_1d_nodes_t_end_rain
    nodes_1d[index_col] = nodes_1d.index
    nodes_1d[id_col] = id_1d_nodes

    nodes_1d = nodes_1d.round(5)
    return nodes_1d

def run_hydraulic_tests(test_env):
    try:
        T_0 = test_env.threedi_vars.scenario_df.loc[t_index_col, t_0_col]
        T_end = test_env.threedi_vars.scenario_df.loc[t_index_col, t_end_rain_col]

        wtrlvl_nodes_at_timesteps = get_nodes_1d(test_env.threedi_vars.result, T_0, T_end)

        channels_gdf = create_structure_gdf(threedi_result=test_env.threedi_vars.result,
                                            structure_name=res_channels,
                                            wtrlvl_nodes_at_timesteps=wtrlvl_nodes_at_timesteps,
                                            t_end=T_end)

        # Find all connection nodes from channels that are primary and combine in list without duplicates
        primary_nodes_series = channels_gdf.loc[channels_gdf.zoom_cat == 4, start_node_col].append(
            channels_gdf.loc[channels_gdf.zoom_cat == 4, end_node_col])
        primary_nodes = primary_nodes_series.unique().tolist()

        culvert_gdf = create_structure_gdf(threedi_result=test_env.threedi_vars.result,
                                           structure_name=res_culverts,
                                           wtrlvl_nodes_at_timesteps=wtrlvl_nodes_at_timesteps,
                                           t_end=T_end,
                                           primary_nodes=primary_nodes)
        orifice_gdf = create_structure_gdf(threedi_result=test_env.threedi_vars.result,
                                           structure_name=res_orifices,
                                           wtrlvl_nodes_at_timesteps=wtrlvl_nodes_at_timesteps,
                                           t_end=T_end,
                                           primary_nodes=primary_nodes)

        # combine orifices and culverts into one dataframe
        structures_gdf = pd.concat([orifice_gdf, culvert_gdf])
        return channels_gdf, structures_gdf
    except Exception as e:
        raise e from None
