import pandas as pd
from hhnk_threedi_tools.dataframe_functions.conversion import create_gdf_from_df
from hhnk_threedi_tools.threedi.geometry_functions.node_coordinates_to_points import coordinates_to_points
from hhnk_threedi_tools.threedi.variables.rain_dataframe import t_end_sum_col, t_end_rain_col, t_end_rain_min_one_col, \
    t_0_col, t_start_rain_col
from hhnk_threedi_tools.threedi.variables.gridadmin import all_1d
from .variables.dataframe_mapping import lvl_start_col, lvl_end_col, lvl_end_rain_col, lvl_rain_col

def run_0d1d_test(test_env):
    """
    Gathers information about water levels at several time steps in the
    scenario
    """
    timesteps_df = test_env.threedi_vars.scenario_df
    cols = [t_end_sum_col, t_end_rain_col, t_end_rain_min_one_col,
            t_start_rain_col, t_0_col]
    try:
        # Get subset of nodes (1D)
        subset_1d = test_env.threedi_vars.result.nodes.subset(all_1d)

        # Create waterlevels dataframe
        waterlevel_lst = subset_1d.timeseries(indexes=list(timesteps_df.values[0])).s1
        waterlevel_df = pd.DataFrame(waterlevel_lst.T, columns=timesteps_df.columns)

        # Gather information into dataframe
        results_df = waterlevel_df[cols].copy()
        results_df.rename(columns={t_start_rain_col: f'wlvl_{t_start_rain_col}',
                                   t_0_col: f'wlvlv_{t_0_col}',
                                   t_end_rain_min_one_col: f'wvlv_{t_end_rain_min_one_col}',
                                   t_end_rain_col: f'wlvlv_{t_end_rain_col}',
                                   t_end_sum_col: f'wlvl_{t_end_sum_col}'},
                          inplace=True)
        # Peilverschillen tussen aantal timesteps
        # verschil in waterstand van start regen tov start berekening
        results_df.insert(0, lvl_start_col, waterlevel_df[t_start_rain_col] - waterlevel_df[t_0_col])
        # peilstijging tijdens neerslagperiode
        results_df.insert(0, lvl_rain_col, waterlevel_df[t_end_rain_col] - waterlevel_df[t_start_rain_col])
        # check of er een evenwicht is aan het einde van de neerslagperiode
        results_df.insert(0, lvl_end_rain_col, waterlevel_df[t_end_rain_col] - waterlevel_df[t_end_rain_min_one_col])
        # keert de waterstand weer terug naar peil begin regen
        results_df.insert(0, lvl_end_col, waterlevel_df[t_end_sum_col] - waterlevel_df[t_0_col])

        # Add timesteps
        for col in cols:
            results_df.insert(results_df.shape[1], col, timesteps_df[col].values[0])

        # Get point geometry from nodes
        crds = coordinates_to_points(nodes=subset_1d)
        results_gdf = create_gdf_from_df(df=results_df, geometry_col=crds)
        return results_gdf
    except Exception as e:
        raise e from None
