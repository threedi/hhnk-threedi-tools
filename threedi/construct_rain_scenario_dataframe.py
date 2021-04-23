import pandas as pd
import numpy as np
from .variables.rain_dataframe import t_0_col, t_start_rain_col, t_end_rain_min_one_col, t_end_sum_col, \
    t_end_rain_col, t_index_col

def create_results_dataframe(timestep, days_dry_start, days_dry_end):
    """
    Calculates the indexes of the timesteps we want and then saves
    corresponding values to dataframe
    """
    T0_values = np.argmax(timestep > 0)
    T_rain_start_values = np.argmax(timestep > days_dry_start * 24 * 60)
    # One step before the end
    T_rain_end_min_one_values = np.argmax(timestep > timestep[-1] - days_dry_end * 24 * 60 - 24 * 60) - 1
    T_rain_end_values = np.argmax(timestep > timestep[-1] - days_dry_end * 24 * 60) - 1
    T_end_sum_values = np.argmax(timestep == timestep[-1])
    timesteps_df_columns = [t_0_col, t_start_rain_col, t_end_rain_min_one_col, t_end_rain_col, t_end_sum_col]
    timesteps_df_values = [T0_values, T_rain_start_values, T_rain_end_min_one_values,
                           T_rain_end_values, T_end_sum_values]
    timesteps_dataframe = pd.DataFrame(data=[timesteps_df_values], columns=timesteps_df_columns, index=[t_index_col])
    return timesteps_dataframe
