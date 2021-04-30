import pandas as pd
import numpy as np
from .variables.rain_dataframe import t_0_col, t_start_rain_col, t_end_rain_min_one_col, t_end_sum_col, \
    t_end_rain_col, t_index_col

def create_results_dataframe(timestep, days_dry_start, days_dry_end):
    """

        create_results_dataframe(timestep   (list of time passed between timestamp and start sum in minutes
                                            (size of slice is constant)),
                                 days_dry_start (number of days before rain),
                                 days_dry_end   (number of dry days after rain))

    Return value: dataframe with timesteps as columns and indexes of timesteps as values

    Calculates the indexes of relevant indexes (start sum, start rain, one day before end rain,
    end rain, end sum)
    """
    # Index in timestep for start sum
    T0_values = np.argmax(timestep > 0) # 0
    # Index of timestep where rain starts
    T_rain_start_values = np.argmax(timestep > days_dry_start * 24 * 60) #int(days_dry_start * 4 * 24)
    # Index of timestep one day before end of rain
    T_rain_end_min_one_values = np.argmax(timestep > timestep[-1] - days_dry_end * 24 * 60 - 24 * 60) - 1 #int((len(timestep) - 1) - (days_dry_end + 1) * 4 * 24)
    # Index of timestep end rain
    T_rain_end_values = np.argmax(timestep > timestep[-1] - days_dry_end * 24 * 60) - 1 #int((len(timestep) - 1) - days_dry_end * 4 * 24)
    # Last index of timestep
    T_end_sum_values = np.argmax(timestep == timestep[-1]) #len(timestep) - 1
    timesteps_df_columns = [t_0_col, t_start_rain_col, t_end_rain_min_one_col, t_end_rain_col, t_end_sum_col]
    timesteps_df_values = [T0_values, T_rain_start_values, T_rain_end_min_one_values,
                           T_rain_end_values, T_end_sum_values]
    timesteps_dataframe = pd.DataFrame(data=[timesteps_df_values], columns=timesteps_df_columns, index=[t_index_col])
    return timesteps_dataframe
