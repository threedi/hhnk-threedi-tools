import numpy as np
import pandas as pd
from threedigrid.admin.gridresultadmin import GridH5ResultAdmin


def calculate_rain_days(rain):
    """
    Calculates days dry before and after rain
    """
    detected_rain = [i for i, e in enumerate(rain) if e > 1e-5]
    # Collect indexes of items in rain where rain falls (every index represents an hour)
    if detected_rain:
        # Detected rain[0] is the first index where rain occurs, so the last dry
        # item is one before that. Dividing by 24 converts to days
        dry_days_start = max(0, (detected_rain[0] - 1) / 24)
        # Detected rain[-1] is the first index where rain occurs, so the last dry
        # item is one after that. Dividing by 24 converts to days
        dry_days_end = max(0, (len(rain) - detected_rain[-1] - 1) / 24)
        return detected_rain, dry_days_start, dry_days_end
    else:
        raise Exception(f"Geen regen gedetecteerd in 3di scenario")


def get_rain_properties(results):
    """
    Calculates the rain scenario used for this result
    """
    try:
        # Calculates the mean of steps between timestamps (in seconds), then converts to minutes
        dt = round(np.mean(np.diff(results.nodes.timestamps)) / 60, 0)
        # Timestep is list of time passed between timestamp and start in minutes
        timestep = results.nodes.timestamps / 60
        # Calculates rain per node between 0 and end of scenario at every step of size dt / 60 (so every hour)
        rain_1d_list = (
            results.nodes.subset("1D_ALL")
            .timeseries(indexes=slice(0, timestep.size, int(60 / dt)))
            .rain.tolist()
        )
        rain_2d_list = (
            results.nodes.subset("2D_ALL")
            .timeseries(indexes=slice(0, timestep.size, int(60 / dt)))
            .rain.tolist()
        )
        # Blijft raar in sublijsten staan ookal lezen we maar 1 node uit, dit haalt dat weg om er 1 list van te maken
        # We pick the index of the first node in the list of rain
        rain_1d = [x[0] for x in rain_1d_list]
        i = 0
        # if the first node we picked has no rain, we try others until we find one that does
        while ((not any(rain_1d)) and (i < len(rain_1d_list))) or (np.all(np.array(rain_1d) <= 1e-5)):
            rain_1d = [x[i] for x in rain_1d_list]
            i += 1

            if i == len(rain_1d_list[0]):
                rain_1d = []
                break

        # Check if there is 2d rain info
        try:
            rain_2d = [x[0] for x in rain_2d_list]
        except:
            rain_2d = [0]
        if any(rain_1d):
            rain = rain_1d
        elif any(rain_2d):
            rain = rain_2d
        else:
            raise Exception(f"Geen regen gedetecteerd in 3di scenario")
        return rain, dt, timestep
    except Exception as e:
        raise e from None


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
    T0_values = np.argmax(timestep > 0)  # 0
    # Index of timestep where rain starts
    T_rain_start_values = np.argmax(
        timestep > days_dry_start * 24 * 60
    )  # int(days_dry_start * 4 * 24)
    # Index of timestep one day before end of rain
    T_rain_end_min_one_values = (
        np.argmax(timestep > timestep[-1] - days_dry_end * 24 * 60 - 24 * 60) - 1
    )  # int((len(timestep) - 1) - (days_dry_end + 1) * 4 * 24)
    # Index of timestep end rain
    T_rain_end_values = (
        np.argmax(timestep > timestep[-1] - days_dry_end * 24 * 60) - 1
    )  # int((len(timestep) - 1) - days_dry_end * 4 * 24)
    # Last index of timestep
    T_end_sum_values = np.argmax(timestep == timestep[-1])  # len(timestep) - 1

    timesteps_df_columns = [
        "t_0",
        "t_start_rain",
        "t_end_rain_min_one",
        "t_end_rain",
        "t_end_sum",
    ]
    timesteps_df_values = [
        T0_values,
        T_rain_start_values,
        T_rain_end_min_one_values,
        T_rain_end_values,
        T_end_sum_values,
    ]
    timesteps_dataframe = pd.DataFrame(
        data=[timesteps_df_values], columns=timesteps_df_columns, index=["value"]
    )
    return timesteps_dataframe


def construct_scenario(grid_result:GridH5ResultAdmin):
    """Get scenario properties from threedi result."""
    try:
        rain, dt, timestep = get_rain_properties(grid_result)
        detected_rain, days_dry_start, days_dry_end = calculate_rain_days(rain)
        timesteps_dataframe = create_results_dataframe(
            timestep, days_dry_start, days_dry_end
        )
        return (
            rain,
            detected_rain,
            timestep,
            days_dry_start,
            days_dry_end,
            timesteps_dataframe,
        )
    except Exception as e:
        raise e from None
