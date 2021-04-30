import numpy as np
from threedigrid.admin.gridresultadmin import GridH5ResultAdmin
from .variables.gridadmin import all_1d, all_2d

def calculate_rain_days(rain):
    """
    Calculates days dry before and after rain
    """
    detected_rain = [i for i, e in enumerate(rain) if e > 0.00001]
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
    '''
    Calculates the rain scenario used for this result
    '''
    try:
        # Calculates the mean of steps between timestamps (in seconds), then converts to minutes
        dt = round(np.mean(np.diff(results.nodes.timestamps)) / 60, 0)
        # Timestep is list of time passed between timestamp and start in minutes
        timestep = results.nodes.timestamps / 60
        # Calculates rain per node between 0 and end of scenario at every step of size dt / 60 (so every hour)
        rain_1d_list = results.nodes.subset(all_1d).timeseries(
            indexes=slice(0, timestep.size, int(60 / dt))).rain.tolist()
        rain_2d_list = results.nodes.subset(all_2d).timeseries(
            indexes=slice(0, timestep.size, int(60 / dt))).rain.tolist()
        # Blijft raar in sublijsten staan ookal lezen we maar 1 node uit, dit haalt dat weg om er 1 list van te maken
        # We pick the index of the first node in the list of rain
        rain_1d = [x[0] for x in rain_1d_list]
        i = 0
        # if the first node we picked has no rain, we try others until we find one that does
        while (not any(rain_1d)) and (i < len(rain_1d_list)):
            rain_1d = [x[i] for x in rain_1d_list]
            i += 1
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

def construct_scenario(test_env):
    try:
        nc_file = test_env.src_paths['nc_file']
        h5_file = test_env.src_paths['h5_file']
        result = GridH5ResultAdmin(h5_file_path=h5_file,
                                   netcdf_file_path=nc_file)
        rain, dt, timestep = get_rain_properties(result)
        detected_rain, days_dry_start, days_dry_end = calculate_rain_days(rain)
        return result, rain, detected_rain, timestep, days_dry_start, days_dry_end
    except Exception as e:
        raise e from None
