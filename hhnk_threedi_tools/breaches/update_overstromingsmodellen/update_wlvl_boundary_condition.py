# %%
import geopandas as gpd


# change time series of boundary condtions with new max water levels. Keep the same gradient.
def update_waterlevel(time_series_old: str, new_max_waterlevel: float) -> str:
    """Return a timeseries string with its peak adjusted to a new max.

    `time_series_old` should be lines of `time,waterlevel`. The function
    preserves the relative differences (gradient) and shifts values so the
    original maximum becomes `new_max_waterlevel`. Returns a formatted
    timeseries string with six-decimal waterlevels.
    """
    rows = []

    # convert the input multiline string into a list of lines
    time_series = time_series_old.strip().splitlines()

    # read the original maximum water level from the first line
    max_waterlevel_old = float(time_series[0].split(",")[1])

    # how much we must shift all values to reach the requested maximum
    delta = max_waterlevel_old - new_max_waterlevel

    # update every line: parse time and old level, compute shifted level
    for value in time_series:
        time = float(value.split(",")[0])
        waterlevel_old = float(value.split(",")[1])
        new_waterlevel = waterlevel_old - delta
        rows.append((time, new_waterlevel))

    # return as newline-separated time,waterlevel with 6 decimals
    format_3di = "\n".join(f"{time:g},{wl:.6f}" for time, wl in rows)
    return format_3di


if __name__ == "__main__":
    model_path = r"Y:\02.modellen\RegionalFloodModel\work in progress\schematisation\RegionalFloodModel.gpkg"

    boundary_condition_gdf = gpd.read_file(model_path, layer="1d_boundary_condition", driver="GPKG")

    # boundary condition IDs and new maximum water levels
    new_max_waterlevels = {
        263: 1.30,
        283: 1.30,
        290: 0.90,
    }

    # loop over the new max water levels and update the time series of the boundary conditions
    for bc_id, new_waterlevel in new_max_waterlevels.items():
        # get the boundary condition with the specified id
        boundary_condition_select = boundary_condition_gdf[boundary_condition_gdf["id"] == bc_id]
        # select the old time series of the boundary condition
        time_series_old = boundary_condition_select["timeseries"].values[0]
        # get the new time series with the updated water level
        new_timeseries = update_waterlevel(time_series_old, new_waterlevel)
        # update the time series of the boundary condition with the new time series
        boundary_condition_gdf.loc[boundary_condition_gdf["id"] == bc_id, "timeseries"] = new_timeseries
    # save the updated boundary conditions to the model path
    boundary_condition_gdf.to_file(
        model_path,
        layer="1d_boundary_condition",
        driver="GPKG",
    )


# %%
