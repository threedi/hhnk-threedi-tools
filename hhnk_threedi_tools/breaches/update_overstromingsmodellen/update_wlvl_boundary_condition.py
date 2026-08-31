# %%
import geopandas as gpd


# change time series of boundary condtions with new max water levels. Keep the same gradient.
def update_waterlevel(time_series_old, new_max_waterlevel):
    rows = []
    # change the format so it can be better processed.
    time_series = time_series_old.strip().splitlines()
    # get maximum water level from old time series
    max_waterlevel_old = float(time_series[0].split(",")[1])
    # calculate the difference between the old and new maximum water level
    delta = max_waterlevel_old - new_max_waterlevel
    # loop through the old time series and update the water level values
    for value in time_series:
        # split the time series value into time and water level
        time = float(value.split(",")[0])
        # get the old water level value
        waterlevel_old = float(value.split(",")[1])
        # caculate the new waterlevel  value.
        new_waterlevel = waterlevel_old - delta
        rows.append((time, new_waterlevel))
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
