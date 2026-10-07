# %%
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from osgeo import ogr


def update_field(
    model_path: str | Path,
    layer_name: str,
    selection_field: str,
    field_name: str,
    values: dict,
) -> None:
    """Update a field for selected features without rewriting the layer."""

    ds = ogr.Open(str(model_path), update=1)
    layer = ds.GetLayerByName(layer_name)

    layer.StartTransaction()

    for feature in layer:
        feature_id = feature.GetField(selection_field)

        if feature_id in values:
            feature.SetField(
                field_name,
                values[feature_id],
            )
            layer.SetFeature(feature)

    layer.CommitTransaction()

    layer = None
    ds = None


def breach_data_to_timeseries(breach_data_pd: pd.DataFrame) -> str:
    """Convert breach data to a 3Di timeseries string."""

    data = breach_data_pd.loc[
        breach_data_pd["time_sec"] > 0,
        ["time_sec", "breach_wlev_upstream"],
    ].copy()

    # Start the timeseries at 0 minutes
    time_min = (data["time_sec"] - data["time_sec"].iloc[0]) / 60
    #Add last value to the timeseries to ensure it reaches the last time step
    last_time = round(data["time_sec"].iloc[-1]/60, 2)
    time_min.loc[len(time_min)] = last_time

    #Star de waterlevel
    waterlevel = round(data["breach_wlev_upstream"], 2)
    #Add last value to the timeseries to ensure it reaches the last waterlevel value
    last_waterlevel = round(data["breach_wlev_upstream"].iloc[-1], 2)
    waterlevel.loc[len(waterlevel)] = last_waterlevel

    # Interpolate from 15-minute to 10-minute intervals
    new_time_min = np.arange(0, time_min.iloc[-1], 10)

    new_waterlevel = np.interp(
        new_time_min,
        time_min,
        waterlevel,
    )

    rows = []

    for time, wl in zip(new_time_min, new_waterlevel):
        rows.append(f"{int(time)},{wl:.2f}")

    timeseries = "\n".join(rows)

    return timeseries


def update_waterlevel(
    time_series_old: str,
    new_max_waterlevel: float,
    last_time_series: int,
) -> str:
    """Return a timeseries with its maximum adjusted to a new water level."""

    rows = []

    # Convert the multiline string into a list of lines.
    time_series = time_series_old.strip().splitlines()

    # Read the original maximum water level from the first line.
    max_waterlevel_old = max(float(value.split(",")[1]) for value in time_series)

    # Shift required to reach the new maximum water level.
    delta = max_waterlevel_old - new_max_waterlevel

    # Apply the same shift to the complete timeseries.
    for value in time_series:
        time = float(value.split(",")[0])
        waterlevel_old = float(value.split(",")[1])

        new_waterlevel = round(waterlevel_old - delta, 2)

        rows.append((time, new_waterlevel))

    last_time_series = last_time_series * 24 * 60
    if time != last_time_series:
        if time < last_time_series:
              # Convert days to minutes
            rows.append((last_time_series, new_waterlevel))

    return "\n".join(f"{time:g},{waterlevel:.6f}" for time, waterlevel in rows)


# %%
if __name__ == "__main__":
    breach_data_pd = pd.read_csv(
        r"H:\03.resultaten\Overstromingsberekeningenprimairedoorbraken2024\output\ROR_PRI-dijktrajecten_12-1_12-2_13-6_13-7_Deel_Zuid\ROR-PRI-OOSTERDIJK_VAN_DRECHTERLAND_0.5-T100000\ROR-PRI-OOSTERDIJK_VAN_DRECHTERLAND_0.5-T100000data.csv",
        sep=";",
        decimal=",",
    )

    # This is the OLD but correct timeseries shape
    time_series_old = breach_data_to_timeseries(breach_data_pd)

    model_path = r"H:\02.modellen\ROR PRI - dijktrajecten 13-8 en 13-9 - Stroom_NO\work in progress\schematisation\ROR PRI - dijktrajecten 13-8 en 13-9 - Stroom_NO.gpkg"

    layer_name = "boundary_condition_1d"

    boundary_condition_gdf = gpd.read_file(
        model_path,
        layer=layer_name,
        driver="GPKG",
    )

    # Boundary condition field = connection_node_id and new maximum water levels.
    new_max_waterlevels = {
        20912: 1.3,  # connection_node_id 20912
        # 21029: 0.9,  # connection_node_id 262
        # 20947: 0.9,  # connection_node_id 276
        # 20942: 0.9,  # connection_node_id 275
        # 20903: 0.9,  # connection_node_id 261
        # 290: 0.90,
    }

    updated_timeseries = {}

    #
    last_time_series = 20  # days
    # Create the new timeseries for each boundary condition.
    for bc_id, new_waterlevel in new_max_waterlevels.items():
        new_timeseries = update_waterlevel(
            time_series_old=time_series_old, new_max_waterlevel=new_waterlevel, last_time_series=last_time_series
        )

        updated_timeseries[bc_id] = new_timeseries
    # %%
    # Update only the timeseries field.
    update_field(
        model_path=model_path,
        layer_name=layer_name,
        selection_field="connection_node_id",
        field_name="timeseries",
        values=updated_timeseries,
    )

# %%
