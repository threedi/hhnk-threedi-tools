# %%
import os
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from hhnk_threedi_tools import Folders

# --------------------------------------------------
# Plot configuration
# --------------------------------------------------

COLOR_IN = "royalblue"
COLOR_OUT = "firebrick"
COLOR_LOSS = "sienna"
COLOR_LATERAL = "seagreen"
COLOR_MIXED = "slategray"
COLOR_STORAGE = "darkorange"

PAIR_COMPONENTS = [
    (
        "1D",
        "1d_in",
        "1d_out",
    ),
    (
        "2D",
        "2d_in",
        "2d_out",
    ),
    (
        "1D boundary",
        "1d_bound_in",
        "1d_bound_out",
    ),
    (
        "2D boundary",
        "2d_bound_in",
        "2d_bound_out",
    ),
    (
        "1D-2D flow (1D)",
        "1d__1d_2d_flow_in",
        "1d__1d_2d_flow_out",
    ),
    (
        "1D-2D flow (2D)",
        "2d__1d_2d_flow_in",
        "2d__1d_2d_flow_out",
    ),
    (
        "1D-2D exchange (1D)",
        "1d__1d_2d_exch_in",
        "1d__1d_2d_exch_out",
    ),
    (
        "1D-2D exchange (2D)",
        "2d__1d_2d_exch_in",
        "2d__1d_2d_exch_out",
    ),
    (
        "Groundwater",
        "2d_groundwater_in",
        "2d_groundwater_out",
    ),
    (
        "Pump",
        "pump_in",
        "pump_out",
    ),
]


def _format_volume(value):
    """Format volume labels depending on magnitude."""

    value_abs = abs(value)

    if value_abs >= 1000:
        return f"{value:,.0f}"

    if value_abs >= 10:
        return f"{value:,.1f}"

    return f"{value:,.2f}"


labels = {
    "1d_in": "1D\nin",
    "1d_out": "1D\nout",
    "2d_in": "2D\nin",
    "2d_out": "2D\nout",
    "1d_bound_in": "1D boundary\nin",
    "1d_bound_out": "1D boundary\nout",
    "2d_bound_in": "2D boundary\nin",
    "2d_bound_out": "2D boundary\nout",
    "1d__1d_2d_flow_in": "1D-2D flow\nin",
    "1d__1d_2d_flow_out": "1D-2D flow\nout",
    "1d__1d_2d_exch_in": "1D-2D exchange\nin",
    "1d__1d_2d_exch_out": "1D-2D exchange\nout",
    "pump_in": "Pump\nin",
    "pump_out": "Pump\nout",
    "rain": "Rain",
    "infiltration_rate_simple": "Infiltration",
    "change_in_storage": "Change in\nstorage",
}


def plot_water_balance(csv_path, output_path):
    balance = pd.read_csv(
        csv_path,
        index_col="time_s",
    )

    components = [
        "rain",
        "infiltration_rate_simple",
        "1d_in",
        "1d_out",
        "2d_in",
        "2d_out",
        "2d__1d_2d_flow_out",
        "d_2d_vol",
        "d_1d_vol",
    ]

    data = balance[components]

    # Remove components that are completely zero
    data = data.loc[:, (data != 0).any(axis=0)]

    time_hours = data.index.to_numpy() / 3600

    fig, ax = plt.subplots(figsize=(12, 6))

    for column in data.columns:
        ax.plot(
            time_hours,
            data[column],
            label=column,
        )

    ax.axhline(0, linewidth=0.8)

    ax.set_xlabel("Time [h]")
    ax.set_ylabel("Flow [m³/s]")
    ax.set_title("Water balance")

    ax.legend(
        loc="upper left",
        bbox_to_anchor=(1.02, 1),
    )

    fig.tight_layout()

    fig.savefig(
        output_path,
        dpi=150,
        bbox_inches="tight",
    )

    plt.close(fig)


def plot_water_balance_volumes(
    csv_path,
    output_path,
):
    """Plot net water balance volumes [m³]."""

    csv_path = Path(csv_path)
    output_path = Path(output_path)

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    # --------------------------------------------------
    # Read volumes
    # --------------------------------------------------

    volumes = pd.read_csv(
        csv_path,
        index_col="component",
    )

    values = volumes["volume_m3"]

    # --------------------------------------------------
    # Change in storage
    # --------------------------------------------------

    change_in_storage = (
        values.get("d_2d_vol", 0.0) + values.get("d_1d_vol", 0.0) + values.get("d_2d_groundwater_vol", 0.0)
    )

    # --------------------------------------------------
    # Build plot data
    # --------------------------------------------------

    labels = []
    plot_values = []
    colors = []

    # Positions where a dotted line is added
    separators = []

    # ------------------------------
    # IN / OUT pairs
    # ------------------------------

    for group_name, component_in, component_out in PAIR_COMPONENTS:
        value_in = values.get(
            component_in,
            0.0,
        )

        value_out = values.get(
            component_out,
            0.0,
        )

        # Skip complete zero pairs
        if value_in == 0 and value_out == 0:
            continue

        labels.extend(
            [
                f"{group_name}\nin",
                f"{group_name}\nout",
            ]
        )

        plot_values.extend(
            [
                value_in,
                value_out,
            ]
        )

        colors.extend(
            [
                COLOR_IN,
                COLOR_OUT,
            ]
        )

        # Separator after every pair
        separators.append(len(plot_values) - 0.5)

    # --------------------------------------------------
    # Other components
    # --------------------------------------------------

    other_components = [
        (
            "Rain on 2D",
            values.get("rain", 0.0),
            COLOR_IN,
        ),
        (
            "Rain on 1D",
            values.get("inflow", 0.0),
            COLOR_IN,
        ),
        (
            "Infiltration",
            values.get(
                "infiltration_rate_simple",
                0.0,
            ),
            COLOR_LOSS,
        ),
        (
            "Lateral flow 1D",
            values.get("lat_1d", 0.0),
            None,
        ),
        (
            "Lateral flow 2D",
            values.get("lat_2d", 0.0),
            None,
        ),
        (
            "Surface sources\nand sinks",
            values.get("q_sss", 0.0),
            COLOR_MIXED,
        ),
    ]

    for label, value, color in other_components:
        if value == 0:
            continue

        # Signed components get color based on direction
        if color is None:
            if value >= 0:
                color = COLOR_LATERAL
            else:
                color = COLOR_OUT

        labels.append(label)
        plot_values.append(value)
        colors.append(color)

    # Storage is always shown
    labels.append("Change in\nstorage")

    plot_values.append(change_in_storage)

    colors.append(COLOR_STORAGE)

    # --------------------------------------------------
    # Create plot
    # --------------------------------------------------

    fig, ax = plt.subplots(
        figsize=(16, 8),
    )

    x = range(len(plot_values))

    bars = ax.bar(
        x,
        plot_values,
        color=colors,
        edgecolor="black",
        linewidth=0.8,
        width=0.75,
    )

    # --------------------------------------------------
    # Zero line
    # --------------------------------------------------

    ax.axhline(
        0,
        color="black",
        linewidth=1.0,
    )

    # --------------------------------------------------
    # Pair separators
    # --------------------------------------------------

    # Do not draw separator after the final pair
    # if other components follow immediately.
    for position in separators:
        ax.axvline(
            position,
            color="gray",
            linestyle=":",
            linewidth=0.8,
            alpha=0.5,
        )

    # --------------------------------------------------
    # Axis labels
    # --------------------------------------------------

    ax.set_xticks(range(len(labels)))

    ax.set_xticklabels(
        labels,
        rotation=25,
        ha="right",
    )

    ax.set_ylabel("Volume [m³]")

    ax.set_xlabel("")

    ax.set_title(
        "Net water balance",
        fontsize=14,
    )

    # --------------------------------------------------
    # Grid
    # --------------------------------------------------

    ax.grid(
        axis="y",
        linestyle="--",
        linewidth=0.7,
        alpha=0.3,
    )

    ax.set_axisbelow(True)

    # --------------------------------------------------
    # Y limits
    # --------------------------------------------------

    min_value = min(
        min(plot_values),
        0,
    )

    max_value = max(
        max(plot_values),
        0,
    )

    data_range = max_value - min_value

    if data_range == 0:
        data_range = 1

    margin = data_range * 0.12

    ax.set_ylim(
        min_value - margin,
        max_value + margin,
    )

    # --------------------------------------------------
    # Value labels
    # --------------------------------------------------

    label_offset = margin * 0.08

    for bar, value in zip(
        bars,
        plot_values,
    ):
        x_position = bar.get_x() + bar.get_width() / 2

        if value >= 0:
            y_position = value + label_offset

            vertical_alignment = "bottom"

        else:
            y_position = value - label_offset

            vertical_alignment = "top"

        ax.text(
            x_position,
            y_position,
            _format_volume(value),
            ha="center",
            va=vertical_alignment,
            fontsize=9,
            fontweight="bold",
        )

    # --------------------------------------------------
    # Save
    # --------------------------------------------------

    fig.tight_layout()

    fig.savefig(
        output_path,
        dpi=150,
        bbox_inches="tight",
    )

    plt.close(fig)


# %%


paths = (
    r"H:\02.modellen\bergen_noord_huidig_situatie_JA",
    r"H:\02.modellen\bergen_noord_variant_1_JA",
    r"H:\02.modellen\bergen_noord_variant_2_JA",
    r"H:\02.modellen\bergen_noord_variant_3_JA",
)

for model in paths:
    folder = Folders(model)
    batch_path = folder.threedi_results.batch.path
    batch_folders = os.listdir(batch_path)
    for results in batch_folders:
        output_raster_path = batch_path / results / "02_output_rasters"
        downloads = os.listdir(output_raster_path)
        for download in downloads:
            plot_water_balance_folder = output_raster_path / download / f"waterbalance_{download}"
            csv_water_balance_timeseries = plot_water_balance_folder / "water_balance_timeseries.csv"
            csv_water_balance_volumen = plot_water_balance_folder / "water_balance_volumes.csv"
            output_path_timeseries = plot_water_balance_folder / "water_balance_timeseries.png"
            output_path_net = plot_water_balance_folder / "water_balance_volume.png"
            # if os.path.exists(output_path_timeseries):
            #     continue
            plot_water_balance(csv_water_balance_timeseries, output_path_timeseries)
            plot_water_balance_volumes(csv_water_balance_volumen, output_path_net)
