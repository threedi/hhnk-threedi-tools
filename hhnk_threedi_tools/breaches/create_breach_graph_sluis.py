import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.lines import Line2D
from matplotlib.ticker import MultipleLocator


def plot_breach_simulation(
    waterlevel_path,
    velocity_path,
    discharge_path,
    breach_path,
    x_end=120,
    save_path=None,
):
    """
    Plot time series from a breach simulation.

    Reads CSV outputs (water levels, velocity, discharge and breach data),
    converts values to numeric, aligns time series and generates a 4-panel plot:
    water levels, velocities, discharges and breach width.

    It also detects when the sluice is active (sea < lake) and marks ON/OFF
    transitions with vertical lines.

    Parameters
    ----------
    waterlevel_path : str
        Path to waterlevel_seg.csv
    velocity_path : str
        Path to velocity_seg.csv
    discharge_path : str
        Path to discharge_seg.csv
    breach_path : str
        Path to breach_data.csv
    x_end : float, optional
        Max time (hours) to show on x-axis (default = 120)
    save_path : str, optional
        If provided, saves the figure to this path
    """

    def to_float(series):
        """Convert string column with comma decimals to float."""
        s = series.astype(str).str.strip().str.replace(",", ".", regex=False)
        return pd.to_numeric(s, errors="coerce")

    # Read input CSV files
    waterlevel_df = pd.read_csv(waterlevel_path, sep=",", decimal=".")
    velocity_df = pd.read_csv(velocity_path, sep=",", decimal=".")
    discharge_df = pd.read_csv(discharge_path, sep=",", decimal=".")
    breach_df = pd.read_csv(breach_path, sep=";")

    # Convert time from seconds to hours
    breach_df["time_sec"] = to_float(breach_df["time_sec"])
    time_hr = breach_df["time_sec"].to_numpy() / 3600.0

    # Extract breach properties
    breach_width = to_float(breach_df["breach_width"]).to_numpy()
    breach_depth = to_float(breach_df["breach_depth"]).to_numpy()
    max_breach_depth = np.nanmax(breach_depth)

    # Extract water levels
    up = to_float(waterlevel_df["upstream_waterlevel"]).to_numpy()
    down = to_float(waterlevel_df["downstream_waterlevel"]).to_numpy()
    sea = to_float(waterlevel_df["waterlevel_sea"]).to_numpy()

    # Extract velocities
    v_bres = to_float(velocity_df["breach_velocity"]).to_numpy()
    v_sluis = to_float(velocity_df["sluis_velocity"]).to_numpy()

    # Extract discharges
    q_bres = to_float(discharge_df["breach_discharge"]).to_numpy()
    q_sluis = to_float(discharge_df["sluis_discharge"]).to_numpy()

    # Ensure all series have same length
    series_list = [time_hr, up, down, sea, v_bres, v_sluis, q_bres, q_sluis, breach_width]
    n = min(len(s) for s in series_list)

    t = time_hr[:n]
    up, down, sea = up[:n], down[:n], sea[:n]
    v_bres, v_sluis = v_bres[:n], v_sluis[:n]
    q_bres, q_sluis = q_bres[:n], q_sluis[:n]
    b_w = breach_width[:n]

    # Detect sluice ON when sea level is lower than lake level
    sluis_on = sea < up

    # Find transition points (OFF→ON and ON→OFF)
    start_idx = np.where(~sluis_on[:-1] & sluis_on[1:])[0] + 1
    stop_idx = np.where(sluis_on[:-1] & ~sluis_on[1:])[0] + 1

    if sluis_on[0]:
        start_idx = np.r_[0, start_idx]
    if sluis_on[-1]:
        stop_idx = np.r_[stop_idx, len(sluis_on) - 1]

    def edge_times_midpoint(t_arr, idx_arr):
        """Place transition markers between time steps."""
        out = []
        for i in idx_arr:
            if i <= 0:
                out.append(t_arr[0])
            else:
                out.append(0.5 * (t_arr[i - 1] + t_arr[i]))
        return np.asarray(out)

    start_times = edge_times_midpoint(t, start_idx)
    stop_times = edge_times_midpoint(t, stop_idx)

    # Create figure with 4 panels
    fig, axes = plt.subplots(4, 1, sharex=True, figsize=(12, 14))
    fig.suptitle(
        f"IJsselmeer-bres simulatie\nMax bres diepte: {max_breach_depth:.2f} m",
        fontsize=15,
    )

    # Water levels
    axes[0].plot(t, up, label="Meer", linewidth=1.8)
    axes[0].plot(t, down, label="Polder", linewidth=1.8)
    axes[0].plot(t, sea, label="Zee", linewidth=1.8)
    axes[0].set_title("Waterstanden")
    axes[0].set_ylabel("m")
    axes[0].grid(True)

    # Velocities
    axes[1].plot(t, v_bres, label="Bres", linewidth=1.8)
    axes[1].plot(t, v_sluis, label="Sluis", linewidth=1.8)
    axes[1].set_title("Snelheid")
    axes[1].set_ylabel("m/s")
    axes[1].grid(True)

    # Discharges
    axes[2].plot(t, q_bres, label="Bres", linewidth=1.8)
    axes[2].plot(t, q_sluis, label="Sluis", linewidth=1.8)
    axes[2].set_title("Debiet")
    axes[2].set_ylabel("m³/s")
    axes[2].grid(True)

    # Breach width
    axes[3].plot(t, b_w, label="Bresbreedte", color="purple", linewidth=1.8)
    axes[3].set_title("Bresbreedte")
    axes[3].set_xlabel("Tijd (uur)")
    axes[3].set_ylabel("m")
    axes[3].grid(True)

    # Add vertical lines for sluice transitions
    start_proxy = Line2D([0], [0], color="green", linestyle="--", label="Sluis aan")
    stop_proxy = Line2D([0], [0], color="red", linestyle=":", label="Sluis uit")

    for ax in axes:
        for x in start_times:
            ax.axvline(x, color="green", linestyle="--", alpha=0.8)
        for x in stop_times:
            ax.axvline(x, color="red", linestyle=":", alpha=0.8)

        handles, labels = ax.get_legend_handles_labels()
        ax.legend(handles + [start_proxy, stop_proxy])

        ax.set_xlim(0, x_end)
        ax.xaxis.set_major_locator(MultipleLocator(24))
        ax.xaxis.set_minor_locator(MultipleLocator(12))

    plt.tight_layout(rect=[0, 0, 1, 0.96])

    # Save figure if path is provided
    if save_path:
        plt.savefig(save_path, dpi=300)

    plt.show()
