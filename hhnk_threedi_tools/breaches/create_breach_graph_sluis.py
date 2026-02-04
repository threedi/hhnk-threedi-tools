# %%
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.lines import Line2D
from matplotlib.ticker import FormatStrFormatter, MultipleLocator

waterlevel = r"\\corp.hhnk.nl\data\Hydrologen_data\Data\03.resultaten\Overstromingsberekeningenprimairedoorbraken2024\output\test_waterbalance\ROR-PRI-test_T100000\01_NetCDF\waterlevel_seg.csv"
velocity = r"\\corp.hhnk.nl\data\Hydrologen_data\Data\03.resultaten\Overstromingsberekeningenprimairedoorbraken2024\output\test_waterbalance\ROR-PRI-test_T100000\01_NetCDF\velocity_seg.csv"
disharge = r"\\corp.hhnk.nl\data\Hydrologen_data\Data\03.resultaten\Overstromingsberekeningenprimairedoorbraken2024\output\test_waterbalance\ROR-PRI-test_T100000\01_NetCDF\discharge_seg.csv"
breach = r"\\corp.hhnk.nl\data\Hydrologen_data\Data\03.resultaten\Overstromingsberekeningenprimairedoorbraken2024\output\test_waterbalance\ROR-PRI-test_T100000\breach_data.csv"
# Disable scientific notation for all floats
pd.set_option("display.float_format", "{:.6f}".format)

waterlevel_df = pd.read_csv(waterlevel, sep=",", decimal=".")
velocity_df = pd.read_csv(velocity, sep=",", decimal=".")
discharge_df = pd.read_csv(disharge, sep=",", decimal=".")
breach_df = pd.read_csv(breach, sep=";")

# time (convert to numeric floats; invalid entries -> NaN)
time_sec = breach_df["time_sec"]
s = breach_df["time_sec"].astype(str).str.strip()
s = s.str.replace(",", ".", regex=False)
time_sec = pd.to_numeric(s, errors="coerce")

# width
breach_width_string = (breach_df["breach_width"]).to_list()
breach_width_string = breach_df["breach_width"].astype(str).str.strip()
breach_width_string = breach_width_string.str.replace(",", ".", regex=False)
breach_width = pd.to_numeric(breach_width_string, errors="coerce")

# waterlevel (ensure numeric floats)
waterlevel_labels = ["downstream_waterlevel", "upstream_waterlevel", "waterlevel_sea"]
for label in waterlevel_labels:
    s = waterlevel_df[label].astype(str).str.strip()
    s = s.str.replace(",", ".", regex=False)
    waterlevel_df[label] = pd.to_numeric(s, errors="coerce")

downstream_waterlevel = (waterlevel_df["downstream_waterlevel"]).to_list()
upstream_waterlevel = (waterlevel_df["upstream_waterlevel"]).to_list()
waterlevel_sea = (waterlevel_df["waterlevel_sea"]).to_list()

velocity_labels = ["breach_velocity", "sluis_velocity"]
for label in velocity_labels:
    s = velocity_df[label].astype(str).str.strip()
    s = s.str.replace(",", ".", regex=False)
    velocity_df[label] = pd.to_numeric(s, errors="coerce")
# velocity
velocity_breach = (velocity_df["breach_velocity"]).to_list()
sluis_velocity = (velocity_df["sluis_velocity"]).to_list()

discharge_labels = ["breach_discharge", "sluis_discharge"]
for label in discharge_labels:
    s = discharge_df[label].astype(str).str.strip()
    s = s.str.replace(",", ".", regex=False)
    discharge_df[label] = pd.to_numeric(s, errors="coerce")
# discharge
disharge_breach = (discharge_df["breach_discharge"]).to_list()
sluis_discharge = (discharge_df["sluis_discharge"]).to_list()

# %%
# 1) Time: ensure numeric + hours
breach_df["time_sec"] = breach_df["time_sec"].astype(str).str.strip().str.replace(",", ".", regex=False).astype(float)
time_hr = breach_df["time_sec"].to_numpy() / 3600.0


#  Align lengths (safe slicing)
series_list = [
    time_hr,
    upstream_waterlevel,
    downstream_waterlevel,
    waterlevel_sea,
    velocity_breach,
    sluis_velocity,
    disharge_breach,
    sluis_discharge,
    breach_width,
]
n = min(len(s) for s in series_list)

t = np.asarray(time_hr[:n])

up = np.asarray(upstream_waterlevel[:n])  # lake level (IJsselmeer)
down = np.asarray(downstream_waterlevel[:n])  # polder
sea = np.asarray(waterlevel_sea[:n])  # sea

v_bres = np.asarray(velocity_breach[:n])
v_sluis = np.asarray(sluis_velocity[:n])

q_bres = np.asarray(disharge_breach[:n])
q_sluis = np.asarray(sluis_discharge[:n])

b_w = np.asarray(breach_width[:n])

# 3) Detect sluis ON/OFF (ON when sea < lake)
#    + place markers at MIDPOINT between time steps (visual fix)
sluis_on = sea < up

start_idx = np.where(~sluis_on[:-1] & sluis_on[1:])[0] + 1  # OFF -> ON
stop_idx = np.where(sluis_on[:-1] & ~sluis_on[1:])[0] + 1  # ON -> OFF

# Include edges if it starts/ends ON
if sluis_on[0]:
    start_idx = np.r_[0, start_idx]
if sluis_on[-1]:
    stop_idx = np.r_[stop_idx, len(sluis_on) - 1]


def edge_times_midpoint(t_arr, idx_arr):
    """Return times located between samples (midpoints) for edge indices."""
    idx_arr = np.asarray(idx_arr, dtype=int)
    out = []
    for i in idx_arr:
        if i <= 0:
            out.append(t_arr[0])
        else:
            out.append(0.5 * (t_arr[i - 1] + t_arr[i]))
    return np.asarray(out)


start_times = edge_times_midpoint(t, start_idx)
stop_times = edge_times_midpoint(t, stop_idx)

# Styling choices
C_BRES = "tab:blue"  # polder + breach group
C_SLUIS = "tab:orange"  # sea + sluis group (NOT green)
C_MEER = "k"  # upstream lake neutral

LW = 1.8
TITLE_FS = 13
LABEL_FS = 12
SUPTITLE_FS = 15

# Plot: 4 panels
fig, axes = plt.subplots(4, 1, sharex=True, figsize=(12, 14))
fig.suptitle("IJsselmeer-bres simulatie: waterstanden, stroomsnelheden, debieten en bresbreedte", fontsize=SUPTITLE_FS)

# Panel 1: Water levels
ax = axes[0]
ax.plot(t, up, label="Waterstand meer (upstream)", color=C_MEER, linewidth=LW)
ax.plot(t, down, label="Waterstand polder (downstream)", color=C_BRES, linewidth=LW)
ax.plot(t, sea, label="Waterstand zee", color=C_SLUIS, linewidth=LW)
ax.set_title("Waterstand door de bres en de Waddenzee", fontsize=TITLE_FS)
ax.set_ylabel("Waterstand (m)", fontsize=LABEL_FS)
ax.grid(True)

# Panel 2: Velocities
ax = axes[1]
ax.plot(t, v_bres, label="Stroomsnelheid door de bres", color=C_BRES, linewidth=LW)
ax.plot(t, v_sluis, label="Stroomsnelheid door de sluis", color=C_SLUIS, linewidth=LW)
ax.set_title("Stroomsnelheid door de bres en door de sluis", fontsize=TITLE_FS)
ax.set_ylabel("Snelheid (m/s)", fontsize=LABEL_FS)
ax.grid(True)

# Panel 3: Discharge
ax = axes[2]
ax.plot(t, q_bres, label="Debiet door de bres", color=C_BRES, linewidth=LW)
ax.plot(t, q_sluis, label="Debiet door de sluis", color=C_SLUIS, linewidth=LW)
ax.set_title("Debiet door de bres en de sluis", fontsize=TITLE_FS)
ax.set_ylabel("Debiet (m³/s)", fontsize=LABEL_FS)
ax.grid(True)

# Panel 4: Breach width
ax = axes[3]
ax.plot(t, b_w, label="Bresbreedte", color="tab:purple", linewidth=LW)
ax.set_title("Breedte van de bres", fontsize=TITLE_FS)
ax.set_xlabel("Tijd (uur)", fontsize=LABEL_FS)
ax.set_ylabel("Breedte (m)", fontsize=LABEL_FS)
ax.grid(True)

# Tick label size
for ax in axes:
    ax.tick_params(axis="both", labelsize=11)

# Vertical markers: green = ON start, red = OFF stop
#    + legend labels with conditions (your request)
start_proxy = Line2D([0], [0], color="green", linestyle="--", linewidth=1.6, label="Sluis aan (zee < meer)")
stop_proxy = Line2D([0], [0], color="red", linestyle=":", linewidth=1.8, label="Sluis uit (zee ≥ meer)")

for ax in axes:
    for x in start_times:
        ax.axvline(x, color="green", linestyle="--", linewidth=1.6, alpha=0.9)
    for x in stop_times:
        ax.axvline(x, color="red", linestyle=":", linewidth=1.8, alpha=0.9)

    handles, labels = ax.get_legend_handles_labels()
    ax.legend(
        handles + [start_proxy, stop_proxy], labels + [start_proxy.get_label(), stop_proxy.get_label()], fontsize=10
    )

# X-axis: show time better (major every 24h, minor every 12h)
# Choose range:
# - To show only the first 120 hours:
x_end = 120
# - Or to show the whole simulation:
# x_end = float(np.nanmax(t))

for ax in axes:
    ax.set_xlim(0, x_end)
    ax.xaxis.set_major_locator(MultipleLocator(24))
    ax.xaxis.set_minor_locator(MultipleLocator(12))

axes[-1].tick_params(axis="x", labelsize=9, pad=6)
plt.setp(axes[-1].get_xticklabels(), rotation=0, ha="right")

plt.tight_layout(rect=[0, 0, 1, 0.96])
plt.show()

# %%
