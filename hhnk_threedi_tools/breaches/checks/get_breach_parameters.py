# %%
import numpy as np
import pandas as pd

# location
file = r"H:\03.resultaten\Overstromingsberekeningenprimairedoorbraken2024\output\ROR_PRI-dijktrajecten_12-1_12-2_13-6_13-7_Deel_Zuid\ROR-PRI-WATERKERING_MEDEMBLIJK_0-T100000\ROR-PRI-WATERKERING MEDEMBLIJK_0-T100000_breach_data_agg.csv"
# breach data
B0 = 50
T0 = 900
# selected time
# Start after T0, when horizontal breach growth is established and the parameter comparison is more robust.
start_time = 2700
end_time = 1717201

g = 9.81

parameters = {
    "sand": [1.20, 0.20, 0.04],
    "clay": [1.79, 0.60, 0.04],
    "clay_rana_Uc_assumed": [0.80, 0.60, 1.40],
}

# read breach data
df = pd.read_csv(file, sep=";", decimal=",")

df = df.dropna(
    subset=[
        "time_sec",
        "breach_width",
        "breach_wlev_upstream",
        "breach_wlev_downstream",
    ]
)
# %%
# filter data to selected time range
df = df[(df["time_sec"] >= start_time) & (df["time_sec"] <= end_time)].copy()

# df = df.sort_values("time_sec").reset_index(drop=True)

# replace negative water levels with 0
df["delta_h"] = (df["breach_wlev_upstream"] - df["breach_wlev_downstream"]).clip(lower=0)

df["dt"] = df["time_sec"].diff().fillna(0)

# calculate total  observed growth
observed_growth = df["breach_width"].iloc[-1] - df["breach_width"].iloc[0]

summary = []
# %%
for name, values in parameters.items():
    f1, uc, f2 = values

    # calculate breach growth rate per time step
    rate = (f1 * f2 / (uc**2 * np.log(10))) * (g * df["delta_h"]) ** 1.5 / (1 + (f2 * g / uc) * (df["time_sec"] - T0))

    # save rate growth to dataframe with name
    df[f"rate_{name}"] = rate

    # accumulative growht rate using trapezoidal rule to calculate growth over time
    # shifit helps to get the previous value of the rate so we can sum and the multiply
    # by the time step. This way we get the area and thereby the growth in METERS over the time.

    # Example:
    # At t = 0 s, the growth rate is 2 m/s.
    # At t = 10 s, the growth rate is 4 m/s.
    # We assume the rate changes gradually between both timesteps.
    # Therefore, the average rate is:
    # (2 + 4) / 2 = 3 m/s
    #
    # The breach growth during the interval is:
    # 3 m/s * 10 s = 30 m

    average_rate = (rate.shift(1) + rate) / 2
    growth_per_timestep = average_rate * df["dt"]

    # calculated cumulative growth over time
    df[f"growth_{name}"] = growth_per_timestep.fillna(0).cumsum()

    # Reconstruct the breach width by adding the calculated cumulative growth
    # to the observed breach width at the start of the selected time range.
    df[f"width_{name}"] = df["breach_width"].iloc[0] + df[f"growth_{name}"]

    # Calculate the difference between the observed breach width and the calculated breach width
    error = df[f"width_{name}"] - df["breach_width"]

    # create summary
    summary.append(
        {
            "material": name,
            "f1": f1,
            "Uc": uc,
            "f2": f2,
            "observed_growth": observed_growth,
            "calculated_growth": df[f"growth_{name}"].iloc[-1],
            "difference": df[f"growth_{name}"].iloc[-1] - observed_growth,
            "RMSE": np.sqrt((error**2).mean()),
        }
    )

# save data
summary = pd.DataFrame(summary)

df.to_csv(
    "breach_calculations.csv",
    sep=";",
    decimal=",",
    index=False,
)

summary.to_csv(
    "breach_summary.csv",
    sep=";",
    decimal=",",
    index=False,
)

print(summary)
print("\nB0 =", B0, "m")
print("T0 =", T0, "s")
# %%
