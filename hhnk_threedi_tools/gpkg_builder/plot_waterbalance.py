# %%
import matplotlib.pyplot as plt
import pandas as pd


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


csv_path = r"H:\02.modellen\bergen_noord_variant_1_JA\03_3di_results\batch_results\results_variant_1_piek\02_output_rasters\piek_ghg_T100\waterbalance_piek_ghg_T100\water_balance_timeseries.csv"
output_path = r"H:\02.modellen\bergen_noord_variant_1_JA\03_3di_results\batch_results\results_variant_1_piek\02_output_rasters\piek_ghg_T100\waterbalance_piek_ghg_T100\water_balance_timeseries.png"
# %%
plot_water_balance(csv_path, output_path)
# %%
