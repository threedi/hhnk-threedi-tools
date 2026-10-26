# %%
import os

import numpy as np
import pandas as pd

from hhnk_threedi_tools.breaches.breaches import Breaches


def create_breach_graph(
    scenarioname,
    time_sec,
    model_name,
    breach_id,
    breach_depth,
    breach_wlev_upstream,
    breach_wlev_downstream,
    breach_q,
    breach_u,
    breach_width,
    fig_path_name,
    initial_breedte,
):
    """
    Create and save a PNG summarising breach time series.

    The function produces a 4-panel figure showing, from top to bottom:
    - upstream and downstream water level (m NAP),
    - discharge through the breach (m3/s),
    - flow velocity through the breach (m/s),
    - breach width (m).

    Parameters
    ----------
    scenarioname : str
        Title used for the figure (scenario or breach name).
    time_sec : array-like (seconds)
        Time vector in seconds since simulation start. Converted to hours for
        plotting. Only values up to 485 hours are used in the figure.
    model_name : str
        Model identifier used in the figure text.
    model_revision : int | str
        Model revision number (included in figure text if desired).
    breach_id : array-like
        Breach identifier array-like; breach_id[0] is used in the title text.
    breach_depth : array-like
        Time series of breach depth (m).
    breach_wlev_upstream : array-like
        Time series of upstream water level (m NAP).
    breach_wlev_downstream : array-like
        Time series of downstream water level (m NAP).
    breach_q : array-like
        Time series of discharge through the breach (m3/s).
    breach_u : array-like
        Time series of flow velocity through the breach (m/s).
    breach_width : array-like
        Time series of breach width (m).
    fig_path_name : str | Path
        Output file path (PNG) where the figure will be saved.

    Returns
    -------
    None

    Side effects
    ------------
    - Saves a PNG file at fig_path_name.
    - Closes and clears matplotlib figures to free memory.

    Notes
    -----
    All time series inputs should have compatible lengths. The function uses
    matplotlib and numpy for plotting and basic time-axis selection/interpolation.
    """
    import matplotlib.gridspec as gridspec
    import matplotlib.pyplot as plt
    import numpy as np

    # Set the Parameter of the graph.png\
    time_h = time_sec / 3600
    time_50h = np.where(time_h <= 485)
    time_h = time_h[(time_50h)]
    size_time = []
    size_time = len(time_50h[0])

    steps = []
    if time_h[-1] >= 480:
        steps = np.arange(0, time_h[-1], 24)
    elif time_h[-1] >= 96:
        steps = np.arange(0, time_h[-1], 12)
    elif time_h[-1] >= 24:
        steps = np.arange(0, time_h[-1], 4)
    else:
        steps = np.arange(0, time_h[-1], 1)
    # Set the titles and strings of the grph
    textstr = (
        f"Model: {model_name} | Breach ID: {str(breach_id[0])}\n"
        f" Bresdiepte: {(np.round(breach_depth[-1], 1))}m | Initial Breedte: {initial_breedte}"
        # + ' Starttijd simulatie: ' + str(start_time)  +')'
    )
    textstr
    # Plot the raw time series
    fig = plt.figure(figsize=(9, 9))
    fig.suptitle(scenarioname, fontsize=16, fontweight="bold")
    fig.text(0.5, 0.92, textstr, fontsize=10, ha="center", multialignment="center")
    gs = gridspec.GridSpec(4, 1, figure=fig)

    # Plot the time_h vs data of Breach wlev_upstream and add the lable. First argument = time, second argument = Data, Third Argument = lable
    ax0 = fig.add_subplot(gs[0, :])

    ax0.plot(time_h, breach_wlev_upstream[0:size_time], label="bovenstrooms")
    ax0.plot(time_h, breach_wlev_downstream[0:size_time], label="benedenstrooms")
    ax0.set_ylabel("[m NAP]")
    ax0.set_xticklabels([])
    ax0.set_xticks(steps)
    ax0.legend()
    ax0.autoscale(enable=True, axis="x", tight=True)
    # plt.title('Waterstand rond de bres', fontsize=13, loc ='center', pad = 5)
    plt.text(
        0.5,
        0.9,
        "Waterstand rond de bres",
        horizontalalignment="center",
        verticalalignment="top",
        transform=ax0.transAxes,
        fontsize=13,
    )
    plt.grid()

    #

    ax = fig.add_subplot(gs[1, :])
    ax.plot(time_h, breach_q[0:size_time])
    ax.set_xticklabels([])
    ax.set_xticks(steps)
    ax.set_ylabel("[m3/s]")
    ax.autoscale(enable=True, axis="x", tight=True)
    # plt.title('Debiet door de bres', fontsize=13)
    plt.text(
        0.5,
        0.9,
        "Debiet door de bres",
        horizontalalignment="center",
        verticalalignment="top",
        transform=ax.transAxes,
        fontsize=13,
    )
    plt.grid()

    #
    ax1 = fig.add_subplot(gs[2, :])
    ax1.plot(time_h, breach_u[0:size_time])
    ax1.set_xticklabels([])
    ax1.set_xticks(steps)
    ax1.set_ylabel("[m/s]")
    ax1.autoscale(enable=True, axis="x", tight=True)
    plt.grid()
    # plt.title('Stroomsnelheid door de bres', fontsize=13)
    plt.text(
        0.5,
        0.9,
        "Stroomsnelheid door de bres",
        horizontalalignment="center",
        verticalalignment="top",
        transform=ax1.transAxes,
        fontsize=13,
    )

    #
    ax2 = fig.add_subplot(gs[3, :])
    ax2.plot(time_h, breach_width[0:size_time])
    ax2.set_ylabel("[m]")
    ax2.set_xticks(steps)
    ax2.autoscale(enable=True, axis="x", tight=True)
    # plt.title('Breedte van de bres', fontsize=13)
    plt.text(
        0.5,
        0.2,
        "Breedte van de bres",
        horizontalalignment="center",
        verticalalignment="top",
        transform=ax2.transAxes,
        fontsize=13,
    )

    ax2.set_xlabel("Uren na bresdoorbraak")
    plt.grid()

    for item in (
        [ax0.xaxis.label, ax0.yaxis.label]
        + ax0.get_xticklabels()
        + ax0.get_yticklabels()
        + [ax.xaxis.label, ax.yaxis.label]
        + ax.get_xticklabels()
        + ax.get_yticklabels()
        + [ax1.xaxis.label, ax1.yaxis.label]
        + ax1.get_xticklabels()
        + ax1.get_yticklabels()
        + [ax2.xaxis.label, ax2.yaxis.label]
        + ax2.get_xticklabels()
        + ax2.get_yticklabels()
    ):
        item.set_fontsize(12)

    # plt.show()

    # # opslaan figuur
    fig.savefig(fig_path_name, format="png")

    # close figure from memory
    plt.cla()
    plt.clf()
    plt.close("all")
    fig = []


# %%
if __name__ == "__main__":
    base_folder = r"H:\03.resultaten\RWS_Test\results_output"
    schematitation_name = os.listdir(base_folder)
    for schema in schematitation_name:
        schema_path = os.path.join(base_folder, schema)
        scenarios = os.listdir(schema_path)
        for scenario in scenarios:
            scenario_path = os.path.join(schema_path, scenario)
            breach = Breaches(scenario_path)
            breach_data_gdf = pd.read_csv(breach.csv.breach_data_agg, sep=";", decimal=",")
            scenarioname = breach_data_gdf.name[0]
            model_name = schema
            breach_id = breach_data_gdf.breach_id
            # timestamps = breach_data_gdf.timestamps.tolist()
            time_sec = breach_data_gdf.time_sec.tolist()
            breach_width = breach_data_gdf.breach_width.tolist()
            breach_depth = breach_data_gdf.breach_depth.tolist()
            breach_q = breach_data_gdf.breach_q.tolist()
            breach_u = breach_data_gdf.breach_u.tolist()
            breach_wlev_upstream = breach_data_gdf.breach_wlev_upstream.tolist()
            breach_wlev_downstream = breach_data_gdf.breach_wlev_downstream.tolist()
            jpeg_name = breach.name + "agg.png"
            fig_path_name = os.path.join(breach.jpeg.path / jpeg_name)
            initial_breedte = 10
            if os.path.exists(fig_path_name):
                continue
            create_breach_graph(
                scenarioname,
                np.array(time_sec),
                model_name,
                breach_id,
                breach_depth,
                breach_wlev_upstream,
                breach_wlev_downstream,
                breach_q,
                breach_u,
                breach_width,
                fig_path_name,
                initial_breedte,
            )

# %%
