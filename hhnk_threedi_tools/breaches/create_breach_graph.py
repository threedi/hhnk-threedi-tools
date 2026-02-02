def create_breach_graph(
    scenarioname,
    time_sec,
    model_name,
    model_revision,
    breach_id,
    breach_depth,
    breach_wlev_upstream,
    breach_wlev_downstream,
    breach_q,
    breach_u,
    breach_width,
    fig_path_name,
):
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
        "(Model: "
        + model_name
        +
        #' #' + str(model_revision) +
        " Breach: "
        + str(breach_id[0])
        + " Bresdiepte: "
        + str(np.round(breach_depth[-1], 1))
        + "m"
        # + ' Starttijd simulatie: ' + str(start_time)  +')'
    )
    textstr
    # Plot the raw time series
    fig = plt.figure(figsize=(9, 9))
    fig.suptitle(scenarioname, fontsize=20)
    fig.text(0.5, 0.925, textstr, fontsize=13, ha="center")
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
