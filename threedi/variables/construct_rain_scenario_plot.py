from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
import matplotlib.pyplot as plt
from matplotlib.figure import Figure
import matplotlib as mplt

def create_results_plot(rain,
                        detected_rain,
                        days_dry_start,
                        days_dry_end,
                        timestep):
    figure = Figure()
    canvas = FigureCanvas(figure)
    ax = figure.add_subplot()
    ax.grid()
    try:
        rain_plot = rain  # Toevoegen zodat er rechte lijnen geplot worden
        rain_plot.insert(detected_rain[-1], rain_plot[detected_rain[-1]])

        rain_plot = rain_plot + [rain_plot[-1]]

        # Plot de regen over de tijd
        x_dagen = [0] + [x / 24 for x in range(1, len(rain) - 1)]

        x_dagen.insert(detected_rain[0] - 1, x_dagen[detected_rain[0] - 1])
        x_dagen.insert(detected_rain[-1] + 1, x_dagen[detected_rain[-1] + 1])
        ax.plot(x_dagen,
                [a * 100000 / 1.05833 for a in rain_plot])
        ax.plot([days_dry_start, days_dry_start],
                [0, max(rain)*100000/1.05833],
                c='green')
        ax.plot([timestep[-1]/60/24 - days_dry_end, timestep[-1]/60/24 - days_dry_end],
                [0, max(rain)*100000/1.05833],
                c='green')
        ax.add_patch(mplt.patches.Polygon([[days_dry_start, 0],
                                           [timestep[-1]/60/24 - days_dry_end, 0],
                                           [timestep[-1]/60/24 - days_dry_end, max(rain)*100000/1.05833],
                                           [days_dry_start, max(rain)*100000/1.05833]],
                                          closed=True, alpha=0.2, color='green'))
        figure.suptitle('Neerslag')
        ax.set_xlabel('Tijd [dagen]', fontsize=10)
        ax.set_ylabel('Neerslag [mm/dag]', fontsize=10)
        plt.legend(['Neerslag', 'Gedetecteerde neerslagperiode'],
                   loc='upper right',
                   fancybox=True,
                   framealpha=0.5)
        return canvas
    except Exception as e:
        raise e from None
