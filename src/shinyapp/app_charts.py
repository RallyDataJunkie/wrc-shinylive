# Chart functions as used in shiny app
from pandas import melt
from matplotlib import pyplot as plt
from seaborn import barplot, boxplot, lineplot

def chart_seaborn_linechart_split_positions(wrc, split_times_wide, split_cols):
    split_times_wide[split_cols] = split_times_wide[split_cols].apply(lambda col: col.rank(method='min', ascending=True))
    split_times_pos_long = melt(
        split_times_wide,
        id_vars=["carNo", "driverName"],
        value_vars=split_cols,
        var_name="roundN",
        value_name="position",
    )
    ax = lineplot(data=split_times_pos_long, x="roundN", y="position", hue="carNo", legend=False)
    x_min, x_max = ax.get_xlim()
    for car in split_times_pos_long["carNo"].unique():
        # Filter data for this car and get the last round's position
        first_point = split_times_pos_long[
            (split_times_pos_long["carNo"] == car)
            & (split_times_pos_long["roundN"] == f"{wrc.SPLIT_PREFIX}1")
        ]
        if not first_point.empty:
            # Get position value for the last point
            y_pos = first_point["position"].values[0]

            # Add text slightly to the right of the maximum round
            ax.text(
                x_min - 0.1,
                y_pos,
                f"#{car}",
                verticalalignment="center",
                fontsize=9,
            )
        last_point = split_times_pos_long[(split_times_pos_long["carNo"] == car) & 
                                        (split_times_pos_long["roundN"] == wrc.SPLIT_FINAL)]

        if not last_point.empty:
            # Get position value for the last point
            y_pos = last_point["position"].values[0]

            # Add text slightly to the right of the maximum round
            ax.text(
                x_max + 0.05,
                y_pos,
                f"#{car}",
                verticalalignment="center",
                fontsize=9,
            )
    ax.set(xlabel=None, ylabel="Position")
    ax.invert_yaxis()
    plt.xlim(x_min - 0.5, x_max + 0.5)
    return ax



def chart_seaborn_barplot_splits(wrc, split_times_wide, rebase_driver, splits_section_plot_type, rebase_reverse_palette):
    split_times_wide_, split_cols = (
        wrc.rebase_splits_wide_with_ult(
            split_times_wide, rebase_driver
        )
    )

    split_times_long = melt(
        split_times_wide_,
        value_vars=split_cols,
        id_vars=["carNo"],
        var_name="roundN",
        value_name="time",
    )

    if splits_section_plot_type == "bydriver":
        ax = barplot(
            split_times_long,
            orient="h",
            hue="roundN",
            x="time",
            y="carNo",
            legend=False,
        )
    else:
        ax = barplot(
            split_times_long,
            orient="h",
            y="roundN",
            x="time",
            hue="carNo",
            legend=False,
        )

    # Get all the bars from the plot
    bars = [patch for patch in ax.patches]

    # Color each bar based on its height
    for bar in bars:
        if rebase_reverse_palette:
            bar.set_color(
                "#2ecc71"
                if bar.get_width() > 0
                else "#e74c3c"
            )
        else:
            bar.set_color(
                "#2ecc71"
                if bar.get_width() <= 0
                else "#e74c3c"
            )
    ax.invert_xaxis()
    return ax