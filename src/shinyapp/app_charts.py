# Chart functions as used in shiny app
from pandas import melt
from matplotlib import pyplot as plt
from seaborn import barplot, boxplot, lineplot
from adjustText import adjust_text


def empty_plot(title=""):
    fig = plt.figure(figsize=(0.01, 0.01), dpi=100)
    ax = fig.add_subplot(111)
    if title:
        ax.set_title(title)
    ax.axis("off")
    return ax

def chart_seaborn_linechart_split_positions(wrc, split_times_wide, split_cols):
    split_times_wide[split_cols] = split_times_wide[split_cols].apply(
        lambda col: col.rank(method="min", ascending=True)
    )
    split_times_pos_long = melt(
        split_times_wide,
        id_vars=["carNo", "driverName"],
        value_vars=split_cols,
        var_name="roundN",
        value_name="position",
    )
    ax = lineplot(
        data=split_times_pos_long, x="roundN", y="position", hue="carNo", legend=False
    )
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
        last_point = split_times_pos_long[
            (split_times_pos_long["carNo"] == car)
            & (split_times_pos_long["roundN"] == wrc.SPLIT_FINAL)
        ]

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


def chart_seaborn_barplot_splits(
    wrc,
    split_times_wide,
    rebase_driver,
    splits_section_plot_type,
    rebase_reverse_palette,
):
    split_times_wide_, split_cols = wrc.rebase_splits_wide_with_ult(
        split_times_wide, rebase_driver
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
            bar.set_color("#2ecc71" if bar.get_width() > 0 else "#e74c3c")
        else:
            bar.set_color("#2ecc71" if bar.get_width() <= 0 else "#e74c3c")
    ax.invert_xaxis()
    return ax


def chart_seaborn_linechart_splits(wrc, stageId, split_times_wide, rebase_driver, max_delta=None):
    insert_point = f"{wrc.SPLIT_PREFIX}1"
    insert_loc = None
    if insert_point in split_times_wide.columns:
        insert_loc = split_times_wide.columns.get_loc(insert_point)
    elif wrc.SPLIT_FINAL in split_times_wide.columns:
        insert_loc = split_times_wide.columns.get_loc(wrc.SPLIT_FINAL)
    start_col = f"{wrc.SPLIT_PREFIX}0"
    if insert_loc is not None and start_col not in split_times_wide:
        split_times_wide.insert(
            loc=insert_loc,
            column=start_col,
            value=0,
        )

    split_cols = wrc.getSplitCols(split_times_wide)

    split_times_wide_ = wrc.rebaseManyTimes(
        split_times_wide,
        rebase_driver,
        "carNo",
        split_cols,
    )

    split_times_long = melt(
        split_times_wide_,
        value_vars=split_cols,
        id_vars=["carNo"],
        var_name="roundN",
        value_name="time",
    )

    split_dists_ = wrc.getStageSplitPoints(stageId=stageId, extended=True)

    split_dists = split_dists_.set_index("name")["distance"].to_dict()

    # Add start point
    split_dists[f"{wrc.SPLIT_PREFIX}0"] = 0

    split_times_long["distance"] = split_times_long["roundN"].map(split_dists)

    fig, ax = plt.subplots()
    # Highlight the region where y > 0 (drawn FIRST)
    ax.axhspan(
        split_times_long["time"].min(), 0, facecolor="lightgrey", alpha=0.5, zorder=0
    )

    ax = lineplot(
        data=split_times_long.sort_values(
            [
                "carNo",
            ]
        ),
        x="distance",
        y="time",
        hue="carNo",
        ax=ax,
    )

    if rebase_driver and rebase_driver != "ult":
        ax.set_ylim(ax.get_ylim()[::-1])

    if max_delta is not None:
        ax.set_ylim(bottom=min(max_delta, split_times_long["time"].max() ))

    texts = []
    for line, label in zip(
        ax.get_lines(),
        split_times_long.sort_values("carNo")["carNo"].unique(),
    ):
        x_data, y_data = (
            line.get_xdata(),
            line.get_ydata(),
        )
        x_last, y_last = x_data[-1], y_data[-1]
        text = ax.text(
            x_data[-1],
            y_data[-1],
            f" {label}",
            ha="left",
            verticalalignment="center",
        )
        texts.append(text)

    # Adjust labels to avoid overlap
    adjust_text(
        texts,
        only_move={
            "text": "y",
            "static": "y",
            "explode": "y",
            "pull": "y",
        },
        arrowprops=dict(arrowstyle="-", color="gray", lw=0.5),
    )

    ax.set_xlim(
        split_times_long["distance"].min(),
        split_times_long["distance"].max() * 1.15,
    )

    ax.legend_.remove()
    return ax


def chart_plot_split_dists(wrc, scaled_splits_wide, splits_section_view):
    split_cols = wrc.getSplitCols(scaled_splits_wide)
    scaled_splits_long = melt(
        scaled_splits_wide,
        id_vars=["carNo", "driverName"],
        value_vars=split_cols,
        var_name="roundN",
        value_name="value",
    )
    ylabel = "Time in section (s)"
    view = splits_section_view
    if view == "pace":
        ylabel = "Pace (s/km)"
    elif view == "speed":
        ylabel = "Speed (km/h)"

    ax = boxplot(data=scaled_splits_long, x="roundN", y="value")
    ax.set(xlabel=None, ylabel=ylabel)
    return ax


def chart_seaborn_linechart_stage_progress_positions(wrc, overall_times_wide):
    return chart_seaborn_linechart_stage_progress_typ(
        wrc, overall_times_wide, typ="position"
    )


def chart_seaborn_linechart_stage_progress_typ(
    wrc, overall_times_wide, typ="position", greyupper=False
):
    overall_cols = wrc.getOverallStageCols(overall_times_wide)
    if typ == "position":
        # This does essentially a category rank on all passed rows
        # TO DO handle category or overall etc properly
        overall_times_wide[overall_cols] = overall_times_wide[overall_cols].apply(
            lambda col: col.rank(method="min", ascending=True)
        )

    overall_times_pos_long = melt(
        overall_times_wide,
        id_vars=["carNo", "driverName"],
        value_vars=overall_cols,
        var_name="roundN",
        value_name=typ,
    )
    fig, ax = plt.subplots()
    if greyupper:
        # Highlight the region where y > 0 (drawn FIRST)
        if "position" in typ or typ == "timeInS":
            ax.axhspan(
                overall_times_pos_long[typ].min(),
                0,
                facecolor="lightgrey",
                alpha=0.5,
                zorder=0,
            )
        else:
            ax.axhspan(
                0,
                overall_times_pos_long[typ].max(),
                facecolor="lightgrey",
                alpha=0.5,
                zorder=0,
            )
    ax = lineplot(
        data=overall_times_pos_long, x="roundN", y=typ, hue="carNo", legend=False, ax=ax
    )
    x_min, x_max = ax.get_xlim()
    for car in overall_times_pos_long["carNo"].unique():
        # Filter data for this car and get the last round's position
        first_point = overall_times_pos_long[
            (overall_times_pos_long["carNo"] == car)
            & (
                overall_times_pos_long["roundN"]
                == overall_times_pos_long["roundN"].iloc[0]
            )
        ]
        if not first_point.empty:
            # Get position value for the last point
            y_pos = first_point[typ].values[0]

            # Add text slightly to the right of the maximum round
            ax.text(
                x_min - 0.1,
                y_pos,
                f"#{car}",
                verticalalignment="center",
                fontsize=9,
            )
        # TO DO if someone drops out before the end, label at the end of their rally.
        last_point = overall_times_pos_long[
            (overall_times_pos_long["carNo"] == car)
            & (
                overall_times_pos_long["roundN"]
                == overall_times_pos_long["roundN"].iloc[-1]
            )
        ]

        if not last_point.empty:
            # Get position value for the last point
            y_pos = last_point[typ].values[0]

            # Add text slightly to the right of the maximum round
            ax.text(
                x_max + 0.05,
                y_pos,
                f"#{car}",
                verticalalignment="center",
                fontsize=9,
            )
    ax.set(xlabel=None, ylabel=f"Overall {typ}")
    if "position" in typ or typ == "timeInS":
        ax.invert_yaxis()

    # To Do - option for having distance along x esp. for timeInS -> gradient gives pace difference
    plt.xlim(x_min - 0.5, x_max + 0.5)
    return ax


def chart_seaborn_barplot_stagetimes(stage_times_df, rebase_reverse_palette):
    rebase_gap_col = "Rebase Gap (s)"

    ax = barplot(
        stage_times_df,
        orient="h",
        y="carNo",
        x=rebase_gap_col,
        legend=False,
    )

    # Get all the bars from the plot
    bars = [patch for patch in ax.patches]

    # Color each bar based on its height
    for bar in bars:
        if rebase_reverse_palette:
            bar.set_color("#2ecc71" if bar.get_width() > 0 else "#e74c3c")
        else:
            bar.set_color("#2ecc71" if bar.get_width() <= 0 else "#e74c3c")
    ax.invert_xaxis()
    return ax


def chart_plot_driver_stagewins(stage_winners):
    # Get value counts and reset index to create a plotting dataframe
    stage_counts = (
        # stage_winners.iloc[: idx[0] + 1]
        stage_winners.groupby("driverName")["code"]
        .count()
        .sort_values(ascending=False)
        .reset_index()
    )

    # Create figure with larger size for better readability
    plt.figure(figsize=(10, 6))

    # Create horizontal bar plot
    ax = barplot(
        data=stage_counts,
        y="driverName",
        x="code",
        orient="h",
        color="steelblue",
    )
    ax.xaxis.set_major_locator(plt.MaxNLocator(integer=True))
    ax.set(xlabel=None, ylabel=None)
    return ax


# TO DO
def sparks_test():
    import pandas as pd
    import numpy as np
    import matplotlib.pyplot as plt
    import matplotlib.gridspec as gridspec
    from matplotlib.ticker import NullFormatter

    # Sample data creation - replace this with your actual data
    np.random.seed(42)

    # Define the codes
    codes = ["OGI", "EVA", "NEU", "TÂN", "FOU", "KAT", "GRE", "BRE", "OST", "SUN"]

    # Create sample data for each metric and code
    n_periods = 30
    data = []

    for code in codes:
        # Generate random time series data with some trends
        overall_pos = np.cumsum(np.random.normal(0, 0.5, n_periods)) + 10
        overall_gap = -np.abs(np.random.normal(0, 1, n_periods)) - 5  # Negative values
        overall_pos_change = np.random.normal(0, 1, n_periods)
        stage_pos = np.cumsum(np.random.normal(0, 0.3, n_periods)) + 5
        stage_gap = -np.abs(np.random.normal(0, 0.8, n_periods)) - 3  # Negative values

        for i in range(n_periods):
            data.append(
                {
                    "code": code,
                    "period": i,
                    "overall_pos": overall_pos[i],
                    "overall_gap": overall_gap[i],
                    "overall_pos_change": overall_pos_change[i],
                    "stage_pos": stage_pos[i],
                    "stage_gap": stage_gap[i],
                }
            )

    # Convert to DataFrame
    df = pd.DataFrame(data)

    # Now create the sparkline dashboard
    # plt.style.use("ggplot")
    plt.style.use("default")
    fig = plt.figure(figsize=(15, 10), facecolor="white")

    # Define metrics to plot
    metrics = [
        "overall_pos",
        "overall_gap",
        "overall_pos_change",
        "stage_pos",
        "stage_gap",
    ]

    # Set up the grid
    n_rows = len(codes)
    n_cols = len(metrics) + 1  # +1 for the code column
    gs = gridspec.GridSpec(n_rows, n_cols, width_ratios=[1] + [3] * len(metrics))

    # Create a title row
    plt.figtext(0.02, 0.95, "code", fontsize=12, fontweight="bold")
    x_pos = 0.1
    x_gap = 0.95 / len(metrics)
    for metric in metrics:
        plt.figtext(
            x_pos,
            0.95,
            metric.replace("_", " ").title(),
            fontsize=12,
            fontweight="bold",
        )
        x_pos += x_gap

    # Plot each sparkline
    for i, code in enumerate(codes):
        code_data = df[df["code"] == code]

        # Add code name
        ax_code = plt.subplot(gs[i, 0])
        ax_code.text(0.5, 0.5, code, fontsize=12, ha="center", va="center")
        ax_code.axis("off")

        # Create sparklines for each metric
        for j, metric in enumerate(metrics):
            ax = plt.subplot(gs[i, j + 1])

            values = code_data[metric].values
            periods = code_data["period"].values

            # Determine color based on the metric
            if "gap" in metric:
                color = "red"  # Negative values
            else:
                color = "blue"

            # Plot the sparkline
            if "change" not in metric and "gap" not in metric:
                # Use a step plot
                ax.step(periods, values, color=color, linewidth=1.5)

                # Add dotted horizontal lines for reference
                if values.max() > 0 and values.min() < 0:
                    ax.axhline(y=0, color="black", linestyle=":", linewidth=0.5)

                # Add top and bottom reference lines
                y_max = values.max() * 1.1
                y_min = values.min() * 1.1 if values.min() < 0 else 0
                ax.axhline(y=y_max, color="black", linestyle=":", linewidth=0.5)
                ax.axhline(y=y_min, color="black", linestyle=":", linewidth=0.5)

            # Add bar chart style for some metrics (like changes or gaps)
            if "change" in metric or "gap" in metric:
                for k, v in enumerate(values):
                    if v > 0:
                        ax.plot([k, k], [0, v], color=color, linewidth=3)
                    else:
                        ax.plot([k, k], [0, v], color="red", linewidth=3)

            # Remove axes
            ax.xaxis.set_major_formatter(NullFormatter())
            ax.yaxis.set_major_formatter(NullFormatter())
            ax.spines["top"].set_visible(False)
            ax.spines["right"].set_visible(False)
            ax.spines["bottom"].set_visible(False)
            ax.spines["left"].set_visible(False)
            ax.tick_params(axis="both", which="both", length=0)

            # Add light horizontal grid line
            ax.axhline(y=0, color="gray", alpha=0.3, linewidth=0.5)

            # Set y-axis limits to be consistent across rows for the same metric
            metric_min = df[metric].min() * 1.1
            metric_max = df[metric].max() * 1.1
            ax.set_ylim(metric_min, metric_max)

    # Add horizontal lines between rows
    for i in range(1, n_rows):
        plt.axhline(
            y=i / n_rows,
            color="green",
            linestyle="-",
            linewidth=0.5,
            xmin=0.05,
            xmax=0.95,
        )

    plt.subplots_adjust(
        wspace=0.1, hspace=0.1, left=0.05, right=0.95, top=0.9, bottom=0.05
    )
    # plt.suptitle("Sparkline Dashboard", fontsize=14, y=0.98)

    plt.tight_layout(rect=[0, 0, 1, 0.93])
    plt.show()
