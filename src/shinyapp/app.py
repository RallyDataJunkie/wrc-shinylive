import pandas as pd
from seaborn import heatmap, lineplot
import json
from pathlib import Path

# import functools

# import requests_cache
# from requests_cache.session import CachedSession

from shiny import render, reactive
from shiny.express import ui, input

from matplotlib import pyplot as plt
from matplotlib.colors import LinearSegmentedColormap

from wrc_rallydj.wrc_api import WRCAPIClient, time_to_seconds
from icons import question_circle_fill

pd.set_option("display.colheader_justify", "left")

# The cacheing is tricky:
# - we want to be able to force updates for live stages etc
# There is internal state in the class, which stores
# the last requested data unless we force an update
wrc = WRCAPIClient(use_cache=True, backend="memory", expire_after=600)

ui.panel_title("RallyDataJunkie WRC Results and Timing Browser", "WRC-RallyDJ")

# Shiny Express API
# https://shiny.posit.co/py/api/express/


@reactive.calc
@reactive.event(input.event)
def getSplitDists():
    year = str(input.season())
    rallyId = str(rally_id_var())
    with open(Path(__file__).parent / "patches.json", "r") as file:
        patches_json = json.load(file)
        try:
            splits = patches_json["split_distances"][year][rallyId]
            splits = pd.DataFrame.from_dict(splits, orient="index")
            splits.columns = [f"round{i}" for i in range(1, splits.shape[1] + 1)]
        except:
            splits = pd.DataFrame()
    return splits


@reactive.calc
@reactive.event(input.stage)
def split_dists_for_stage():
    split_dists = getSplitDists()
    stageIdFromCode = {v: k for k, v in wrc.stage_codes.items()}
    try:
        split_cumdists = (
            split_dists.loc[stageIdFromCode[input.stage()]].dropna().to_dict()
        )
        split_cumdists = {k: split_cumdists[k] for k in sorted(split_cumdists)}

        # Extract values in the sorted order of keys
        split_dists = {}
        prev = 0

        for k, v in split_cumdists.items():
            split_dists[k] = v - prev
            prev = v  # Update the previous value for the next iteration

    except:
        split_cumdists = {}
        split_dists = {}

    return split_cumdists, split_dists


# Create season selector
# Currently offers a hard coded set of season year options
ui.input_select(
    "season", "Season:", [str(i) for i in range(2024, 2026)], selected="2025"
)

# Create event selector
# Dynamically populated using a list of events
# based on the season selection
ui.input_select("event", "Event:", {}, selected=wrc.rallyId)

# Create stages selector
ui.input_select(
    "stage",
    "Stages:",
    {},
)


@reactive.calc
@reactive.event(input.event)
def rally_id_var():
    rally_id = input.event()
    wrc.eventId = wrc.rallyId2eventId[rally_id]
    wrc.rallyId = rally_id
    # Forcing the startlist sets the car2driver mapping
    _ = wrc.getStartlist()
    return wrc.rallyId


@reactive.calc
@reactive.event(input.event)
def carNum2name():
    _ = wrc.getStartlist()
    return wrc.carNum2name


@reactive.calc
@reactive.event(input.season)
def season_data():
    wrc.initialise(year=int(input.season()))
    # WRC API data fetch
    season = wrc.getResultsCalendar()
    return season


# TO DO - we whould have a single reactive value for wrc.rallyId
@reactive.calc
@reactive.event(input.event)
def stages_data():
    wrc.rallyId = rally_id_var()
    wrc.eventId = wrc.rallyId2eventId[wrc.rallyId]
    # WRC API data fetch
    stages = wrc.getStageDetails(update=True)
    return stages


@reactive.calc
def itinerary_data():
    wrc.rallyId = rally_id_var()
    wrc.eventId = wrc.rallyId2eventId[wrc.rallyId]
    # WRC API data fetch
    itinerary = wrc.getItinerary(update=True)
    return itinerary


@reactive.calc
def startlist_data():
    wrc.rallyId = rally_id_var()

    # WRC API data fetch
    startlist = wrc.getStartlist(update=True)
    return startlist


@reactive.calc
@reactive.event(input.stage)
def stage_times_data():
    wrc.stageId = input.stage()
    # WRC API data fetch
    stage_times = wrc.getStageTimes()
    return stage_times


@reactive.calc
@reactive.event(input.stage)
def split_times_data():
    wrc.stageId = input.stage()
    # WRC API data fetch
    # TO DO remove try - errors should be caught elsewhere?
    try:
        split_times_wide = wrc.getSplitTimes()
        split_times_long = wrc.getSplitsLong(split_times_wide)
        split_times_wide_numeric = wrc.get_splits_as_numeric(
            split_times_wide, regularise=input.stage() != "SHD"
        )
        return split_times_wide, split_times_long, split_times_wide_numeric
    except:
        return (pd.DataFrame(), pd.DataFrame(), pd.DataFrame())

@reactive.effect
@reactive.event(input.season)
def update_events_select():
    season = season_data()
    # events = season["EventName"].to_list()
    events = (
        season[["rallyId", "rallyTitle"]].set_index("rallyId")["rallyTitle"].to_dict()
    )
    ui.update_select("event", choices=events)


@reactive.effect
@reactive.event(input.event)
def update_stages_select():
    stages_df = stages_data()
    stages = stages_df[["STAGE", "stageId"]].set_index("stageId")["STAGE"].to_dict()
    ui.update_select("stage", choices=stages)


@reactive.effect
@reactive.event(input.stage)
def update_stages_driver_rebase_select():
    rebase_drivers = (
        stage_times_data()[["carNo", "driver"]].set_index("carNo")["driver"].to_dict()
    )
    ui.update_select("stage_rebase_driver", choices=rebase_drivers)


@reactive.effect
@reactive.event(input.stage)
def update_splits_driver_rebase_select():
    rebase_drivers = (
        stage_times_data()[["carNo", "driver"]].set_index("carNo")["driver"].to_dict()
    )
    rebase_drivers["ult"] = "Ultimate"
    ui.update_select("splits_rebase_driver", choices=rebase_drivers)


with ui.navset_card_underline():

    with ui.nav_panel("season"):

        @render.data_frame
        def season_frame():
            season = season_data()
            return render.DataGrid(season)

    with ui.nav_panel("stages"):

        @render.data_frame
        def stages_frame():
            stages = stages_data()
            return render.DataGrid(stages)

    with ui.nav_panel("itinerary"):

        @render.data_frame
        def itinerary_frame():
            itinerary = itinerary_data()
            return render.DataGrid(itinerary)

    with ui.nav_panel("startlist"):

        @render.data_frame
        def startlist_frame():
            return render.DataGrid(wrc.getStartlist())

    with ui.nav_panel("stagetimes"):

        # Create stage driver rebase selector
        ui.input_select(
            "stage_rebase_driver",
            "Driver rebase:",
            {},
        )

        @render.ui
        @reactive.event(input.stage_rebase_driver)
        def stage_times_short_frame():
            stage_times = stage_times_data()
            core_cols = [
                "pos",
                "carNo",
                "driver",
                "stageTime",
                "diffFirst",
                "diffPrev",
                "speed (km/h)",
                "pace (s/km)",
                "pace diff (s/km)",
            ]
            if input.stage() == "SHD":
                core_cols += [c for c in stage_times.columns if c.startswith("round")]
            stage_times = stage_times[
                list(set(core_cols).intersection(stage_times.columns))
            ]
            if "diffFirst" in stage_times.columns:
                rebase_gap_col = "Rebase Gap (s)"
                stage_times[rebase_gap_col] = stage_times["diffFirst"].apply(
                    time_to_seconds, retzero=True
                )

                rebase_driver = input.stage_rebase_driver()
                stage_times.loc[:, rebase_gap_col] = wrc.rebaseTimes(
                    stage_times, rebase_driver, "carNo", rebase_gap_col
                )
                cols_order = [
                    "pos",
                    "carNo",
                    "driver",
                    "stageTime",
                    rebase_gap_col,
                    "diffFirst",
                    "diffPrev",
                    "speed (km/h)",
                    "pace (s/km)",
                    "pace diff (s/km)",
                ]
                html = (
                    stage_times[[c for c in cols_order if c in stage_times.columns]]
                    .style.format(precision=1)
                    .bar(
                        subset=[rebase_gap_col],
                        align="zero",
                        color=["#5fba7d", "#d65f5f"],
                    )
                    .to_html()
                )
            else:
                html = (
                    stage_times[[c for c in core_cols if c in stage_times.columns]]
                    .style.format(precision=1)
                    .to_html()
                )
            return ui.HTML(html)
            # return render.DataGrid(
            #    stage_times[
            #        ["pos", "carNo", "driver", "stageTime", "Time", "Gap", "Diff"]
            #    ]
            # )

        # @render.data_frame
        # def stage_times_frame():
        #    stage_times = stage_times_data()
        #    return render.DataGrid(stage_times)

    with ui.nav_panel("splittimes"):
        # @render.text
        # def display_split_dists():
        #    return f"{split_dists_for_stage()}"

        with ui.tooltip(id="splits_rebase_tt"):
            ui.input_select(
                "splits_rebase_driver",
                "Driver rebase:",
                {},
            ),
            "Times are displayed relative to rebased driver. The 'Ultimate' time represents the quickest time in each split section."

        with ui.card(class_="mt-3"):
            with ui.card_header():
                with ui.tooltip(placement="right", id="splits_in_section_delta_tt"):
                    ui.span(
                        "Time gained / lost within each section in seconds relative to rebase driver ",
                        question_circle_fill,
                    )
                    "Delta times within each split section. Times are relative to rebased driver's time. Bright column: good/bad split section for rebased driver. Bright row: good/bad sections for (row) driver."

            with ui.tooltip(id="splits_reverse_palette_tt"):
                ui.input_checkbox(
                    "splits_reverse_palette", "Reverse rebase palette", False
                ),
                "Reverse the rebase palette to show deltas relative to the rebased driver's perspective."

            @render.plot(alt="Heatmap of within split delta times.")
            @reactive.event(
                input.stage, input.splits_rebase_driver, input.splits_reverse_palette
            )
            def seaborn_heatmap_splits():
                if input.stage() == "SHD":
                    return
                split_times_wide, split_times_long, split_times_wide_numeric = (
                    split_times_data()
                )
                if split_times_wide_numeric.empty:
                    return
                split_times_wide_numeric = split_times_wide_numeric.copy()
                split_cols = [
                    c for c in split_times_wide_numeric.columns if c.startswith("round")
                ]
                # output_ = split_times_wide_numeric
                output_ = wrc.get_split_duration(
                    split_times_wide_numeric,
                    split_cols,
                )

                # output_ = wrc.subtract_from_rows(
                #    output_, split_cols, ignore_first_row=False
                # )

                ult_row = {"carNo": "ult"}

                # Find minimum non-zero values for each round column
                for col in split_cols:
                    # Convert to numeric, filter non-zero, find minimum
                    min_val = pd.to_numeric(
                        output_[col][output_[col] > 0], errors="coerce"
                    ).min()
                    ult_row[col] = min_val
                output_ = pd.concat(
                    [output_, pd.DataFrame([ult_row])], ignore_index=True
                )

                rebase_driver = input.splits_rebase_driver()
                output_ = wrc.rebaseManyTimes(
                    output_, rebase_driver, "carNo", split_cols
                )
                output_ = output_[output_["carNo"] != "ult"]
                colors = (
                    ["red", "white", "green"]
                    if input.splits_reverse_palette()
                    else ["green", "white", "red"]
                )

                cmap = LinearSegmentedColormap.from_list("custom_cmap", colors)
                output_["carNo"] = output_["carNo"].map(carNum2name())
                output_.set_index("carNo", inplace=True)
                output_.columns = [
                    f"Split {i}" for i in range(1, output_.shape[1] + 1)
                ]  # [:-1] + ["Finish"]

                return heatmap(
                    output_, cmap=cmap, fmt=".1f", center=0, annot=True, cbar=False
                )

            @render.plot(alt="Line chart of within split delta times.")
            @reactive.event(
                input.stage, input.splits_rebase_driver, input.splits_reverse_palette
            )
            def seaborn_linechart_splits():
                if input.stage() == "SHD":
                    return
                split_times_wide, split_times_long, split_times_wide_numeric = (
                    split_times_data()
                )
                if split_times_long.empty:
                    return
                split_times_long = split_times_long.copy()
                rebase_driver = input.splits_rebase_driver()

                # TO DO - need a function to rebase a long df by group
                ll2 = split_times_long.pivot(
                    index="carNo", columns="roundN", values="timeInS"
                ).reset_index()
                cols = [c for c in ll2.columns if c.startswith("round")]
                lw = wrc.rebaseManyTimes(ll2, rebase_driver, "carNo", cols)
                lw["round0"] = 0.0
                lw = lw[["carNo", "round0"] + cols]
                ll3 = pd.melt(
                    lw,
                    id_vars=["carNo"],
                    value_vars=["round0"] + cols,
                    var_name="roundN",
                    value_name="timeInS",
                )
                ll3["round"] = ll3["roundN"].str.replace("round", "").astype(int)
                split_cumdists, split_dists = split_dists_for_stage()
                ll3["carNo"] = ll3["carNo"].map(carNum2name())
                if split_cumdists:
                    split_cumdists["round0"] = 0.0
                    ll3["dist"] = ll3["roundN"].map(split_cumdists)
                    g = lineplot(data=ll3, x="dist", y="timeInS", hue="carNo")

                else:
                    g = lineplot(data=ll3, x="round", y="timeInS", hue="name")
                g.set_ylim(g.get_ylim()[::-1])
                return g

        with ui.card(class_="mt-3"):
            with ui.card_header():
                with ui.tooltip(placement="right", id="splits_section_report_tt"):
                    ui.span(
                        "Split section report ",
                        question_circle_fill,
                    )
                    "Split section report. View section reports as time in section (s), or, if split distance available, average pace in section (s/km), or average speed in section (km/s)."

            with ui.tooltip(id="splits_section_view_tt"):
                ui.input_select(
                    "splits_section_view",
                    "Section report view",
                    {
                        "time": "Time in section (s)",
                        "pace": "Av. pace in section (s/km)",
                        "speed": "Av. speed in section (km/s)",
                    },
                    selected="time",
                ),
                "Select split section report type; Time (s), or, if available, average Pace (s/km) or average Speed (km/s)."

            @render.ui
            @reactive.event(input.splits_section_view)
            def split_report_view():
                view = input.splits_section_view()
                typ = {
                    "time": ("(s)", "(*Lower* is better.)"),
                    "speed": ("(km/s)", "(*Higher* is better.)"),
                    "pace": ("(s/km)", "(*Lower* is better.)"),
                }[view]
                return ui.markdown(
                    f"*{view.capitalize()}* {typ[0]} for each split. {typ[1]}"
                )

            @render.table
            @reactive.event(input.splits_section_view, input.stage)
            def split_report_in_section():
                view = input.splits_section_view()
                if input.stage() == "SHD":
                    return
                split_times_wide, split_times_long, split_times_wide_numeric = (
                    split_times_data()
                )
                if split_times_wide_numeric.empty:
                    return
                split_times_wide_numeric = split_times_wide_numeric.copy()
                split_cols = [
                    c for c in split_times_wide_numeric.columns if c.startswith("round")
                ]
                output_ = wrc.get_split_duration(
                    split_times_wide_numeric,
                    split_cols,
                )
                # Scope the view if data available
                split_cumdists, split_dists = split_dists_for_stage()
                if split_dists:
                    if view == "pace":
                        output_.update(
                            output_.loc[:, split_dists.keys()].apply(
                                lambda s: s / split_dists[s.name]
                            )
                        )
                    elif view == "speed":
                        output_.update(
                            output_.loc[:, split_dists.keys()].apply(
                                lambda s: 3600 * split_dists[s.name] / s
                            )
                        )
                styles = {c: "{0:0.1f}" for c in split_cols}

                output_["carNo"] = output_["carNo"].map(carNum2name())
                return output_.style.format(styles)

            # Select view type
            # Should we also have a radio button,
            # e.g. for absolute or relative;
            # And maybe accumulated or in-section
            # TO DO
            # ui.input_select(
            #    "splits_view",
            #    "Splits View (NOT WORKING YET):",
            #    {},
            # )

        with ui.card(class_="mt-3"):
            with ui.card_header():
                with ui.tooltip(placement="right", id="splits_times_original_tt"):
                    ui.span(
                        "WRC split times data ",
                        question_circle_fill,
                    )
                    "Original timing data from WRC live timing API."

            @render.table
            @reactive.event(input.stage)
            def split_times_original():
                split_times_wide, split_times_long, split_times_wide_numeric = (
                    split_times_data()
                )
                if split_times_wide.empty:
                    return pd.DataFrame()

                display_cols = [
                    "roadPos",
                    "start",
                    "carNo",
                    "driver",
                    "team/car",
                    "teamName",
                    "eligibility",
                    "groupClass",
                    "stageTime",
                    "diffFirst",
                ]
                # A set intersection does not preserve order?
                display_cols = [
                    c for c in display_cols if c in split_times_wide.columns
                ] + [c for c in split_times_wide.columns if c.startswith("round")]

                return split_times_wide[display_cols]

        with ui.card(class_="mt-3"):
            with ui.card_header():
                with ui.tooltip(placement="right", id="splits_times_acculumated_tt"):
                    ui.span(
                        "Raw accumulated time at each split in seconds ",
                        question_circle_fill,
                    )
                    "Accumulated time in seconds across the stage at each split."

            @render.data_frame
            #@render.table
            @reactive.event(input.splits_section_view, input.stage)
            def split_times_numeric():
                if input.stage() == "SHD":
                    return
                split_times_wide, split_times_long, split_times_wide_numeric = (
                    split_times_data()
                )
                if split_times_wide_numeric.empty:
                    return
                split_times_wide_numeric = split_times_wide_numeric.copy()
                # Package version error in cmap?
                # cm = sns.light_palette("green", as_cmap=True)
                # html = split_times_wide_numeric.style.background_gradient(cmap=cm, subset=[c for c in split_times_wide_numeric.columns if c.startswith("round")]).to_html()
                # def style_negative(v, props=''):
                #    return props if v < 100 else None
                split_cols = [
                    c for c in split_times_wide_numeric.columns if c.startswith("round")
                ]
                # The following sort of styling does work
                # html = (
                #    split_times_wide_numeric.style.map(
                #        style_negative, subset=split_cols, props="color:red;"
                #    )
                #    .map(
                #        lambda v: "opacity: 20%;" if (v < 100) and (v > -0.3) else None,
                #        subset=split_cols,
                #    )
                #    .to_html()
                # )
                # return ui.HTML(html)

                # "{:.1f}".format},

                split_times_wide_numeric["carNo"] = split_times_wide_numeric[
                    "carNo"
                ].map(carNum2name())
                # TO DO  precision number format formatting
                # styles = {c: "{0:0.1f}" for c in split_cols}
                # return split_times_wide_numeric.style.format(styles)
                split_times_wide_numeric[split_cols] = split_times_wide_numeric[
                    split_cols
                ].round(1)
                return render.DataGrid(
                    split_times_wide_numeric,
                    #styles={
                    #    "cols": split_cols,
                    #    "style": {"format": "{:.1f}"},
                    #},
                 )

            # @render.table
            # def split_times_rich2():
            #    split_times_wide, split_times_long, split_times_wide_numeric = (
            #        split_times_data()
            #    )
            #    return split_times_wide
            # @render.table
            # def split_times_rich3():
            #    split_times_wide, split_times_long, split_times_wide_numeric = (
            #        split_times_data()
            #    )
            #    return split_times_long
            # @render.data_frame
            # def split_times_frame():
            #    split_times_wide, split_times_long, split_times_wide_numeric = split_times_data()
            #    return render.DataGrid(split_times_wide)

    with ui.nav_panel("penalties"):

        @render.data_frame
        def penalties_frame():
            penalties = wrc.getPenalties()[
                [
                    "carNo",
                    "driver",
                    "team/car",
                    "teamName",
                    "eligibility",
                    "groupClass",
                    "control",
                    "reason",
                    "penaltyTime",
                    "penaltyDuration",
                ]
            ]
            return render.DataGrid(penalties)

    with ui.nav_panel("retirements"):

        @render.data_frame
        def retirements_frame():
            retirements = wrc.getRetirements()[
                [
                    "carNo",
                    "driver",
                    "team/car",
                    "teamName",
                    "eligibility",
                    "groupClass",
                    "control",
                    "reason",
                ]
            ]
            return render.DataGrid(retirements)
