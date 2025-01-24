import pandas as pd
import seaborn as sns

# import functools

# import requests_cache
# from requests_cache.session import CachedSession

from shiny import render, reactive
from shiny.express import ui, input

from matplotlib import pyplot as plt

from wrc_rallydj.wrc_api import WRCAPIClient, time_to_seconds

# The cacheing is tricky:
# - we want to be able to force updates for live stages etc
# There is internal state in the class, which stores
# the last requested data unless we force an update
wrc = WRCAPIClient(use_cache=True, backend="memory", expire_after=600)

ui.panel_title(
    "RallyDataJunkie WRC Results and Timing Browser", "WRC-RallyDJ"
)

# Create season selector
# Currently offers a hard coded set of season year options
ui.input_select("season", "Season:", list(range(2024, 2026)), selected=2025)

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
    rally_id= input.event()
    wrc.eventId = wrc.rallyId2eventId[rally_id]
    wrc.rallyId = rally_id
    return wrc.rallyId

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
    split_times_wide = wrc.getSplitTimes()
    split_times_long = wrc.getSplitsLong(split_times_wide)
    split_times_wide_numeric = wrc.get_splits_as_numeric(
        split_times_wide, regularise=input.stage() != "SHD"
    )
    return split_times_wide, split_times_long, split_times_wide_numeric


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
    stages = (
        stages_df[["STAGE", "stageId"]].set_index("stageId")["STAGE"].to_dict()
    )
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
            if input.stage()=="SHD":
                core_cols += [c for c in stage_times.columns if c.startswith("round")]
            stage_times = stage_times[list(set(core_cols).intersection(stage_times.columns))]
            if "diffFirst" in stage_times.columns:
                rebase_gap_col = "Rebase Gap (s)"
                stage_times[rebase_gap_col] = stage_times["diffFirst"].apply(
                time_to_seconds, retzero=True)

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
                    stage_times[[c for c in cols_order if c in stage_times.columns]].style.format(precision=1)
                    .bar(
                        subset=[rebase_gap_col], align="zero", color=["#5fba7d", "#d65f5f"]
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

        # Create splits driver rebase selector
        ui.input_select(
            "splits_rebase_driver",
            "Driver rebase (NOT WORKING YET):",
            {},
        )

        # Select view type
        # Should we also have a radio button,
        # e.g. for absolute or relative;
        # And maybe accumulated or in-section
        ui.input_select(
            "splits_view",
            "Splits View (NOT WORKING YET):",
            {},
        )

        ui.markdown(
            """

## Original data view (diff to stage winner)

                    """
        )

        @render.table
        def split_times_base():
            split_times_wide, split_times_long, split_times_wide_numeric = (
                split_times_data()
            )
            display_cols=["pos",
                    "start",
                    "carNo",
                    "driver",
                    "team/car",
                    "teamName",
                    "eligibility",
                    "groupClass",
                    "stageTime",
                    "diffFirst"]
            # A set intersection does not preserve order?
            display_cols = [c for c in display_cols if c in split_times_wide.columns] + [c for c in split_times_wide.columns if c.startswith("round")]

            return split_times_wide[display_cols]

        ui.markdown(
            """

## Time within each section in seconds

                    """
        )

        @render.table
        def split_times_in_section():
            if input.stage() == "SHD":
                return
            split_times_wide, split_times_long, split_times_wide_numeric = (
                split_times_data()
            )
            split_cols = [
                c for c in split_times_wide_numeric.columns if c.startswith("round")
            ]
            output_ = wrc.get_split_duration(
                split_times_wide_numeric,
                split_cols,
            )
            wrc.subtract_from_rows_inplace(output_, split_cols)
            return output_

        ui.markdown(
            """

## Raw accumulated time at each split in seconds

                    """
        )

        # @render.ui
        @render.table
        def split_times_numeric():
            split_times_wide, split_times_long, split_times_wide_numeric = (
                split_times_data()
            )
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

            styles = {c:"{0:0.1f}" for c in split_cols }
            return split_times_wide_numeric.style.format(styles)

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
                    "reason","penaltyTime", "penaltyDuration"
                ]
            ]
            return render.DataGrid(penalties)

    with ui.nav_panel("retirements"):

        @render.data_frame
        def retirements_frame():
            retirements = wrc.getRetirements()[["carNo", "driver","team/car", "teamName", "eligibility","groupClass","control","reason"]]
            return render.DataGrid(retirements)
