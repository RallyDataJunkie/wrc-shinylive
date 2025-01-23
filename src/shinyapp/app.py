import pandas as pd

# import functools

# import requests_cache
# from requests_cache.session import CachedSession

from shiny import render, reactive
from shiny.express import ui, input

from matplotlib import pyplot as plt

from wrc_rallydj.wrc_api import WRCAPIClient

# The cacheing is tricky:
# - we want to be able to force updates for live stages etc
# There is internal state in the class, which stores
# the last requested data unless we force an update
wrc = WRCAPIClient(use_cache=True, backend="memory", expire_after=600)

ui.panel_title(
    "RallyDataJunkie WRC Results and Timing Browser", "WRC-RallyDJ"
)

# Create season selector
ui.input_select("season", "Season:", list(range(2024, 2026)), selected=2025)

# Create event selector
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


@reactive.effect
def itinerary_data():
    wrc.rallyId = rally_id_var()
    wrc.eventId = wrc.rallyId2eventId[wrc.rallyId]
    # WRC API data fetch
    itinerary = wrc.getItinerary(update=True)
    return itinerary


@reactive.effect
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
    return split_times_wide, split_times_long


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

        @render.data_frame
        def stage_times_frame():
            stage_times = stage_times_data()
            return render.DataGrid(stage_times)

    with ui.nav_panel("splittimes"):

        @render.data_frame
        def split_times_frame():
            split_times_wide, split_times_long = split_times_data()
            return render.DataGrid(split_times_wide)

    with ui.nav_panel("penalties"):

        @render.data_frame
        def penalties_frame():
            penalties = wrc.getPenalties()
            return render.DataGrid(penalties)

    with ui.nav_panel("retirements"):

        @render.data_frame
        def retirements_frame():
            retirements = wrc.getRetirements()
            return render.DataGrid(retirements)
