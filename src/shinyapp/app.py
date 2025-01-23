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

# Create season selector
ui.input_select(
    "season",
    "Season:",
    list(range(2024, 2026)),
)

# Create event selector
ui.input_select(
    "event",
    "Event:",
    {},
)

# Create stages selector
ui.input_select(
    "stages",
    "Stages:",
    {},
)


@reactive.calc
@reactive.event(input.season)
def season_data():
    wrc.initialise(year=input.season())
    # WRC API data fetch
    season = wrc.getResultsCalendar()
    return season


@reactive.event(input.event)
def stages_data():
    wrc.rallyId = input.event()
    wrc.eventId = wrc.rallyId2eventId[wrc.rallyId]
    # WRC API data fetch
    stages = wrc.getStageDetails(update=True)
    return stages


@reactive.event(input.event)
def itinerary_data():
    wrc.rallyId = input.event()
    wrc.eventId = wrc.rallyId2eventId[wrc.rallyId]
    # WRC API data fetch
    itinerary = wrc.getItinerary(update=True)
    return itinerary


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
    ui.update_select("stages", choices=stages)


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
