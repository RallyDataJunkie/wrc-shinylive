from shiny import render, reactive
from shiny.express import ui, input
from shiny import ui as uis
from wrc_rallydj.utils import (
    enrich_stage_winners,
    format_timedelta,
    dateNow,
    is_date_in_range,
)

from wrcapi_rallydj.data_api import WRCDataAPIClient

from datetime import datetime
from icons import question_circle_fill
from pandas import DataFrame, isna, to_numeric, to_datetime, NA
from seaborn import heatmap
from matplotlib.colors import LinearSegmentedColormap, TwoSlopeNorm

import math

import re
from rules_processor import (
    Nth,
    nth,
    p,
    numToWords,
    andList,
    core_stage,
    process_rally_overall_rules,
)

from ipyleaflet import Map, Marker, DivIcon
import matplotlib.pyplot as plt

from requests_cache import CachedSession

session = CachedSession(expire_after=5)

## Heros and banners
from .app_heroes import (
    get_overall_result_hero,
    get_stage_result_hero,
    get_rebased_driver_hero,
)

# Charts
from .app_charts import (
    empty_plot,
    chart_seaborn_linechart_stage_progress_positions,
    chart_seaborn_linechart_stage_progress_typ,
    chart_seaborn_barplot_stagetimes,
    chart_plot_driver_stagewins,
    chart_seaborn_linechart_split_positions,
    chart_seaborn_barplot_splits,
    chart_seaborn_linechart_splits,
    chart_plot_split_dists,
)

# Tables
from .app_tables import df_color_gradient_styler

from shinywidgets import render_widget

# Remarks
from .remarks import (
    season_background_remarks,
    season_overview_remarks,
    event_background_remarks,
    stage_times_remarks,
)

# Interpretations
from .interpretations import (
    about_md,
    rally_progression_interpretation_md,
    rally_progression_base_interpretation_md,
    rally_progression_report_interpretation_md,
    rally_progression_heatmap_interpretation_md,
    rally_progression_linechart_interpretation_md,
    stage_progression_rebase_select_md,
    stage_progression_interpretation_md,
    stage_progression_linechart_interpretation_md,
    stage_progression_barchart_interpretation_md,
    split_times_heat_interpretation_md,
    live_map_interpretation_md,
    micro_splits_base_interpretation_md,
)

# from itables.widget import ITable

from wrc_rallydj.livetiming_api2 import WRCTimingResultsAPIClientV2

wrc = WRCTimingResultsAPIClientV2(
    use_cache=True, backend="memory", expire_after=30, liveCatchup=True
)

wrcapi = WRCDataAPIClient(usegeo=True)

progression_report_types = {
    "bystagetime": "timeInS",
    "bystagepos": "position",
    "bystagegap": "Gap",
    "bystagediff": "Diff",
    "bystagechase": "Chase",  # TO DO
    "byrallytime": "timeInS",
    "byrallypos": "position",
    "byrallyclassposs": "categoryPosition",
    "byrallygap": "Gap",
    "byrallydiff": "Diff",
    "byrallychase": "Chase",  # TO DO
}

INTEPRETATION_PANEL_COLOUR = "honeydew"

ui.panel_title("RallyDataJunkie WRC Results and Timing Browser", "WRC-RallyDJ")

# TO DO - in creating dropdowns, where index values are integers they should be set to strings BUT
# this should be done in an immutable way and not modify the original dict integer values


def seasonInfo(updateDB=False):
    return wrc.getSeasons(updateDB=updateDB)


with ui.sidebar(open="desktop"):
    # Create season selector
    # Currently offers a hard coded set of season year options
    ui.input_select(
        "year",
        "Year:",
        {},
    )

    ui.input_select(
        "rally_seasonId",
        "Championship:",
        {},
    )

    # TO DO - Category should be condensed from full list of championships
    ui.input_select(
        "season_round",
        "Rounds:",
        {},
    )

    ui.input_select("category", "Category:", {})

    ui.input_select("event_day", "Day:", {})

    ui.input_select("event_section", "Section:", {})

    # Create stages selector
    ui.input_select(
        "stage",
        "Stage:",
        {},
    )

    with ui.tooltip(id="interpretation_prompt_switch_tt"):
        ui.input_switch(
            "interpretation_prompt_switch", "Show interpretation prompts", True
        )
        ui.span(
            "This application contains a lot of different ways of representing and viewing the timing and results data provided by the WRC. Interpretation prompts are used to support underestanding, and may include brief descriptions of the data used to generate the display, explanations of settings, hints on what to look for in the data, what stories it may help you to tell, etc."
        )

# Some reactive calculations


@reactive.calc
@reactive.event(input.year, input.season_round, input.category)
def getWRCAPI2event():
    if not input.year() or not input.category() or not input.season_round():
        return {}

    # TO DO year, sas-eventid is round, typ is .upper() on championship
    # Could we get a race here from wrc.championship?
    r = wrcapi.get_rallies_data(int(input.year()), typ=wrc.championship.upper())
    r = r[r["sas-eventid"].astype(str) == str(input.season_round())]
    retval = r.to_dict(orient="records")[0] if not r.empty else {}
    return retval


@reactive.calc
@reactive.event(getWRCAPI2event)
def rally_geodata():
    geodata = getWRCAPI2event()
    if "kmlfile" in geodata:
        kmlstub = geodata["kmlfile"]
        geostages = wrcapi.read_kmlfile(kmlstub)
        return geostages
    return DataFrame()


# The accordion widget provides collapsible elements
with ui.accordion(open=False):
    with ui.accordion_panel("About"):

        @render.ui
        def about():
            txt = about_md
            return ui.markdown(txt)

    with ui.accordion_panel("Season info"):

        @render.ui
        @reactive.event(input.rally_seasonId)
        def season_headline():
            seasonId = input.rally_seasonId()
            if not seasonId:
                return "*No season available.*"
            seasonId = int(seasonId)
            season_rounds = wrc.getSeasonRounds(seasonId=seasonId)

            if season_rounds.empty:
                return "*No season information available.*"

            seasons_info = seasonInfo()
            season_info = seasons_info[seasons_info["seasonId"] == seasonId].iloc[0]
            md = f"""__{season_info["year"]} {season_info["name"]}__"""

            if seasons_info["name"].nunique() > 1:
                others_ = [
                    f"`{c}`"
                    for c in seasons_info["name"].unique()
                    if c != season_info["name"]
                ]
                md = f"""{md}\n\n*Also available, data for {andList(others_)}.*"""

            return ui.markdown(md)

        with ui.accordion(open=False, id="season_background_accordion"):
            with ui.accordion_panel("Season Background"):

                @render.ui
                @reactive.event(input.rally_seasonId)
                def app_season_background_remarks():
                    seasonId = input.rally_seasonId()
                    if not seasonId:
                        return "*No season available.*"
                    md = season_background_remarks(wrc, int(seasonId))

                    return ui.markdown(f"""{md}\n\n""")

        # TO DO - remark on number of completed rounds
        # TO DO - remark on championship lead
        # TO DO - remark on next upcoming round

        with ui.accordion(open=False, id="season_overview_accordion"):
            with ui.accordion_panel("Season Overview"):

                @render.ui
                def app_season_overview_remarks():
                    seasonId = input.rally_seasonId()
                    if not seasonId:
                        return "*No season available.*"
                    md = season_overview_remarks(wrc, seasonId)
                    return ui.markdown(f"""{md}\n\n""")

        with ui.accordion(open=False, id="season_elements_accordion"):
            with ui.accordion_panel("Season Events"):

                @render.data_frame
                @reactive.event(input.rally_seasonId)
                def season_frame():
                    season = wrc.getSeasonRounds()
                    if not season.empty:
                        retcols = [
                            "order",
                            "name",
                            "country.name",
                            "location",
                            "startDate",
                            "surfaces",
                        ]
                        return render.DataGrid(season[retcols])

                    return "No season information available."

            with ui.accordion_panel("Season Event Winners"):

                @render.data_frame
                @reactive.event(input.rally_seasonId, input.championships)
                def season_winners():
                    season_winners = wrc.getChampionshipByRound(
                        on_season=True, on_championship=True, raw=False
                    )
                    if season_winners.empty:
                        return
                    season_winners = season_winners[season_winners["position"] == 1]
                    retcols = [
                        "eventName",
                        "startDate",
                        "LastName",
                        "totalPoints",
                        "pointsBreakdown",
                        "surfaces",
                    ]
                    return render.DataGrid(season_winners[retcols])

    with ui.accordion_panel("Championship results"):
        # TO DO  - select championships based on wrc.getChampionships() ?
        # TO DO wrc.setChampionship()
        # wrc.getChampionshipOverall() gives championshipEntryId
        # wrc.getChampionshipByRound() gives championshipEntryId
        # JOIN wrc.getDrivers(by_championship=True) gives personId, name etc
        # JOIN wrc.getChampionshipEntries gives championshipEntryId, personId map
        # wrc.getChampionshipRounds()

        # @render.ui
        # @reactive.event(input.rally_seasonId, input.championships)
        # def championship_info():
        # TO DO this is not set for ERC
        # wrc.setChampionship()
        #    getChampionships()
        #    return ui.markdown(
        #        f"__{wrc.championshipName}__"
        #    )

        # Create championships selector
        ui.input_select(
            "championships",
            "Championships:",
            {},
        )
        # @render.data_frame
        # @reactive.event(
        #    input.rally_seasonId,
        #    input.category,
        # )
        # def show_championships():
        #    seasonId = input.rally_seasonId()
        #    if not seasonId:
        #        return
        #    championships_df = getChampionships()
        #    cols = ["name", "type"] #champioshipId
        #    return render.DataGrid(championships_df[cols])

        ui.markdown("__Championship Overall Points__")
        ui.markdown("*TO DO - points at end of selected round*")

        @render.data_frame
        @reactive.event(
            input.rally_seasonId,
            input.category,
            input.season_round,
            input.championships,
        )
        def championship_overall_frame():
            seasonId = input.rally_seasonId()
            eventId = input.season_round()
            championshipId = input.championships()
            if not seasonId or not eventId or not championshipId:
                return
            # The following is the latest overall, not keyed by anything
            # need a switch for ERC etc
            # Need a different function if we specify at a particular round
            eventId = int(eventId)
            championshipId = int(championshipId)
            wrc.setChampionship(championshipId=championshipId)
            # TO DO cache championship and set it
            # championships = getChampionships()
            # championshipId = championships[championships["type"]=="Drivers"]["championshipId"].iloc[0]

            championship_overall = wrc.getChampionshipOverall(
                championshipId=championshipId, raw=False
            )

            # TO DO limit columns - may be manufacturers etc etc
            cols = [
                "Round",
                "overallPosition",
                "LastName",
                "Manufacturer",
                "Team",
                "TyreManufacturer",
                "overallPoints",
            ]
            cols = [c for c in cols if c in championship_overall.columns]
            # return render.DataGrid(championship_overall[cols].sort_values("overallPosition"))
            return render.DataGrid(
                championship_overall[cols].sort_values("overallPosition")
            )

    with ui.accordion_panel("Event overview"):

        @render.ui
        @reactive.event(input.season_round)
        def rally_overview_hero():
            eventId = int(input.season_round())
            # eventData = getEventData()
            season = wrc.getSeasonRounds()

            event = season.loc[season["eventId"] == eventId].iloc[0]

            so = ui.value_box(
                title=event["startDate"],
                value=event["name"],
                theme="text-black",
                showcase=ui.markdown(f"__ROUND {event['order']}__"),
                showcase_layout="left center",
                full_screen=True,
            )
            return so

        # TO DO - There is ambiguity here; a stage may be running or cancelled etc
        # but we may have all the resultd in for the priority group
        ui.input_checkbox(
            "display_latest_overall",
            "Use most recent data",
            True,
        )

        @render.ui
        @reactive.event(input.stage, input.display_latest_overall, input.category)
        def rally_overview_latest_hero():
            # TO DO - for winner give overall stage distance, av speed, av pace
            # TO DO for 2nd / 3rd, av speed, av pace delta
            stageId, stagesInfo, overallResults = getOverallStageResultsData()
            if not overallResults.empty:
                return get_overall_result_hero(stageId, stagesInfo, overallResults)
            else:
                return ui.markdown("No results yet available for this event.")

        with ui.accordion(open=False, id="event_accordion"):

            with ui.accordion_panel("Event background"):

                @render.ui
                @reactive.event(input.season_round)
                def app_event_background_remarks():
                    eventId = input.season_round()
                    if not eventId:
                        return ui.markdown("\n\nNo event info...")

                    md = event_background_remarks(wrc, eventId)

                    # TO DO - add itinerary remarks if available

                    return ui.markdown(md)

            # TO DO - overall report
            # TO DO - day report
            # TO DO - section/loop report
            with ui.accordion_panel("Event review"):

                @render.ui
                @reactive.event(input.season_round, input.stage)
                def app_event_review_remarks():
                    eventId = input.season_round()
                    if not eventId:
                        return ui.markdown("*No event info...*")

                    # BROKEN - sometimes fails and returns blank?

                    season_rounds = wrc.getSeasonRounds(on_season=True)

                    if season_rounds.empty:
                        return ui.markdown("*No season information.*")

                    md = []

                    event_df = season_rounds[season_rounds["eventId"] == int(eventId)]

                    event = event_df.iloc[0]

                    # md.append(f"""eventId {eventId}: {event_df.to_dict()}""")

                    # Event status: -1 has run, 0 running, +1 to run
                    in_range = is_date_in_range(
                        datetime.now(), event.to_dict(), tristate=True
                    )
                    base_md = f""" TO DO {eventId} """
                    # md.append(base_md)

                    if in_range <= 0:
                        if not input.stage():
                            return ui.markdown("*Awaiting stage detail...*")
                        setStageData()
                        stagesInfo = wrc.getStageInfo(on_event=True)

                        if not stagesInfo.empty:
                            stagesInfo = stagesInfo.sort_values(
                                by="number", ascending=True
                            )

                        overallResults = wrc.getStageOverallResults(
                            raw=False, last=True
                        )
                        if not overallResults.empty:
                            # HACK TO DO TO REMOVE
                            print(overallResults.iloc[-1].to_dict())

                    if wrc.isRallyInDate():
                        event_status_ = "*__This event is currently running.__*"
                        md.append(event_status_)

                        stagesInfo["code_name"] = stagesInfo.apply(
                            lambda row: f"{row['code']} {row['name']}", axis=1
                        )
                        completed_stages = stagesInfo[
                            ~stagesInfo["status"]
                            .str.lower()
                            .isin(["to run", "running"])
                        ]

                        if not completed_stages.empty:
                            completed_stages_ = f"""{numToWords(completed_stages.shape[0]).capitalize()} stages have run so far: {andList(completed_stages["code_name"].to_list())} """
                            md.append(completed_stages_)

                        currently_running = stagesInfo[
                            stagesInfo["status"].str.lower().isin(["running"])
                        ]
                        if not currently_running.empty:
                            currently_running_ = f"""{numToWords(currently_running.shape[0]).capitalize()} {p.plural("stage", currently_running.shape[0])} currently running ({andList(currently_running["code_name"].to_list())})"""
                            md.append(currently_running_)

                        to_run = stagesInfo[
                            stagesInfo["status"].str.lower().isin(["to run"])
                        ]
                        if not to_run.empty:
                            if to_run.shape[0] == 1:
                                to_run_ = f"""There is just one stage left to run: {to_run["code_name"].iloc[0]}."""
                            else:
                                to_run_ = f"""{numToWords(to_run.shape[0]).capitalize()} {p.plural("stage", to_run.shape[0])} still to run"""
                            md.append(to_run_)

                    elif in_range == -1:
                        event_status_ = "*This event is now over.*"
                        superspecials = stagesInfo[
                            stagesInfo["stageType"] == "SuperSpecialStage"
                        ]
                        if not superspecials.empty:
                            superspecial_ = f""", including __{numToWords(superspecials.shape[0])} superspecial {p.plural("stage",superspecials.shape[0])}__,"""
                        else:
                            superspecial_ = ""
                        # TO DO  if in priority need to use class position

                        finalStageOverallWinner = overallResults[
                            overallResults["position"] == 1
                        ].iloc[-1]
                        previous_stages_ = f"""Comprising __{numToWords(stagesInfo["code"].shape[0])} competitive {p.plural("stage", stagesInfo["code"].shape[0])}__{superspecial_} over a total competitive rally distance of __{stagesInfo["distance"].sum().round(1)} km__, *{event["name"]}* was won by __{finalStageOverallWinner["driverName"]}__ and co-driver __{finalStageOverallWinner["codriverName"]}__ in a __{finalStageOverallWinner["entrantName"]} *{finalStageOverallWinner["vehicleModel"]}*__ with an overall rally time of {finalStageOverallWinner["stageTime"]} ({finalStageOverallWinner["penaltyTime"]} penalties)."""
                        # Possible comment about team dominance. In terms of team standings, tool all three podium positions, top four etx
                        md.append(previous_stages_)

                    elif in_range == 1:
                        event_status_ = "*This event has not yet started.*"
                    else:
                        event_status_ = ""

                    md.append(event_status_)

                    # Tidy out any empty strings
                    md = [md_ for md_ in md if md_]

                    md = "\n\n".join(md)
                    # TO DO Start to explore md cleaning, just in case
                    md = md.replace(" ,", ",")

                    return ui.markdown(md)

            with ui.accordion_panel("Event stages map"):

                @render_widget
                @reactive.event(rally_geodata)
                def allstages_map():
                    geostages = rally_geodata()
                    # eventgeodata = getWRCAPI2event()
                    # poi_df = wrcapi.get_poilist_data(eventgeodata["poilistid"])
                    labelcoords = list(zip(geostages["name"], geostages["start"]))
                    m = wrcapi.GeoTools.simple_stage_map(
                        geostages, labelcoords=labelcoords
                    )
                    return m

            with ui.accordion_panel("Event stage times"):

                @render.ui
                def app_stage_times_remarks():
                    md = stage_times_remarks(wrc)
                    # also include status
                    return ui.markdown(md)

            with ui.accordion_panel("Event results"):

                @render.data_frame
                @reactive.event(
                    input.stage,
                    input.category,
                    input.display_latest_overall,
                )
                def event_results_frame():
                    setStageData()
                    _, _, overallResults = getOverallStageResultsData()
                    inputCategory = input.category()
                    pos = "position" if inputCategory == "P0" else "categoryPosition"
                    cols = [
                        "Position",
                        "driverCode",
                        "carNo",
                        "driverName",
                        "stageTime",
                        "stageCode",
                        "diffFirst",
                        "diffPrev",
                        "penaltyTime",
                        "vehicleModel",
                        "entrantName",
                    ]
                    if overallResults.empty:
                        return

                    overallResults = overallResults.copy().rename(
                        columns={pos: "Position"}
                    )
                    # TO DO also add retirements onto end, reverse ordered by how far they got through rally
                    return render.DataGrid(overallResults[cols])

        # TO DO  if still stages to run, give eg 7 stages / 120km of 18 stages / 250km completed, 11 stages / 130km still to run. BUT how to handle cancelled stages?
        # TO DO remarks regarding how much pace required to make back time over N stages.
        with ui.accordion(open=False, id="event_championship_points_accordion"):
            with ui.accordion_panel("Championship Points on Event"):
                # Create championships selector
                # TO DO - should this be limited by Category?
                ui.input_select(
                    "event_championships",
                    "Championships:",
                    {},
                )

                @render.data_frame
                @reactive.event(
                    input.rally_seasonId,
                    input.category,
                    input.season_round,
                    input.event_championships,
                )
                def championship_event_frame():
                    seasonId = input.rally_seasonId()
                    eventId = input.season_round()
                    championshipId = input.event_championships()
                    if not seasonId or not eventId:
                        return
                    # The following is the latest overall, not keyed by anything
                    # need a switch for ERC etc
                    # Need a different function if we specify at a particular round
                    eventId = int(eventId)
                    championshipId = int(championshipId)
                    wrc.setChampionship(championshipId=championshipId)
                    # TO DO cache championship and set it
                    # championships = getChampionships()

                    # championshipId = championships[championships["type"] == "Drivers"][
                    #    "championshipId"
                    # ].iloc[0]
                    championship_event = wrc.getChampionshipByRound(
                        championshipId=championshipId,
                        eventId=eventId,
                        raw=False,
                        # on_event=True
                    )
                    if championship_event.empty:
                        return

                    # TO DO limit columns
                    cols = [
                        "position",
                        "LastName",
                        "TyreManufacturer",
                        "Manufacturer",
                        "Team",
                        "totalPoints",
                        "pointsBreakdown",
                    ]
                    for c in ["Manufacturer", "Team", "TyreManufacturer"]:
                        if c in wrc.championshipName:
                            cols = [c, "totalPoints", "pointsBreakdown", "position"]

                    cols = [c for c in cols if c in championship_event.columns]

                    def custom_sort_key(x):
                        if isna(x) or x == "":
                            return (
                                2,
                                0,
                            )  # Empty values last, with value 0 as secondary key
                        elif x == "R":
                            return (
                                1,
                                0,
                            )  # "R" values in the middle, with value 0 as secondary key
                        else:
                            try:
                                # Integers first, sorted by their value
                                return (0, int(x))
                            except:
                                # Any other values (shouldn't happen in your case)
                                return (3, str(x))

                    # TO DO fix cols filter for eg manufacturers etc
                    return render.DataGrid(
                        # championship_event[championship_event["status"] != "DidNotEnter"][
                        #    cols
                        # ].sort_values("position", key=lambda x: x.map(custom_sort_key))
                        championship_event[
                            championship_event["status"] != "DidNotEnter"
                        ][cols].sort_values(
                            ["totalPoints", "pointsBreakdown"], ascending=False
                        )
                    )
                    # return render.DataGrid(championship_event[cols].sort_values("position"))

        with ui.accordion(open=False, id="rally_progression_accordion"):
            with ui.accordion_panel("Rally progression"):

                @render.express
                @reactive.event(input.interpretation_prompt_switch)
                def progression_interpretation_container():
                    ui.input_switch(
                        "progression_interpretation_switch",
                        "Show interpretation prompts",
                        False,
                    )

                @render.ui
                @reactive.event(input.progression_interpretation_switch)
                def progression_interpretation():
                    if input.progression_interpretation_switch():
                        md = rally_progression_interpretation_md
                        return ui.markdown(
                            f"""<hr/>\n\n<div style="background-color:{INTEPRETATION_PANEL_COLOUR}">{md}</div>\n\n<hr/>\n\n"""
                        )

                # TO DO - tidy up  reactive.event
                # Move get_overall_pos_wide declaration up the page and use it in decorator
                @render.plot(alt="Line chart of overall rally positions.")
                @reactive.event(
                    input.category,
                    input.stage,
                    input.event_day,
                    input.event_section,
                    input.progression_report_type,
                    input.display_latest_overall,
                )
                def seaborn_linechart_stage_progress_positions():
                    overall_times_wide = get_overall_pos_wide()
                    if overall_times_wide.empty:
                        return empty_plot(title="No overall stage times...")
                    ax = chart_seaborn_linechart_stage_progress_positions(
                        wrc, overall_times_wide
                    )
                    ax.tick_params(axis="x", labelrotation=45)
                    return ax

                with ui.tooltip(id="progression_report_type_tt"):
                    ui.input_select(
                        "progression_report_type",
                        "Progression report type:",
                        {
                            "bystagetime": "Stage time (s)",
                            "bystagepos": "Stage position",
                            "bystagegap": "Stage gap (s)",
                            "bystagediff": "Stage diff (s)",
                            "bystagechase": "Stage chase (s)",
                            "byrallytime": "Overall rally time (s)",
                            "byrallypos": "Overall rally position",
                            "byrallyclassposs": "Overall rally class position",
                            "byrallygap": "Overall rally gap (s)",
                            "byrallydiff": "Overall rally diff (s)",
                            "byrallychase": "Overall rally chase (s)",
                        },
                        selected="byrallytime",
                    ),
                    "Progression report type; dimension to be displayed. Use stage basis for summary reporting of individual stages, progression bases for reporting on rally progression."

                @render.express
                @reactive.event(input.interpretation_prompt_switch)
                def progression_report_interpretation_container():
                    ui.input_switch(
                        "progression_report_interpretation_switch",
                        "Show interpretation prompts",
                        False,
                    )

                @render.ui
                @reactive.event(input.progression_report_interpretation_switch)
                def progression_report_interpretation():
                    if input.progression_report_interpretation_switch():
                        md = rally_progression_report_interpretation_md

                        return ui.markdown(
                            f"""<hr/>\n\n<div style="background-color:{INTEPRETATION_PANEL_COLOUR}">{md}</div>\n\n<hr/>\n\n"""
                        )

                # TO DO implement report by type

                @render.data_frame
                @reactive.event(
                    input.category,
                    input.stage,
                    input.event_day,
                    input.event_section,
                    input.progression_report_type,
                )
                def stage_progress_frame():
                    overall_typ_wide = get_overall_typ_wide()
                    if overall_typ_wide.empty:
                        return
                    # TO DO - this should have options for within and accumulated statge time views
                    # as well as a driver rebase option
                    # TO DO - this should be sorted by position ASC for the latest stage
                    # How is this done on the seaborn_linechart_stage_progress_positions label positions?
                    # TO DO  - sort by each stage column
                    stage_cols = wrc.getStageCols(overall_typ_wide)
                    stage_cols.reverse()
                    return render.DataGrid(
                        overall_typ_wide.copy()
                        .drop(columns="entryId")
                        .sort_values(stage_cols)
                    )

            with ui.accordion_panel("Rally progression rebase"):

                @render.express
                @reactive.event(input.interpretation_prompt_switch)
                def rally_progression_report_base_interpretation_container():
                    ui.input_switch(
                        "progression_base_interpretation_switch",
                        "Rally progression rebase notes",
                        False,
                    )

                @render.ui
                @reactive.event(input.progression_base_interpretation_switch)
                def rally_progression_report_base_interpretation():
                    if input.progression_base_interpretation_switch():
                        md = rally_progression_base_interpretation_md

                        return ui.markdown(
                            f"""<hr/>\n\n<div style="background-color:{INTEPRETATION_PANEL_COLOUR}">{md}</div>\n\n<hr/>\n\n"""
                        )

                with ui.tooltip(id="progression_rebase_type_tt"):
                    ui.input_select(
                        "progression_rebase_type",
                        "Progression rebase type:",
                        {
                            "bystagetime": "Stage time",
                            "byrallytime": "Overall rally time",
                        },
                        selected="bystagetime",
                    ),
                    "Progression rebase type; dimension to be rebased. Use stage basis for summary reporting of individual stages, progression bases for reporting on rally progression."

                # Create stage driver rebase selector
                ui.input_select(
                    "rally_progression_rebase_driver",
                    "Driver rebase:",
                    {},
                )

                with ui.tooltip(id="rprog_rebase_incols_tt"):
                    ui.input_checkbox(
                        "rprog_rebase_incols",
                        "Within column heatmap",
                        True,
                    ),
                    "Create heatmap palette within a column rather than across all columns."

                with ui.tooltip(id="rebase_rally_progression_reverse_palette_tt"):
                    ui.input_checkbox(
                        "rebase_rally_progression_reverse_palette",
                        "Reverse rebase palette",
                        False,
                    ),
                    "Reverse the rebase palette to show deltas relative to the rebased driver's perspective."

                @render.express
                @reactive.event(input.interpretation_prompt_switch)
                def rally_progression_report_heatmap_interpretation_container():
                    ui.input_switch(
                        "rally_progression_heatmap_interpretation_switch",
                        "Show interpretation prompts",
                        False,
                    )

                @render.ui
                @reactive.event(input.rally_progression_heatmap_interpretation_switch)
                def rally_progression_report_heatmap_interpretation():
                    if input.rally_progression_heatmap_interpretation_switch():

                        md = rally_progression_heatmap_interpretation_md

                        ui.markdown(
                            f"""<hr/>\n\n<div style="background-color:{INTEPRETATION_PANEL_COLOUR}">{md}</div>\n\n<hr/>\n\n"""
                        )

                @render.ui
                @reactive.event(
                    input.category,
                    input.stage,
                    input.event_day,
                    input.event_section,
                    input.progression_rebase_type,
                    input.rally_progression_rebase_driver,
                    input.rprog_rebase_incols,
                    input.display_latest_overall,
                    input.rebase_rally_progression_reverse_palette,
                )
                def stage_progression_heat():
                    progression_rebase_type = input.progression_rebase_type()
                    rebase_reverse_palette = (
                        input.rebase_rally_progression_reverse_palette()
                    )
                    overall_typ_wide = get_overall_typ_wide2_rebased()
                    if not progression_rebase_type or overall_typ_wide.empty:
                        return

                    stage_cols = wrc.getStageCols(overall_typ_wide)
                    html = (
                        df_color_gradient_styler(
                            overall_typ_wide.drop(columns="entryId").sort_values(
                                stage_cols[::-1]
                            ),
                            cols=stage_cols,
                            within_cols_gradient=input.rprog_rebase_incols(),
                            reverse_palette=rebase_reverse_palette,
                        )
                        .hide()
                        .to_html()
                    )
                    return ui.HTML(html)

                @render.express
                @reactive.event(input.interpretation_prompt_switch)
                def rally_progression_report_linechart_interpretation_container():
                    ui.input_switch(
                        "rally_progression_linechart_interpretation_switch",
                        "Show interpretation prompts",
                        False,
                    )

                @render.ui
                @reactive.event(input.rally_progression_linechart_interpretation_switch)
                def rally_progression_report_linechartinterpretation():
                    if input.rally_progression_linechart_interpretation_switch():
                        md = rally_progression_linechart_interpretation_md

                        ui.markdown(
                            f"""<hr/>\n\n<div style="background-color:{INTEPRETATION_PANEL_COLOUR}">{md}</div>\n\n<hr/>\n\n"""
                        )

                @render.plot(
                    alt="Line chart of rally progression of selected dimension."
                )
                @reactive.event(
                    input.category,
                    input.stage,
                    input.event_day,
                    input.event_section,
                    # input.progression_rebase_type,
                    input.rally_progression_rebase_driver,
                    input.display_latest_overall,
                )
                def seaborn_linechart_stage_typ():
                    # HACK this chart is harwdired:
                    # - line chart makes no sense for stage progression
                    overall_typ_wide = get_overall_typ_wide3_rebased()
                    progression_type = "byrallytime"  # input.progression_rebase_type()
                    if overall_typ_wide.empty or not progression_type:
                        return empty_plot(title="Missing data / selection...")
                    typ = progression_report_types[progression_type]
                    overall_typ_wide = overall_typ_wide.copy()
                    # Replace values > 100 with NaN or clip them
                    stage_cols = wrc.getStageCols(overall_typ_wide)
                    THRESHOLD = 100
                    overall_typ_wide[stage_cols] = overall_typ_wide[stage_cols].where(
                        overall_typ_wide[stage_cols] <= THRESHOLD, NA
                    )

                    ax = chart_seaborn_linechart_stage_progress_typ(
                        wrc, overall_typ_wide, typ, greyupper=True
                    )
                    ax.tick_params(axis="x", labelrotation=45)
                    return ax

        with ui.accordion(open=False, id="stage_info_accordion"):
            with ui.accordion_panel("Event details"):

                with ui.accordion(open=False, id="stage_accordion"):

                    with ui.accordion_panel("Stages info"):

                        @render.data_frame
                        @reactive.event(
                            input.stage, input.event_day, input.event_section
                        )
                        def stages_frame():
                            setStageData()
                            itineraryLegId = input.event_day()
                            itinerarySectionId = input.event_section()
                            if itineraryLegId:
                                itineraryLegId = int(itineraryLegId)
                                itinerarySectionId = int(itinerarySectionId)
                            stages = wrc.getStageInfo(
                                itineraryLegId=itineraryLegId,
                                itinerarySectionId=itinerarySectionId,
                                raw=False,
                            )
                            if stages.empty:
                                return
                            retcols = [
                                "code",
                                "name",
                                # "day",
                                "distance",
                                "stageType",
                                "status",
                                "day",
                                "sectionName",
                            ]

                            return render.DataGrid(stages[retcols])

                    with ui.accordion_panel(title="Itinerary"):

                        @render.data_frame
                        def itinerary_frame():
                            itinerary = getItinerary()
                            if itinerary.empty:
                                return

                            retcols = [
                                "name",
                                "code",
                                "type",
                                "location",
                                "distance",
                                "firstCarDueDateTime",
                                "targetDuration",
                                "timingPrecision",
                                "controlPenalties",
                                "status",
                            ]
                            return render.DataGrid(itinerary[retcols])

                    with ui.accordion_panel("Startlist"):

                        # Create startlist type selector
                        # Dynamically populated using available startlists
                        ui.input_select("startlist", "Startlist:", {})

                        @render.data_frame
                        @reactive.event(
                            input.startlist, input.season_round, input.event_day
                        )
                        def startlist_frame():
                            startlist = wrc.getStartList(raw=False)
                            if startlist.empty:
                                return
                            retcols = [
                                "order",
                                "carNo",
                                "driverName",
                                "startDateTimeLocal",
                                "codriverName",
                                "manufacturerName",
                                "entrantName",
                                "vehicleModel",
                                "eligibility",
                                "priority",
                                # "groupClass",
                            ]
                            startlist = startlist[
                                startlist["name"] == input.startlist()
                            ]
                            return render.DataGrid(startlist[retcols])

                    with ui.accordion_panel("Shakedown"):
                        # TO DO in ERC the Qualifying stage (QS) is shakedown run number 2
                        @render.data_frame
                        @reactive.event(
                            input.stage_review_accordion, input.category, input.stage
                        )
                        def shakedown_short():
                            eventId = input.season_round()
                            priority = input.category()
                            if not eventId:
                                return
                            eventId = int(eventId) if eventId else eventId

                            shakedown_df = wrc.getEventShakeDownTimes(
                                eventId=eventId, priority=priority, raw=False
                            )
                            if shakedown_df.empty:
                                return
                            shakedown_df["runDurationMs"] = shakedown_df[
                                "runDurationMs"
                            ].apply(format_timedelta)
                            shakedown_df_wide = shakedown_df.pivot(
                                index=[
                                    "driverCode",
                                    "driverName",
                                    "codriverName",
                                    "manufacturerName",
                                    "entrantName",
                                    "vehicleModel",
                                    "carNo",
                                ],
                                columns="runNumber",
                                values="runDurationMs",
                            )

                            # Rename the columns to run1, run2, etc.
                            shakedown_df_wide.columns = [
                                f"run{col}" for col in shakedown_df_wide.columns
                            ]
                            shakedown_df_wide.reset_index(inplace=True)
                            return render.DataGrid(shakedown_df_wide)

                        @render.data_frame
                        @reactive.event(
                            input.stage_review_accordion, input.category, input.stage
                        )
                        def shakedown_long():
                            eventId = input.season_round()
                            priority = input.category()
                            if not eventId:
                                return
                            eventId = int(eventId) if eventId else eventId

                            shakedown_df = wrc.getEventShakeDownTimes(
                                eventId=eventId, priority=priority, raw=False
                            )
                            if shakedown_df.empty:
                                return
                            shakedown_df = shakedown_df.sort_values("runDurationMs")
                            shakedown_df["runTime"] = shakedown_df[
                                "runDurationMs"
                            ].apply(format_timedelta)
                            cols = [
                                "carNo",
                                "driverCode",
                                "shakedownNumber",
                                "runNumber",
                                "runTime",
                                "driverName",
                                "codriverName",
                                "manufacturerName",
                                "entrantName",
                                "vehicleModel",
                            ]
                            return render.DataGrid(shakedown_df[cols])

                    with ui.accordion_panel("Stage winners"):

                        @render.data_frame
                        def stage_winners_short():
                            stagewinners = getStageWinners()
                            if stagewinners.empty:
                                return
                            # TO DO - need enrichers
                            retcols = [
                                # "stageType",
                                "code",
                                "stageName",
                                "driverName",
                                "codriverName",
                                "manufacturerName",
                                "entrantName",
                                "vehicleModel",
                                "elapsedDuration",
                                "day",
                                "sectionName",
                                "carNo",
                                # "time",
                                # "eligibility",
                                "wins_overall",
                                "daily_wins",
                                "timeInS",
                                "distance",
                                "pace (s/km)",
                                "speed (km/h)",
                            ]
                            # TO DO have option to limit view of stages up to and including selected stage
                            return render.DataGrid(stagewinners[retcols])

                        @render.plot(alt="Bar chart of stage wins.")
                        def plot_driver_stagewins():
                            stage_winners = getStageWinners()
                            if stage_winners.empty:
                                return empty_plot(title="No stage winners data...")
                            # TO DO - make use of commented out elements
                            # which limit counts  up to and including current stage
                            # stage_winners["_stagenum"] = stage_winners["stageNo"].str.replace("SS", "")
                            # stage_winners["_stagenum"] = stage_winners["_stagenum"].astype(int)

                            # idx = stage_winners[stage_winners["stageId"] == input.stage()].index
                            # if len(idx) == 0:
                            #    return]

                            # Drop empty rows
                            # stage_winners = stage_winners[stage_winners["carNo"].str.strip() != ""]
                            ax = chart_plot_driver_stagewins(stage_winners)
                            return ax

                    with ui.accordion_panel("Retirements"):
                        # Try to be sensible about how often we call
                        # getRetirements and getPenalties
                        # If the event has completed
                        # we need only do this one
                        @render.data_frame
                        @reactive.event(
                            input.season_round, input.stage, input.stage_accordion
                        )
                        def retirements_frame():
                            if "Retirements" not in input.stage_accordion():
                                return
                            retirements = wrc.getRetirements(raw=False)
                            if retirements.empty:
                                return
                            retcols = [
                                "carNo",
                                "driverName",
                                "code",
                                "type",
                                "reason",
                                "retirementDateTime",
                                "status",
                                "codriverName",
                                "vehicleModel",
                                "location",
                                "manufacturerName",
                                "entrantName",
                            ]
                            return render.DataGrid(retirements[retcols])

                    with ui.accordion_panel("Penalties"):

                        @render.data_frame
                        @reactive.event(
                            input.season_round, input.stage, input.stage_accordion
                        )
                        def penalties_frame():
                            if "Penalties" not in input.stage_accordion():
                                return
                            penalties = wrc.getPenalties(raw=False)
                            if penalties.empty:
                                return
                            retcols = [
                                "carNo",
                                "driverName",
                                "code",
                                "type",
                                "penaltyDuration",
                                "reason",
                                "codriverName",
                                "vehicleModel",
                                "location",
                                "manufacturerName",
                                "entrantName",
                            ]
                            return render.DataGrid(penalties[retcols])

    with ui.accordion_panel("Stage Review"):
        with ui.card(class_="mt-3"):

            # TO DO - do an stage summary hero, colour black

            @render.ui
            def stageresult_hero():
                setStageData()
                stagesInfo = wrc.getStageInfo(on_event=True).sort_values(
                    by="number", ascending=True
                )
                stageId = input.stage()
                priority = input.category()
                if stagesInfo.empty or not stageId or not priority:
                    return ui.markdown("*No results available for this stage.*")

                if stageId and not stagesInfo.empty:
                    stageId = int(stageId)
                stage_times_data = wrc.getStageTimes(
                    priority=priority, raw=False
                ).sort_values("position")
                if stage_times_data.empty:
                    return

                if priority != "P0":
                    stage_times_data["position"] = range(1, len(stage_times_data) + 1)
                    stage_times_data["diffFirstMs"] = (
                        stage_times_data["diffFirstMs"]
                        - stage_times_data["diffFirstMs"].iloc[0]
                    )
                return get_stage_result_hero(stageId, stagesInfo, stage_times_data)

            with ui.accordion(open=False, id="stage_review_accordion"):

                with ui.accordion_panel("Stage notes"):

                    @render.ui
                    def stage_text_intro():
                        md = []
                        stageId = input.stage()
                        if not stageId:
                            return ui.markdown("No stage to report on...")
                        stageId = int(stageId)

                        stages_info = wrc.getStageInfo(raw=False)
                        itinerary_df = getItinerary()

                        stages_info["stageInDay"] = (
                            stages_info.groupby(["day"]).cumcount() + 1
                        )

                        stage_info = stages_info[
                            stages_info["stageId"] == stageId
                        ].iloc[0]

                        stage_code = stage_info["code"]
                        ss_index = itinerary_df[
                            itinerary_df["code"] == stage_code
                        ].index[0]
                        itinerary_for_stage = itinerary_df.iloc[ss_index]

                        # Stage status
                        state_status = itinerary_df.iloc[ss_index]["status"]
                        stage_status_ = f" [{state_status}]" if state_status else ""
                        # Remark on stage name
                        stage_name = stage_info["name"]
                        _md = f"""*{stage_code} {stage_name} ({stage_info["distance"]}km)* {stage_status_}"""
                        md.append(_md)

                        # Remark on being the Nth stage of the day
                        nth_stage = f"""the {Nth(stage_info["stageInDay"])}"""
                        if (
                            stage_info["stageInDay"]
                            == stages_info[stages_info["day"] == stage_info["day"]][
                                "stageInDay"
                            ].max()
                        ):
                            nth_stage = f"{nth_stage}, and last,"

                        # Remark on stage start time
                        start_time = datetime.fromisoformat(
                            itinerary_for_stage["firstCarDueDateTime"]
                        )
                        time_str = (
                            start_time.strftime("First car due on stage at %I.%M%p.")
                            # .lower()
                            .replace(" 0", " ")
                        )
                        _md = f"""{stage_code} ({stage_info["day"]}), {nth_stage} stage of the day. {time_str}"""

                        # Remark on, or imply, the repeated run nature of this stage
                        repeated_run = re.match(r".*\s(\d+)\s*", stage_name)
                        run_number = (
                            Nth(int(repeated_run.group(1))) if repeated_run else "only"
                        )
                        _md = f"{_md} This is the {run_number} run of this stage."

                        # Remark on being the longest stage of the rally
                        if stage_info["distance"] == stages_info["distance"].max():
                            _md = f"{_md} *It is the longest stage on the rally.*"

                        # Remark on number of split points
                        splits_ = wrc.getStageSplitPoints(stageId=int(stageId))
                        if not splits_.empty:
                            splitsLen_ = len(splits_)
                            # For example: There are three split points, at Pkm, Qkm and Rkm.
                            _md = f"""{_md}. *En route*, there {p.plural("is", splitsLen_)} {p.number_to_words(splitsLen_)} split {p.plural("point", splitsLen_)} (at {andList(splits_["distance"].to_list())} km)."""

                        md.append(f"{_md}\n\n")

                        # Remark on previous liaison stage
                        previous_tc = itinerary_df.iloc[ss_index - 1]
                        previous_out = itinerary_df.iloc[ss_index - 2]
                        previous_location = (
                            f"previous *{previous_out['location']}* stage"
                            if previous_out["type"] == "FlyingFinish"
                            else f'*{previous_out["location"]} {previous_out["type"]}*'
                        )
                        art_ = p.a(p.number_to_words(previous_tc["distance"])).split()[
                            0
                        ]
                        _md = f'Prior to the stage, {art_} {previous_tc["distance"]} km liaison section to the *{previous_tc["location"]} {previous_tc["type"]}* from the {previous_location}.'
                        md.append(_md)

                        # Remark on stage status
                        # TO DO

                        # Remark on following liaison stage
                        future_ = itinerary_df.iloc[ss_index + 1 :]
                        # Get indices of time controls
                        next_tc_idx = future_[
                            future_["code"].str.startswith("T")
                        ].index[0]
                        next_tc = itinerary_df.iloc[next_tc_idx]
                        art_ = p.a(p.number_to_words(next_tc["distance"])).split()[0]
                        arrival_time = datetime.fromisoformat(
                            next_tc["firstCarDueDateTime"]
                        )
                        next_arrival_time = (
                            arrival_time.strftime("%I.%M%p").lower().replace(" 0", " ")
                        )
                        _md_final = f'Following the stage, {art_} {next_tc["distance"]} km liaison section to *{next_tc["location"]}* (stage running from {next_arrival_time}).'

                        md.append(_md_final)
                        return ui.markdown("\n\n".join(md))

                with ui.accordion_panel("Stage remarks"):

                    ui.input_checkbox(
                        "stage_remarks_category_rank",
                        "Use category rankings",
                        True,
                    )

                    @render.ui
                    @reactive.event(
                        input.category,
                        input.stage,
                        input.category,
                        input.stage_remarks_category_rank,
                    )
                    def stage_report_remarks():
                        # TO DO - move this into stage_times_remarks() ?
                        priority = input.category()
                        stageId = input.stage()
                        if not priority or not stageId:
                            return "Still initialising..."
                        stageId = int(stageId)
                        # TO DO - the following is called all over the place
                        # TO DO need to address this; the reactivitry has gone to pot
                        setStageData()

                        stages_info = wrc.getStageInfo(raw=False)
                        # TO DO elsewhere we return into overallResults not overall_df
                        _, _, overall_df = getOverallStageResultsCore(
                            stageId, priority, stages_info
                        )

                        stages_info["stageInDay"] = (
                            stages_info.groupby(["day"]).cumcount() + 1
                        )
                        stage_info = stages_info[
                            stages_info["stageId"] == stageId
                        ].iloc[0]
                        md = []

                        stage_code = stage_info["code"]
                        stage_name = stage_info["name"]

                        _md = f"""*{stage_code} {stage_name} ({stage_info["distance"]}km)*"""
                        md.append(_md)

                        _md = f"""Category results ({priority})"""

                        times = wrc.getStageTimes(
                            stageId=stageId, priority=priority, raw=False
                        )
                        times.sort_values("position", inplace=True)

                        # TO DO do we still need stages_info ? Or could we return it as raw and remove the stages_info requirement previously?

                        if times.empty or overall_df.empty:
                            return  # Anything else we could report here?

                        overall_pos = overall_df.loc[
                            overall_df["carNo"] == times.iloc[0]["carNo"], "position"
                        ].iloc[0]
                        _md = f"""__{times.iloc[0]["driverName"]}__ was in __{Nth(1)}__ position on stage and __{Nth(overall_pos)} overall__.
                        """
                        md.append(_md)

                        stagewinners = getStageWinners()
                        if not stagewinners.empty:
                            winner_row = stagewinners.loc[
                                stagewinners["stageId"] == stageId
                            ]

                            _md = f"""This was his *__{Nth(winner_row.iloc[0]["daily_wins"])}__ stage win of the day* and his *__{Nth(winner_row.iloc[0]["wins_overall"])}__ stage win overall*."""

                            md.append(_md)

                        # TO DO remark eg team made clean sweep of podium with X in second, M behind, and Y in third, a further Z back.

                        # Teams remarks
                        # Get teams in top 3
                        for n in list(range(5, 2, -1)):
                            topNteamsOverall = times[times["position"] <= n][
                                "entrantName"
                            ].to_list()
                            if len(set(topNteamsOverall)) == 1:
                                md_ = f"""__{topNteamsOverall[0]}__ dominated the stage, taking """
                                if n != 3:
                                    md_ = f"""{md_} __the top *{numToWords(n)}* positions__."""
                                else:
                                    md_ = f"""{md_} __all three podium positions__."""
                                md.append(md_)
                                break

                        if times.iloc[0]["carNo"] != overall_df.iloc[0]["carNo"]:
                            leader_row = times.loc[
                                times["carNo"] == overall_df.iloc[0]["carNo"]
                            ]
                            leader = leader_row.iloc[0]["driverName"]
                        else:
                            leader = ""
                            leader_row = DataFrame()

                        CLOSE_PACE = 0.1  # 0.05
                        on_the_pace = times[times["pace diff (s/km)"] < CLOSE_PACE]
                        leader_handled = False

                        if len(on_the_pace) > 1:
                            _md = "Also on the pace"
                            for _, r in on_the_pace[1:].iterrows():
                                if leader == r["driverName"]:
                                    leader_handled = True
                                    leader_text = "rally leader "
                                else:
                                    leader_text = ""
                                _md = (
                                    _md
                                    + f""", __{leader_text}{r["driverName"]}__ was just {r["Gap"]}s behind ({round(r["pace diff (s/km)"], 2)} s/km off the stage winner)"""
                                )
                            md.append(_md + ".")

                        if (
                            not leader_row.empty and not leader_handled
                        ):  # Check if leader exists in times
                            leaderPos = leader_row.iloc[0]["position"]
                            leaderDiff = leader_row.iloc[0]["Gap"]
                            # TO DO - the following is badly duped if we get a new leader
                            _md = f"""Rally leader {overall_df.iloc[0]["driverName"]} was {leaderDiff} seconds off the stage winner in {Nth(leaderPos)} position."""
                            md.append(_md)  # Properly append the string

                        # External rules test
                        rerank = input.stage_remarks_category_rank()
                        _overall_diff = core_stage(
                            wrc,
                            stages_info,
                            stageId,  # stage_code,
                            priority=priority,
                            rerank=rerank,
                        )
                        remarks = process_rally_overall_rules(_overall_diff)
                        for remark in remarks:
                            md.append(remark[0])

                        if md:
                            return ui.markdown("\n\n".join(md))

                with ui.accordion_panel("Stage map"):

                    @render_widget
                    @reactive.event(rally_geodata, input.stage)
                    def single_stage_map():
                        stageId = input.stage()
                        if not stageId:
                            return ui.markdown("No stage to report on...")

                        geostages = rally_geodata()
                        if geostages.empty:
                            return ui.markdown("No route data available...")

                        stages_info = wrc.getStageInfo(raw=False)
                        if stages_info.empty:
                            return ui.markdown("Awaiting stages data...")

                        stage_info = stages_info[
                            stages_info["stageId"] == int(stageId)
                        ].iloc[0]
                        # print(geostages["stages"], stage_info["code"])
                        m = wrcapi.GeoTools.simple_stage_map(
                            geostages, stages=stage_info["code"]
                        )
                        return m

                # TO DO
                # Add a stage briefing accordion panel:
                # Overall lead coming into the stage (use championship if SS1; for first round, announce new season.)
                # Some comment on the road order.
                # If this is the second run of a stage, add commentary about the first run

                with ui.accordion_panel("Stage times"):
                    # Create stage driver rebase selector
                    ui.input_select(
                        "stage_rebase_driver",
                        "Driver rebase:",
                        {},
                    )

                    # Remarks on stage result / items of interest
                    # TO DO
                    # Remark on stage winner and their overall position
                    # Remark on stage winner's stage win stats.
                    # Remark on rally leader at start of stage position if not stage winner.
                    # Remark on rally leader at end of stage position if not stage winner.
                    # Remark on person losing overall lead, if approppriate.
                    # If this is N>1st run of a stage, make comparisons with previous run(s)

                    @render.plot(alt="Barplot of stage times.")
                    @reactive.event(
                        input.stage_review_accordion,
                        input.category,
                        input.stage,
                        input.stage_rebase_driver,
                    )
                    def seaborn_barplot_stagetimes():
                        stage_times_df = get_rebased_data()
                        if stage_times_df is None:
                            return empty_plot(title="No stage times data...")
                        rebase_reverse_palette = input.rebase_reverse_palette()
                        ax = chart_seaborn_barplot_stagetimes(
                            stage_times_df, rebase_reverse_palette
                        )
                        return ax

                    @render.data_frame
                    @reactive.event(
                        input.stage_review_accordion,
                        input.category,
                        input.stage,
                        input.stage_rebase_driver,
                    )
                    def stage_results_short():
                        stage_times_df = get_rebased_data()
                        if stage_times_df is None:
                            return

                        cols = [
                            "carNo",
                            "driverName",
                            "roadPos",
                            "position",
                            "categoryPosition",
                            "Gap",
                            "Diff",
                            "Rebase Gap (s)",
                            "Rebase %",
                            "Rebase pace diff (s/km)",
                            "timeInS",
                            "speed (km/h)",
                            "pace (s/km)",
                            "timeToCarBehind",
                            "codriverName",
                            "manufacturerName",
                            "entrantName",
                            "vehicleModel",
                            "priority",
                            "eligibility",
                        ]
                        # TO DO — make timeInS a nice, human readable time
                        cols = [c for c in cols if c in stage_times_df.columns]
                        return render.DataGrid(stage_times_df[cols])

                with ui.accordion_panel("Overall rally positions"):

                    # TO DO
                    # Remarks on the overall rally positions
                    # Changes in overall
                    # Progress of previous overall / top three
                    # Maybe a mention of manufacturer or team progress?

                    @render.data_frame
                    @reactive.event(
                        input.stage_review_accordion, input.category, input.stage
                    )
                    def overall_short():
                        stageId = input.stage()
                        priority = input.category()
                        if not stageId:
                            return
                        stageId = int(stageId)
                        overall_df = wrc.getStageOverallResults(
                            stageId=stageId, priority=priority, raw=False
                        )
                        if overall_df.empty:
                            return
                        retcols = [
                            "roadPos",
                            "position",
                            "categoryPosition",
                            "carNo",
                            "driverName",
                            "codriverName",
                            "entrantName",
                            "stageTimeMs",
                            "diffFirstMs",
                            "diffPrevMs",
                            "penaltyTimeMs",
                            "totalTimeMs",
                            "priority",
                            "eligibility",
                        ]
                        retcols = [
                            k
                            for k in retcols
                            + [k for k in overall_df.columns if k.startswith("round")]
                            if k in overall_df.columns
                        ]
                        overall_df = overall_df[retcols]
                        if "stageTimeMs" in overall_df:
                            overall_df["stageTimeMs"] = overall_df["stageTimeMs"].apply(
                                format_timedelta
                            )
                        if "diffFirstMs" in overall_df:
                            overall_df["diffFirstMs"] = overall_df["diffFirstMs"].apply(
                                lambda x: format_timedelta(x, addplus=True)
                            )
                        if "diffPrevMs" in overall_df:
                            overall_df["diffPrevMs"] = overall_df["diffPrevMs"].apply(
                                lambda x: format_timedelta(x, addplus=True)
                            )
                        if "penaltyTimeMs" in overall_df:
                            overall_df["penaltyTimeMs"] = overall_df[
                                "penaltyTimeMs"
                            ].apply(format_timedelta)
                        if "totalTimeMs" in overall_df:
                            overall_df["totalTimeMs"] = overall_df["totalTimeMs"].apply(
                                format_timedelta
                            )

                        overall_df.rename(
                            columns={
                                "stageTimeMs": "stageTime",
                                "diffFirstMs": "diffFirst",
                                "diffPrevMs": "diffPrev",
                                "penaltyTimeMs": "penaltyTime",
                                "totalTimeMs": "totalTime",
                            },
                            inplace=True,
                        )
                        return render.DataGrid(overall_df)

    with ui.accordion_panel(title="Splits Analysis"):

        with ui.accordion(open=False, id="splits_geo1_accordion"):

            with ui.accordion_panel("Split route sections map"):

                @render.plot(alt="Route map split sections.")
                @reactive.event(
                    input.year,
                    input.season_round,
                    input.category,
                    input.stage,
                    rally_geodata,
                )
                def split_sections_map():
                    stageId = input.stage()

                    if not stageId:
                        return empty_plot(title="No stage selected...")

                    geostages = rally_geodata()

                    if geostages.empty:
                        return empty_plot(title="No geo data...")

                    ax2 = split_sections_map_core(wrc, stageId, geostages)
                    return ax2

        ui.markdown("\n\n")

        ui.input_action_button("splits_refresh", "Refresh split times")
        ui.markdown("*Manually refresh split times in live stage.*\n\n")

        with ui.accordion(open=False, id="splits_review_accordion"):

            with ui.accordion_panel("Split sections summary"):

                ui.markdown("*Position / elapsed time across the split sections.*")

                @render.plot(alt="Line chart of elapsed split positions.")
                @reactive.event(
                    input.splits_review_accordion,
                    input.category,
                    input.stage,
                    input.splits_refresh,
                )
                def seaborn_linechart_split_positions():
                    split_times_wide = get_split_times_wide()
                    if split_times_wide.empty:
                        return empty_plot(title="No split times data...")
                    split_times_wide = split_times_wide.copy()
                    split_cols = wrc.getSplitCols(split_times_wide)

                    ax = chart_seaborn_linechart_split_positions(
                        wrc, split_times_wide, split_cols
                    )
                    ax.tick_params(axis="x", labelrotation=45)
                    return ax

                @render.data_frame
                @reactive.event(
                    input.splits_review_accordion,
                    input.category,
                    input.stage,
                    input.splits_refresh,
                )
                def split_results_wide():
                    split_times_data = get_split_times_wide()
                    if split_times_data is None or split_times_data.empty:
                        return
                    return render.DataGrid(split_times_data)

            with ui.accordion_panel("Split sections detail"):

                with ui.tooltip(id="splits_section_view_tt"):
                    ui.input_select(
                        "splits_section_view",
                        "Section report view",
                        {
                            "time": "Within section time (s)",
                            "time_acc": "Acc. section time (s)",
                            "pace": "Av. pace in-section (s/km)",
                            "speed": "Av. speed in-section (km/h)",
                            "pos_within": "Within section rank",
                            "pos_acc": "Acc. section rank",
                        },
                        selected="time",
                    ),
                    "Select split section report type; time (s), position within or across splits, or, if available, average Pace (s/km) or average Speed (km/h)."
                    # Scope the view if data available

                @render.express
                @reactive.event(input.interpretation_prompt_switch)
                def stage_progression_report_interpretation_container():
                    ui.input_switch(
                        "stage_progression_interpretation_switch",
                        "Show interpretation prompts",
                        False,
                    )

                @render.ui
                @reactive.event(input.stage_progression_interpretation_switch)
                def stage_progression_report_interpretation():
                    if input.stage_progression_interpretation_switch():
                        md = stage_progression_interpretation_md

                        return ui.markdown(
                            f"""<hr/>\n\n<div style="background-color:{INTEPRETATION_PANEL_COLOUR}">{md}</div>\n\n<hr/>\n\n"""
                        )

                with ui.card(class_="mt-3"):
                    with ui.card_header():
                        with ui.tooltip(
                            placement="right", id="splits_section_report_tt"
                        ):
                            ui.span(
                                "Split section report ",
                                question_circle_fill,
                            )
                            "Split section report. View section reports as time in section (s), or, if split distance available, average pace in section (s/km), or average speed in section (km/h)."

                    @render.ui
                    @reactive.event(
                        input.stage, input.splits_section_view, input.splits_refresh
                    )
                    def split_report_view():
                        view = input.splits_section_view()
                        typ = {
                            "time": "Time (s) within each split",
                            "speed": "Speed (km/h) within each split",
                            "pace": "Pace (s/km) within each split",
                            "time_acc": "Accumulated time (s) across all splits",
                            "pos_within": "Rank position within split",
                            "pos_acc": "Rank position of accumulated time at each split",
                        }
                        return ui.markdown(typ[view])

                    # @render.table
                    @render.data_frame
                    @reactive.event(
                        input.splits_section_view,
                        input.stage,
                        input.category,
                        input.splits_refresh,
                    )
                    def split_report():
                        scaled_splits = get_scaled_splits()
                        if scaled_splits is None or scaled_splits.empty:
                            return
                        return render.DataGrid(scaled_splits)

                with ui.accordion(open=False):
                    with ui.accordion_panel("Split section speed/pace distributions"):

                        @render.plot(
                            alt="Box plot of split section speed/pace distributions."
                        )
                        @reactive.event(
                            input.stage, input.splits_section_view, input.splits_refresh
                        )
                        def plot_split_dists():
                            scaled_splits_wide = get_scaled_splits()
                            if scaled_splits_wide is None or scaled_splits_wide.empty:
                                return empty_plot(title="No splits data...")
                            splits_section_view = input.splits_section_view()
                            ax = chart_plot_split_dists(
                                wrc, scaled_splits_wide, splits_section_view
                            )
                            return ax

            with ui.accordion_panel("Rebased driver reports"):
                with ui.card(class_="mt-3"):
                    with ui.card_header():

                        with ui.tooltip(
                            placement="right", id="rebased_driver_report_tt"
                        ):
                            ui.span(
                                "Rebased driver report ",
                                question_circle_fill,
                            )
                            'Rebased delta times and pace are calculated relative to the selected "rebase" driver.'

                    """ TO DO: sort order dropdown (SP.., RoadPos, position); show: Top10, Top20, All"""

                    # Create driver rebase selector
                    with ui.tooltip(id="rebase_driver_tt"):
                        ui.input_select(
                            "rebase_driver",
                            "Driver rebase:",
                            {},
                        ),
                        '"Rebase" times relative to a nominated driver. The "ULTIMATE" driver is derived from the quickest times within each split sector .'

                    @render.express
                    @reactive.event(input.interpretation_prompt_switch)
                    def stage_progression_rebase_select_interpretation_container():
                        ui.input_switch(
                            "stage_progression_rebase_select_interpretation_switch",
                            "Show interpretation prompts",
                            False,
                        )

                    @render.ui
                    @reactive.event(
                        input.stage_progression_rebase_select_interpretation_switch
                    )
                    def stage_progression_report_rebase_select_interpretation():
                        if (
                            input.stage_progression_rebase_select_interpretation_switch()
                        ):
                            md = stage_progression_rebase_select_md

                            return ui.markdown(
                                f"""<hr/>\n\n<div style="background-color:{INTEPRETATION_PANEL_COLOUR}">{md}</div>\n\n<hr/>\n\n"""
                            )

                    @render.ui
                    @reactive.event(
                        input.stage, input.rebase_driver, input.splits_refresh
                    )
                    def rebase_driver_info():
                        stageId = input.stage()
                        rebase_driver = input.rebase_driver()
                        # TO DO: provide ult view if rebase_driver=="ult"
                        if not stageId or not rebase_driver or rebase_driver == "ult":
                            return
                        stageId = int(stageId)
                        rebase_driver = (
                            int(rebase_driver)
                            if rebase_driver and rebase_driver != "ult"
                            else rebase_driver
                        )
                        stages = wrc.getStageInfo(stage_code=stageId, raw=False)
                        times = wrc.getStageTimes(stageId=stageId, raw=False)
                        if stages.empty or times.empty:
                            return ui.markdown("*No data available.*")

                        pr = get_rebased_driver_hero(
                            stageId, rebase_driver, stages, times
                        )
                        return pr

                    with ui.tooltip(id="split_prog_rebase_incols_tt"):
                        ui.input_checkbox(
                            "split_prog_rebase_incols",
                            "Within column heatmap",
                            True,
                        ),
                        "Create heatmap palette within a column rather than across all columns."

                    with ui.tooltip(id="rebase_reverse_palette_tt"):
                        ui.input_checkbox(
                            "rebase_reverse_palette",
                            "Reverse rebase palette",
                            False,
                        ),
                        "Reverse the rebase palette to show deltas relative to the rebased driver's perspective."

                    with ui.accordion(open=False):
                        with ui.accordion_panel(
                            "Time gained/lost within and across each split"
                        ):
                            ui.input_switch(
                                "rebased_splits_type_switch",
                                "Within section time delta (default is accumulated stage time delta)",
                                False,
                            )

                            @render.ui
                            def rebased_splits_type_value():
                                typ = (
                                    "Within section"
                                    if input.rebased_splits_type_switch()
                                    else "Accumulated stage"
                                )
                                return ui.markdown(
                                    f"\n__{typ} time delta across split points.__\n\n"
                                )

                            ui.markdown(
                                "TO DO - option to sort by start order, stage position"
                            )

                            ui.input_slider(
                                "rebased_splits_palette_upper_limit",
                                "Rebase palette saturation upper limit",
                                min=0,
                                max=60,
                                value=60,
                            )

                            ui.input_slider(
                                "rebased_splits_palette_lower_limit",
                                "Rebase palette saturation lower limit",
                                min=-60,
                                max=0,
                                value=-60,
                            )

                            @render.express
                            @reactive.event(input.interpretation_prompt_switch)
                            def split_times_heat_interpretation_container():
                                ui.input_switch(
                                    "split_times_heat_interpretation_switch",
                                    "Show interpretation prompts",
                                    False,
                                )

                            @render.ui
                            @reactive.event(
                                input.split_times_heat_interpretation_switch
                            )
                            def split_times_heat_interpretation():
                                if input.split_times_heat_interpretation_switch():
                                    md = split_times_heat_interpretation_md

                                    return ui.markdown(
                                        f"""<hr/>\n\n<div style="background-color:{INTEPRETATION_PANEL_COLOUR}">{md}</div>\n\n<hr/>\n\n"""
                                    )

                            @reactive.calc
                            @reactive.event(
                                input.splits_review_accordion,
                                input.category,
                                input.stage,
                                input.rebase_driver,
                                input.splits_refresh,
                                input.rebased_splits_type_switch,
                                input.split_prog_rebase_incols,
                                input.rebased_splits_palette_upper_limit,
                                input.rebased_splits_palette_lower_limit,
                            )
                            def split_times_heat_vals():
                                split_times_wide = get_split_times_wide()
                                rebase_driver = input.rebase_driver()

                                if split_times_wide.empty or not rebase_driver:
                                    return DataFrame(), []
                                split_cols = wrc.getSplitCols(split_times_wide)
                                rebase_driver = (
                                    int(rebase_driver)
                                    if rebase_driver and rebase_driver != "ult"
                                    else rebase_driver
                                )
                                split_times_wide, split_cols = (
                                    wrc.rebase_splits_wide_with_ult(
                                        split_times_wide,
                                        rebase_driver,
                                        use_split_durations=input.rebased_splits_type_switch(),
                                    )
                                )

                                return split_times_wide, split_cols

                            @render.ui
                            @reactive.event(
                                split_times_heat_vals,
                                input.rebase_reverse_palette,
                                input.rebased_splits_palette_upper_limit,
                                input.rebased_splits_palette_lower_limit,
                            )
                            def split_times_heat():
                                rebase_reverse_palette = input.rebase_reverse_palette()
                                split_times_wide, split_cols = split_times_heat_vals()
                                if split_times_wide.empty:
                                    return
                                html = (
                                    df_color_gradient_styler(
                                        split_times_wide,
                                        cols=split_cols,
                                        within_cols_gradient=input.split_prog_rebase_incols(),
                                        reverse_palette=rebase_reverse_palette,
                                        # TO DO - consider pace based thresholds
                                        # Pass in sector/stage distances and set a nominal pace threshold (s/km)
                                        # Then set colour based on max-ing the color at the pace threshold
                                        use_linear_cmap=True,
                                        drop_last_quantile=False,
                                        upper_limit=max(
                                            0.01,
                                            input.rebased_splits_palette_upper_limit(),
                                        ),
                                        lower_limit=min(
                                            -0.01,
                                            input.rebased_splits_palette_lower_limit(),
                                        ),
                                    )
                                    .hide()
                                    .to_html()
                                )
                                return ui.HTML(html)

                            @reactive.effect
                            @reactive.event(
                                input.split_times_heat_interpretation_switch,
                                input.stage,
                                input.rebase_driver,
                                input.splits_heatmap_driver,
                            )
                            def update_rebased_splits_palette_limits():
                                split_times_wide, split_cols = split_times_heat_vals()
                                max_ = split_times_wide[split_cols].max().max()
                                min_ = split_times_wide[split_cols].min().min()
                                # XX
                                ui.update_slider(
                                    id="rebased_splits_palette_upper_limit",
                                    min=0,
                                    max=math.ceil(max_),
                                    value=max_,
                                )
                                ui.update_slider(
                                    id="rebased_splits_palette_lower_limit",
                                    min=math.ceil(min_ - 1),
                                    max=0,
                                    value=min_,
                                )

                        with ui.accordion_panel("Time gained/lost within each split"):

                            with ui.tooltip(id="heatmap_outliers_tt"):
                                ui.input_checkbox(
                                    "heatmap_outliers",
                                    "Heatmap outliers",
                                    False,
                                ),
                                "Pack analysis: calculate z-scores to identify outliers."

                            with ui.card(class_="mt-3"):
                                with ui.card_header():
                                    with ui.tooltip(
                                        placement="right",
                                        id="splits_in_section_delta_heatmap_tt",
                                    ):
                                        ui.span(
                                            "Time gained / lost within each section in seconds relative to rebase driver (heatmap) ",
                                            question_circle_fill,
                                        )
                                        "Delta times within each split section. Times are relative to rebased driver's time. Bright column: good/bad split section for rebased driver. Bright row: good/bad sections for (row) driver."

                                @render.plot(alt="Heatmap of within split delta times.")
                                def seaborn_heatmap_splits():
                                    rebase_driver = input.rebase_driver()
                                    if not rebase_driver:
                                        return empty_plot(
                                            title="No rebase driver selected..."
                                        )

                                    rebase_driver = (
                                        int(rebase_driver)
                                        if rebase_driver != "ult"
                                        else rebase_driver
                                    )

                                    # Get the cached split times data
                                    split_times_wide = get_split_times_wide()
                                    if (
                                        split_times_wide is None
                                        or split_times_wide.empty
                                    ):
                                        return empty_plot(
                                            title="No split times data..."
                                        )
                                    output_, split_cols = (
                                        wrc.rebase_splits_wide_with_ult(
                                            split_times_wide,
                                            rebase_driver,
                                            use_split_durations=True,
                                        )
                                    )
                                    # TO DO - find a more flexible /informative way of setting
                                    # the index / driver identifier
                                    output_.set_index("carNo", inplace=True)
                                    # split_cols = wrc.getSplitCols(output_)
                                    dropcols = [
                                        c
                                        for c in output_.columns
                                        if c not in split_cols
                                    ]
                                    output_.drop(columns=dropcols, inplace=True)
                                    output_.columns = [
                                        f"Split {i}"
                                        for i in range(1, output_.shape[1] + 1)
                                    ]  # [:-1] + ["Finish"]

                                    if input.heatmap_outliers():
                                        z_scores = (
                                            output_ - output_.mean()
                                        ) / output_.std()
                                        output_ = z_scores
                                        # A boolean throws an inconsistent type error
                                        # output_.loc[:, split_cols] = (
                                        #    abs(z_scores) > 3
                                        # ).any(axis=1)

                                    colors = (
                                        ["red", "white", "green"]
                                        if input.rebase_reverse_palette()
                                        else ["green", "white", "red"]
                                    )
                                    cmap = LinearSegmentedColormap.from_list(
                                        "custom_cmap", colors
                                    )

                                    output_.rename(
                                        columns={
                                            s: s.replace("Split ", "s")
                                            for s in output_.columns
                                        },
                                        inplace=True,
                                    )

                                    return heatmap(
                                        output_,
                                        cmap=cmap,
                                        fmt=".1f",
                                        center=0,
                                        annot=True,
                                        cbar=False,
                                    )

                        with ui.accordion_panel("Split times stage section heatmaps"):
                            with ui.tooltip(id="split_times_heatmap_driver_tt"):
                                ui.input_select(
                                    "splits_heatmap_driver",
                                    "Splits heatmap driver:",
                                    {},
                                ),
                                "Get the driver we want to plot the rebased times for on a split sections map."

                            ui.input_checkbox(
                                "heat_splitmap_use_limits",
                                "Use palette limit sliders",
                                False,
                            )

                            @render.plot(alt="Route map split sections heatmap.")
                            @reactive.event(
                                input.stage,
                                input.rebase_driver,
                                input.splits_heatmap_driver,
                                input.rebased_splits_palette_upper_limit,
                                input.rebased_splits_palette_lower_limit,
                                input.heat_splitmap_use_limits
                            )
                            def route_sections_heatmap():
                                stageId = input.stage()
                                rebase_driver = input.rebase_driver()
                                heatmap_driver = input.splits_heatmap_driver()

                                # Use the common setup function
                                split_times_wide, rebase_driver, error_msg = (
                                    prepare_split_times_data(stageId, rebase_driver)
                                )

                                if error_msg:
                                    return empty_plot(title=error_msg)

                                # Additional validation specific to this function
                                if not heatmap_driver:
                                    return empty_plot(
                                        title="No heatmap driver selected..."
                                    )

                                if rebase_driver == heatmap_driver:
                                    return empty_plot(
                                        title="You need to select different\nrebase and splits heatmap drivers...."
                                    )

                                heatmap_driver = int(heatmap_driver)

                                # Original code from here on - no changes
                                output_, split_cols = wrc.rebase_splits_wide_with_ult(
                                    split_times_wide,
                                    rebase_driver,
                                    use_split_durations=True,
                                )

                                def _get_heatmap_colors(df, cols, vmax=None, vmin=None):
                                    vmax = df[cols].values.max() if not vmax else vmax
                                    vmax = vmax if vmax > 0 else 1
                                    vmin = df[cols].values.min() if not vmin else vmin
                                    vmin = vmin if vmin < 0 else -1
                                    colors = ["green", "white", "red"]

                                    cmap = LinearSegmentedColormap.from_list(
                                        "custom_cmap", colors
                                    )
                                    colors = []
                                    for c in cols:
                                        val = df[c].iloc[0]

                                        if isna(val):
                                            colors.append(
                                                "#d9d9d9"
                                            )  # Light gray for NaN
                                        elif val == 0:
                                            colors.append("#f0f0f0")
                                        else:
                                            normed = TwoSlopeNorm(
                                                vmin=vmin, vcenter=0, vmax=vmax
                                            )
                                            colormap = cmap(
                                                normed(val)
                                            )  # norm maps val to 0-1, cmap maps 0-1 to color
                                            colormap = cmap(
                                                normed(val)
                                            )  # norm maps val to)
                                            r, g, b, a = [
                                                int(255 * c) for c in colormap
                                            ]
                                            # color = f"rgba({r},{g},{b},{a})"
                                            color = "#{:02x}{:02x}{:02x}".format(
                                                r, g, b
                                            )
                                            colors.append(color)
                                    return colors

                                selected_rebased_time_wide = output_[
                                    output_["carNo"] == heatmap_driver
                                ]
                                # Get min.max across all the cols
                                # so we can compare across drivers
                                vmax = output_[split_cols].stack().max()
                                vmin = output_[split_cols].stack().min()

                                if input.heat_splitmap_use_limits():
                                    # use the controls for the palette saturation limit
                                    vmax = max(
                                        0.01,
                                        input.rebased_splits_palette_upper_limit(),
                                    )
                                    vmin = min(
                                        -0.01,
                                        input.rebased_splits_palette_lower_limit(),
                                    )

                                # Generate heat colours for section
                                # We need len(split_cols)+1 colours
                                heat_colours = _get_heatmap_colors(
                                    selected_rebased_time_wide, split_cols, vmax, vmin
                                )

                                geostages = rally_geodata()
                                fig, ax = plt.subplots(facecolor="black")
                                ax = split_sections_map_core(
                                    wrc,
                                    stageId,
                                    geostages,
                                    ax=ax,
                                    heat_colours=heat_colours,
                                )
                                ax.set_title(
                                    f"Within split section time deltas:\ncar {heatmap_driver} compared to car {rebase_driver}."
                                )
                                ax.title.set_color("white")

                                return ax

                        with ui.accordion_panel("Split times group barplots"):
                            with ui.tooltip(id="splits_section_plot_type_tt"):
                                ui.input_select(
                                    "splits_section_plot_type",
                                    "Section plot view",
                                    {
                                        "bysplit": "Split section groups",
                                        "bydriver": "Driver groups",
                                    },
                                    selected="time",
                                ),
                                "Select split section report group; view split section difference by split sections group or driver group. Split section group is good for showing strong/weak sections; driver grouping is good for showing split section comparisons relative to each other driver."
                                # Scope the view if data available

                            @render.express
                            @reactive.event(input.interpretation_prompt_switch)
                            def stage_progression_report_barchart_interpretation_container():
                                ui.input_switch(
                                    "stage_progression_barchart_interpretation_switch",
                                    "Show interpretation prompts",
                                    False,
                                )

                            @render.ui
                            @reactive.event(
                                input.stage_progression_barchart_interpretation_switch
                            )
                            def stage_progression_report_barchart_interpretation():
                                if (
                                    input.stage_progression_barchart_interpretation_switch()
                                ):
                                    md = stage_progression_barchart_interpretation_md

                                    ui.markdown(
                                        f"""<hr/>\n\n<div style="background-color:{INTEPRETATION_PANEL_COLOUR}">{md}</div>\n\n<hr/>\n\n"""
                                    )

                            with ui.card(class_="mt-3"):
                                with ui.card_header():
                                    with ui.tooltip(
                                        placement="right",
                                        id="splits_in_section_delta_barplot_tt",
                                    ):
                                        ui.span(
                                            "Time gained / lost within each section in seconds relative to rebase driver (stacked barplot) ",
                                            question_circle_fill,
                                        )
                                        "Delta times within each split section. Times are relative to rebased driver's time. Bright column: good/bad split section for rebased driver. Bright row: good/bad sections for (row) driver."

                                @render.plot(alt="Barplot of within split delta times.")
                                def seaborn_barplot_splits():
                                    rebase_driver = input.rebase_driver()
                                    rebase_reverse_palette = (
                                        input.rebase_reverse_palette()
                                    )
                                    splits_section_plot_type = (
                                        input.splits_section_plot_type()
                                    )
                                    # print(f"Rebasing on {rebase_driver}")
                                    if not rebase_driver:
                                        return empty_plot(
                                            title="No rebase driver selected..."
                                        )

                                    rebase_driver = (
                                        int(rebase_driver)
                                        if rebase_driver != "ult"
                                        else rebase_driver
                                    )

                                    # Get the cached split times data
                                    split_times_wide = get_split_times_wide()
                                    if (
                                        split_times_wide is None
                                        or split_times_wide.empty
                                    ):
                                        return empty_plot(
                                            title="No split times data..."
                                        )

                                    ax = chart_seaborn_barplot_splits(
                                        wrc,
                                        split_times_wide,
                                        rebase_driver,
                                        splits_section_plot_type,
                                        rebase_reverse_palette,
                                    )

                                    return ax

                        with ui.accordion_panel("Split times linecharts"):
                            with ui.card(class_="mt-3"):
                                with ui.card_header():
                                    with ui.tooltip(
                                        placement="right",
                                        id="splits_in_sectionlineplot_tt",
                                    ):
                                        ui.span(
                                            "Accumulated time gained / lost across sections in seconds relative to rebase driver (line chart) ",
                                            question_circle_fill,
                                        )
                                        "Accumuluated time deltas across each split section. Times are relative to rebased driver's time. Lines above x=0 are cars ahead, lines below are times behind."

                                @render.express
                                @reactive.event(input.interpretation_prompt_switch)
                                def stage_progression_report_linechart_interpretation_container():
                                    ui.input_switch(
                                        "stage_progression_linechart_interpretation_switch",
                                        "Show interpretation prompts",
                                        False,
                                    )

                                @render.ui
                                @reactive.event(
                                    input.stage_progression_linechart_interpretation_switch
                                )
                                def stage_progression_report_linechart_interpretation():
                                    if (
                                        input.stage_progression_linechart_interpretation_switch()
                                    ):
                                        md = stage_progression_linechart_interpretation_md

                                        ui.markdown(
                                            f"""<hr/>\n\n<div style="background-color:{INTEPRETATION_PANEL_COLOUR}">{md}</div>\n\n<hr/>\n\n"""
                                        )

                                @render.plot(
                                    alt="Line chart of within split delta times."
                                )
                                def seaborn_linechart_splits():
                                    stageId = input.stage()
                                    rebase_driver = input.rebase_driver()

                                    # Use the common setup function
                                    split_times_wide, rebase_driver, error_msg = (
                                        prepare_split_times_data(stageId, rebase_driver)
                                    )

                                    if error_msg:
                                        return empty_plot(title=error_msg)

                                    # Use the original chart function with the prepared data
                                    ax = chart_seaborn_linechart_splits(
                                        wrc, stageId, split_times_wide, rebase_driver
                                    )

                                    return ax


# @render.ui
# @reactive.event(input.year, input.season_round, input.category)
# def WRCapidf():
#    return str(getWRCAPI2event())

## Reactive calcs


## LIVE DATA


def get_data_feed():
    if not hasattr(get_data_feed, "available"):
        get_data_feed.available = True
    if not get_data_feed.available:
        return ()

    if not hasattr(get_data_feed, "prev"):
        get_data_feed.prev = {}  # Initialize on first call

    if not wrc.isRallyInDate() or not input.live_map_accordion():
        get_data_feed.prev

    # If we are running this in a central server, multiuser context, does this let us be nice?
    # if the service is down, note that and stop trying
    # TO DO  - provide a way to reet / restart this?
    try:
        json = session.get(
            "https://webappsdata.wrc.com/srv/wrc/json/api/liveservice/getData?timeout=5000"
        ).json()["_entries"]
        get_data_feed.prev = json
        return json
    except:
        get_data_feed.available = False


@reactive.poll(
    get_data_feed, 30
)  # 5.1 if we want to collect max telemetry, see if a car has stopped
@reactive.event(input.live_map_accordion)
def car_getdata():
    if input.live_map_accordion():
        print("polling live position data")
        return DataFrame(get_data_feed())


with ui.accordion(open=False, id="live_map_accordion"):

    with ui.accordion_panel("Live Map"):

        @render.express
        @reactive.event(input.interpretation_prompt_switch)
        def livemap_interpretation_container():
            ui.input_switch(
                "live_map_interpretation_switch",
                "Show interpretation prompts",
                False,
            )

        @render.ui
        @reactive.event(input.live_map_interpretation_switch)
        def live_map_interpretation():
            if input.live_map_interpretation_switch():
                md = live_map_interpretation_md

                ui.markdown(
                    f"""<hr/>\n\n<div style="background-color:{INTEPRETATION_PANEL_COLOUR}">{md}</div>\n\n<hr/>\n\n"""
                )

        # ui.input_checkbox(
        #    "pause_live_map",
        #    "Pause live map updates",
        #    False,
        # )

        # Map rendering function using ipyleaflet
        @render_widget
        @reactive.event(rally_geodata, car_getdata)  # input.pause_live_map,
        def show_map():
            def add_marker(row, m):
                # Create marker
                # TO DO color the moving indication by speed?
                # Use different colour label to show car has stopped (speed 0)
                speed_color = (
                    "rgba(51, 136, 255, 0.7)"
                    if row["speed"] > 0
                    else "rgba(255, 136, 51, 0.7)"
                )
                custom_icon = DivIcon(
                    html=f'<div style="background-color:{speed_color}; display:inline-block; padding:2px 5px; color:white; font-weight:bold; border-radius:3px; border:none; box-shadow:none;">{row["name"]}</div>',
                    className="",  # Empty string removes the default leaflet-div-icon class which has styling
                    icon_size=[0, 0],  # Set icon size to zero
                    icon_anchor=[0, 0],  # Adjust anchor point
                    bgcolor="transparent",  # Ensure background is transparent
                    border_color="transparent",
                )
                # DivIcon(
                #    html=f'<div style="background-color:#3388ff; width:30px; height:30px; border-radius:50%; display:flex; justify-content:center; align-items:center; color:white; font-weight:bold;">{row["name"]}</div>',
                #    icon_size=(30, 30),
                #    icon_anchor=(15, 15),
                # )
                marker = Marker(
                    location=(row["lat"], row["lon"]), icon=custom_icon
                )  # , draggable=False)
                # Add a popup with the name that opens by default
                # message = HTML()
                # message.value = f"<b>{row['name']}</b>"
                # marker.popup = Popup(
                #    location=marker.location,
                #    child=message,
                # )

                # Add marker to map
                m.add_layer(marker)

            setStageData()
            _, _, overallResults = getOverallStageResultsData()
            carNos = overallResults["carNo"].tolist()

            # Get the latest data
            # df = car_getdata()
            # if input.pause_live_map():
            #    with reactive.isolate():
            #        df = car_getdata()
            # else:
            # Get the latest data
            #    df = car_getdata()
            df = car_getdata()
            if df.empty or not "name" in df.columns:
                return

            df["carNo"] = to_numeric(df["name"], errors="coerce").astype("Int64")
            df = df[df["carNo"].isin(carNos)]
            if df.empty:
                return

            # Create a base map centered on the average location
            center_lat = df["lat"].mean()
            center_lon = df["lon"].mean()
            # m = Map(center=(center_lat, center_lon), zoom=9)
            geostages = rally_geodata()
            m = wrcapi.GeoTools.simple_stage_map(geostages)
            # Add markers for each point in the data
            # Add markers to map
            df.apply(lambda row: add_marker(row, m), axis=1)

            buffer_percentage = 0.05
            minx, miny, maxx, maxy = (
                df["lon"].min(),
                df["lat"].min(),
                df["lon"].max(),
                df["lat"].max(),
            )  # total_bounds returns (minx, miny, maxx, maxy)
            x_buffer = (maxx - minx) * buffer_percentage
            y_buffer = (maxy - miny) * buffer_percentage

            # Expand the bounding box
            minx -= x_buffer
            maxx += x_buffer
            miny -= y_buffer
            maxy += y_buffer
            m.fit_bounds([[miny, minx], [maxy, maxx]])

            return m


@reactive.calc
@reactive.event(input.season_round)
def getEventData():
    wrc.setEventById(int(input.season_round()))
    return wrc.getItinerarySections()


@reactive.calc
@reactive.event(input.stage)
def setStageData():
    stageId = input.stage()
    if not stageId:
        return
    stageId = int(stageId)
    wrc.setStageById(stageId=stageId)
    return int(stageId)


@reactive.calc
@reactive.event(input.stage, input.display_latest_overall, input.category)
def getOverallStageResultsData():
    """Get the overall stage results at the end of a specified stage, or the last comleted stage."""
    stageId = input.stage()
    priority = input.category()
    if not stageId or not priority:
        return DataFrame(), DataFrame(), DataFrame()

    setStageData()

    stagesInfo = wrc.getStageInfo(on_event=True).sort_values(
        by="number", ascending=True
    )

    stageId = None
    if input.display_latest_overall() and "status" in stagesInfo:
        # TO DO  - this is ambiguous; stage may be running /cancelled but the priority group may be complete?
        completed_stages = stagesInfo[
            stagesInfo["status"].isin(["Completed", "Cancelled", "Interrupted"])
        ]
        if not completed_stages.empty:
            stageId = completed_stages.iloc[-1]["stageId"]
    else:
        stageId = input.stage()
        stageId = int(stageId)

    return getOverallStageResultsCore(stageId, priority, stagesInfo)


def getOverallStageResultsCore(stageId, priority, stagesInfo):
    overallResults = DataFrame()

    if stageId and not stagesInfo.empty:
        overallResults = wrc.getStageOverallResults(
            stageId=stageId, priority=priority, raw=False
        )

        if not overallResults.empty:
            if priority != "P0":
                overallResults["position"] = range(1, len(overallResults) + 1)
                overallResults["diffFirstMs"] = (
                    overallResults["diffFirstMs"]
                    - overallResults["diffFirstMs"].iloc[0]
                )

    return stageId, stagesInfo, overallResults


## Update UI widgets


def update_year_select():
    season_info = seasonInfo()
    if season_info.empty:
        return
    years = [str(i) for i in season_info["year"].unique()]

    # HACK

    ui.update_select("year", choices=years, selected=years[-1])


@reactive.effect
@reactive.event(input.year)
def update_rally_seasonId_select():
    season_info = seasonInfo()
    if season_info.empty:
        return

    ui.update_select("category", choices={})
    ui.update_select("event_day", choices={})
    ui.update_select("stage", choices={})
    ui.update_select("season_round", choices={})
    ui.update_select("rally_seasonId", choices={})

    year = int(input.year())

    rally_seasonIds = (
        season_info[season_info["year"] == year].set_index("seasonId")["name"].to_dict()
    )

    wrc.championshipLookup = rally_seasonIds

    ui.update_select("rally_seasonId", choices=rally_seasonIds)


@reactive.effect
@reactive.event(input.year, input.rally_seasonId)
def update_season_round_select():
    seasonId = input.rally_seasonId()
    if not seasonId:
        return
        # TO DO Reset downstream selectors

    # Set Championship code as seasonId TO DO

    ui.update_select("category", choices={})
    ui.update_select("event_day", choices={})
    ui.update_select("stage", choices={})
    ui.update_select("season_round", choices={})

    seasonId = int(seasonId)
    if not seasonId:
        return
    if seasonId in wrc.championshipLookup:
        wrc.championship = wrc.CHAMPIONSHIP_CODES[wrc.championshipLookup[seasonId]]

    wrc.setSeason(seasonId=seasonId)

    season_rounds = wrc.getSeasonRounds(on_season=True)
    if season_rounds.empty:
        ui.update_select("season_round", choices={})
        return

    # Get most current / previous event
    current_event = season_rounds.sort_values("startDate")[
        season_rounds["startDate"] <= dateNow(weekend=True)
    ]["eventId"].iloc[-1]

    season_rounds = (
        season_rounds[season_rounds["seasonId"] == seasonId]
        .set_index("eventId")["name"]
        .to_dict()
    )

    # TO DO Reset downstream selectors
    ui.update_select("category", choices={})
    ui.update_select("event_day", choices={})
    ui.update_select("stage", choices={})

    ui.update_select("season_round", choices=season_rounds, selected=str(current_event))


@reactive.effect
@reactive.event(input.rally_seasonId, input.season_round)
def update_category_select():
    seasonId = input.rally_seasonId()
    eventId = input.season_round()
    if not seasonId or not eventId:
        ui.update_select("category", choices={})
        return
    # The eventId and rallyId need to be set by this point
    getEventData()

    entries = wrc.getEntries(on_event=True)
    eligibilities = entries["eligibility"].unique().tolist()
    priorities = entries["priority"].unique().tolist()
    _categories = ["All"]

    # HACK - the following coding is contrived
    # TO DO - how is priority in api handled for ERC?
    CMAP = {
        "wrc": {"All": "P0", "WRC": "P1", "WRC2": "P2", "WRC3": "P3", "JWRC": "P4"},
        "erc": {
            "All": "P0",
            "ERC1": "ERC1",
            "ERC3": "ERC3",
            "ERC4": "ERC4",
            # "FIA_ERC1": "FIA/ ERC1",
        },
    }

    # TO DO - set wrc or erc

    cmap = CMAP[wrc.championship]
    if wrc.championship == "wrc":
        for c1 in eligibilities:
            for c2 in c1.split():
                c2 = c2.strip()
                if c2.startswith("M"):
                    if "WRC" not in _categories:
                        _categories.append("WRC")
                elif (
                    c2
                    and c2 != "/"
                    and not c2.startswith("(")
                    and c2 not in _categories
                ):
                    _categories.append(c2)
    elif wrc.championship == "erc":
        for c1 in priorities:
            c1 = c1.replace("FIA/", "").strip()
            # eligibilities: eg ERC, T; ERC, M; ERC, T, M; ERC4 (J), T
            if c1 not in _categories:
                _categories.append(c1)

    # TO DO - the downstream logic for this is wrong because
    # JWRC is (or, weaker, may also be?) WRC3
    # TO DO - this needs updating for ERC cf. WRC
    categories = {cmap[c]: c for c in _categories if c in cmap}
    # TO DO - set to WRC or ERC if available?
    selected = ""
    if "P1" in categories:
        selected = "P1"
    elif "ERC1" in categories:
        selected = "ERC1"
    ui.update_select("category", choices=categories, selected=selected)


@reactive.effect
@reactive.event(input.rally_seasonId, input.season_round)
def update_day_select():
    eventId = input.season_round()
    if not eventId:
        ui.update_select("event_day", choices={})
        return
    # Ensure the event data is loaded
    getEventData()

    days_ = wrc.getItineraryLegs(eventId=eventId)
    days = {0: "All"}

    days.update(days_.set_index("itineraryLegId")["name"].to_dict())

    ui.update_select("event_day", choices=days)


@reactive.effect
@reactive.event(input.season_round, input.event_day)
def update_section_select():
    itineraryLegId = input.event_day()
    eventId = input.season_round()
    if not eventId or not itineraryLegId:
        ui.update_select("event_section", choices={})
        return
    # Ensure the event data is loaded
    getEventData()
    eventId = int(eventId) if eventId else eventId
    itineraryLegId = int(itineraryLegId) if itineraryLegId else itineraryLegId
    sections_ = wrc.getItinerarySections(itineraryLegId=itineraryLegId, eventId=eventId)
    sections = {0: "All"}
    sections.update(sections_.set_index("itinerarySectionId")["name"].to_dict())

    ui.update_select("event_section", choices=sections)


@reactive.effect
@reactive.event(
    input.season_round,
    input.event_day,
)
def update_startlist_select():
    eventId = input.season_round()
    if not eventId:
        ui.update_select("startlist", choices={})
        return
    eventId = int(eventId) if eventId else None
    startlist = wrc.getStartList(eventId=eventId)
    if startlist.empty:
        return
    # Need to select today
    startLists = startlist["name"].unique().tolist()
    if startLists:
        # TO DO - this is a hack ? We assume the name of the startlist is a day
        day_today = datetime.today().strftime("%A")
        if len(startLists) == 1 or day_today not in startLists:
            startwith = startLists[0]
        else:
            startwith = day_today
        ui.update_select("startlist", choices=startLists, selected=startwith)


@reactive.effect
@reactive.event(
    input.rally_seasonId, input.season_round, input.event_day, input.event_section
)
def update_stage_select():
    eventId = input.season_round()
    itineraryLegId = input.event_day()
    itinerarySectionId = input.event_section()
    if not eventId or not itineraryLegId or not itinerarySectionId:
        ui.update_select("stage", choices={})
        return

    eventId = int(eventId)
    itineraryLegId = int(itineraryLegId)
    itinerarySectionId = int(itinerarySectionId)

    stages = wrc.getItineraryStages(
        eventId=eventId,
        itineraryLegId=itineraryLegId,
        itinerarySectionId=itinerarySectionId,
    )

    stages["label"] = stages.apply(lambda row: f"{row['code']} ({row['name']})", axis=1)
    stages = stages.set_index("stageId")["label"].to_dict()
    if wrc.isRallyLive():
        live_stages = wrc.getLiveStages()
        if not live_stages.empty:
            live_stage = live_stages["stageId"].iloc[-1]
            if live_stage in stages:
                ui.update_select("stage", choices=stages, selected=str(live_stage))
    else:
        ui.update_select("stage", choices=stages)


@reactive.effect
@reactive.event(input.season_round, input.stage, input.category)
def update_stages_driver_rebase_select():
    priority = input.category()
    stageId = input.stage()
    if not stageId:
        return
    stageId = int(stageId)
    stage_times_df = wrc.getStageTimes(stageId=stageId, priority=priority, raw=False)
    if stage_times_df.empty:
        return
    rebase_drivers = (
        stage_times_df[["carNo", "driverName"]]
        .set_index("carNo")["driverName"]
        .to_dict()
    )

    ui.update_select("stage_rebase_driver", choices=rebase_drivers)

    ui.update_select("rally_progression_rebase_driver", choices=rebase_drivers)

    ui.update_select("splits_heatmap_driver", choices=rebase_drivers)

    rebase_drivers["ult"] = "ULTIMATE"
    ui.update_select("rebase_driver", choices=rebase_drivers)


@reactive.effect
@reactive.event(input.rally_seasonId, input.category)
def update_championships_select():
    seasonId = input.rally_seasonId()
    if not seasonId:
        return
    championships_df = getChampionships()  # name, type,
    if championships_df.empty:
        return
    championships = (
        championships_df[["championshipId", "name"]]
        .set_index("championshipId")["name"]
        .str.replace("World Rally Championship", "WRC")
        .str.replace("European Rally Championship", "ERC")
        .str.replace("Championship for", "")
        .str.replace("Masters ", "")
        .str.replace(" for ", " ")
        .to_dict()
    )
    ui.update_select("championships", choices=championships)
    ui.update_select("event_championships", choices=championships)


## Other core functions


def prepare_split_times_data(stageId, rebase_driver):
    """Common setup code for split times visualizations"""
    # Validate stage selection
    if not stageId:
        return None, None, "No stage selected..."

    # Validate rebase driver
    if not rebase_driver:
        return None, None, "No rebase driver selected..."

    # Convert rebase_driver to proper format
    rebase_driver = int(rebase_driver) if rebase_driver != "ult" else rebase_driver

    # Get split times data
    split_times_wide = get_split_times_wide().copy()
    if split_times_wide is None or split_times_wide.empty:
        return None, None, "No split times data..."

    return split_times_wide, rebase_driver, None


def split_sections_map_core(wrc, stageId, geostages, ax=None, heat_colours=None):
    stages_info = wrc.getStageInfo(raw=False)
    splits = wrc.getStageSplitPoints(stageId=int(stageId))
    if splits.empty or stages_info.empty:
        return

    stage_info = stages_info[stages_info["stageId"] == int(stageId)].iloc[0]

    if not heat_colours:
        colors = ["lightgrey", "blue"]
        colors = [colors[i % len(colors)] for i in range(len(splits) + 1)]
    else:
        colors = heat_colours

    if not ax:
        fig2, ax = plt.subplots()

    dists = (splits["distance"] * 1000).tolist()

    geostage = geostages[geostages["stages"].apply(lambda x: stage_info["code"] in x)]
    if geostage.empty:
        return
    line = geostage["geometry"].iloc[0]
    gdf_segments2 = wrcapi.GeoTools.route_N_segments_meters(line, dists, toend=True)
    gdf_segments2.plot(ax=ax, lw=3, color=colors)
    ax.set_axis_off()

    # Add a green dot at start and a red dot at endl s is the dot size
    # Get first point coordinates from first row
    first_x, first_y = gdf_segments2.iloc[0].geometry.coords[0]
    # Get last point coordinates from last row
    last_x, last_y = gdf_segments2.iloc[-1].geometry.coords[-1]
    # Plot the points directly with matplotlib
    ax.scatter(first_x, first_y, color="green", s=10, zorder=5)
    ax.scatter(last_x, last_y, color="red", s=10, zorder=5)

    return ax


## Reactive calcs


@reactive.calc
@reactive.event(input.season_round, input.category)
def getStageWinners():
    # TO DO - this needs fixing to reflect category
    priority = input.category()

    # TO DO priority not yet handled below??
    stagewinners = wrc.getStageWinners(priority=priority, raw=False)
    stagewinners = enrich_stage_winners(stagewinners)

    return stagewinners


@reactive.calc
@reactive.event(
    input.rally_seasonId, input.season_round, input.event_day, input.event_section
)
def getItinerary():
    eventId = input.season_round()
    itineraryLegId = input.event_day()
    itinerarySectionId = input.event_section()

    eventId = int(eventId) if eventId else eventId
    itineraryLegId = int(itineraryLegId) if itineraryLegId else itineraryLegId
    itinerarySectionId = (
        int(itinerarySectionId) if itinerarySectionId else itinerarySectionId
    )

    itinerary = wrc.getItineraryControls(
        eventId=eventId,
        itineraryLegId=itineraryLegId,
        itinerarySectionId=itinerarySectionId,
    )
    return itinerary


@reactive.calc
@reactive.event(input.stage, input.category, input.display_latest_overall)
def get_overall_pos_wide():
    stageId = input.stage()
    if not stageId:
        return DataFrame()
    stageId = int(stageId)
    # TO DO - up to
    stageId = None
    priority = input.category()
    running = not input.display_latest_overall()
    overall_times_wide = wrc.getStageOverallWide(
        stageId=stageId,
        priority=priority,
        completed=True,
        running=running,
        typ="position",
    )  # typ: position, totalTimeInS

    return overall_times_wide


@reactive.calc
@reactive.event(input.rally_seasonId, input.championships)
def getChampionships():
    # print("get championship")
    seasonId = input.rally_seasonId()
    if not seasonId:
        return DataFrame()
    championships = wrc.getChampionships(seasonId=seasonId)
    championshipId = (
        championships[championships["type"] == "Drivers"]["championshipId"].iloc[0]
        if not input.championships()
        else int(input.championships())
    )
    wrc.setChampionship(championshipId=championshipId)
    return championships


@reactive.calc
@reactive.event(
    input.stage,
    input.category,
    input.progression_report_type,
    input.display_latest_overall,
)
def get_overall_typ_wide():
    stageId = input.stage()
    if not stageId:
        return DataFrame()
    stageId = int(stageId)
    progression_report_typ = input.progression_report_type()
    # TO DO - up to
    stageId = None
    priority = input.category()
    running = not input.display_latest_overall()
    return _get_overall_typ_wide_core(
        stageId, priority, progression_report_typ, running=running
    )


def _get_overall_typ_wide_core(
    stageId, priority, progression_report_typ, running=False
):
    typ = progression_report_types[progression_report_typ]
    if "rally" in progression_report_typ.lower():
        overall_times_wide = wrc.getStageOverallWide(
            stageId=stageId, priority=priority, completed=True, running=running, typ=typ
        )  # typ: position, totalTimeInS
    elif "stage" in progression_report_typ.lower():
        overall_times_wide = wrc.getStageTimesWide(
            stageId=stageId, priority=priority, completed=True, running=running, typ=typ
        )
    else:
        overall_times_wide = DataFrame()
    return overall_times_wide


def get_overall_typ_wide_core_1(
    stageId, progression_report_typ, priority, running, rebase_driver
):

    overall_times_wide = _get_overall_typ_wide_core(
        stageId, priority, progression_report_typ, running=running
    )

    if overall_times_wide.empty or not rebase_driver:
        return DataFrame()

    stage_cols = wrc.getStageCols(overall_times_wide)
    rebase_driver = (
        int(rebase_driver)
        if rebase_driver and rebase_driver != "ult"
        else rebase_driver
    )
    output_ = wrc.rebaseManyTimes(
        overall_times_wide, rebase_driver, "carNo", stage_cols
    )
    return output_


@reactive.calc
@reactive.event(
    input.stage,
    input.category,
    input.rally_progression_rebase_driver,
    input.progression_rebase_type,
    input.display_latest_overall,
)
def get_overall_typ_wide2_rebased():
    return get_overall_typ_wide_core_1(
        stageId=None,
        progression_report_typ=input.progression_rebase_type(),
        priority=input.category(),
        running=not input.display_latest_overall(),
        rebase_driver=input.rally_progression_rebase_driver(),
    )


@reactive.calc
@reactive.event(
    input.stage,
    input.category,
    input.rally_progression_rebase_driver,
    input.display_latest_overall,
)
def get_overall_typ_wide3_rebased():
    return get_overall_typ_wide_core_1(
        stageId=None,
        progression_report_typ="byrallytime",
        priority=input.category(),
        running=not input.display_latest_overall(),
        rebase_driver=input.rally_progression_rebase_driver(),
    )


@reactive.calc
@reactive.event(input.stage, input.category, input.splits_refresh)
def get_split_times_wide():
    """Cache for split times wide data"""
    stageId = input.stage()
    if not stageId:
        return DataFrame()
    stageId = int(stageId)
    priority = input.category()

    return wrc.getSplitTimesWide(stageId=stageId, priority=priority, extended=True)


@reactive.calc
@reactive.event(
    input.stage, input.category, input.splits_section_view, input.splits_refresh
)
def get_scaled_splits():
    """Cache for scaled splits data"""
    stageId = input.stage()
    if not stageId:
        return None
    stageId = int(stageId)
    priority = input.category()
    view = input.splits_section_view()

    return wrc.getScaledSplits(stageId, priority, view)


@reactive.calc
@reactive.event(
    input.stage_review_accordion,
    input.category,
    input.stage,
)
def get_stage_data():
    stageId = input.stage()
    if not stageId:
        return None
    stageId = int(stageId)
    priority = input.category()
    stage_times_df = wrc.getStageTimes(stageId=stageId, priority=priority, raw=False)
    if stage_times_df.empty:
        return None

    # Add position and road position processing
    stage_times_df["roadPos"] = range(1, len(stage_times_df) + 1)
    stage_times_df["position_"] = stage_times_df["position"]
    if priority != "P0" and priority != "P1":
        stage_times_df.sort_values("position", inplace=True)
        stage_times_df["position"] = range(1, len(stage_times_df) + 1)
        stage_times_df.sort_values("roadPos", inplace=True)

    return stage_times_df


# Create a reactive cached function for rebased calculations
@reactive.calc
@reactive.event(
    input.stage_review_accordion,
    input.category,
    input.stage,
    input.stage_rebase_driver,
)
def get_rebased_data():
    stage_times_df = get_stage_data()
    if stage_times_df is None:
        return None

    # Get rebase driver
    rebase_driver = input.stage_rebase_driver()
    # Return if no rebase driver selected
    if not rebase_driver:
        return stage_times_df
    rebase_driver = (
        int(rebase_driver)
        if rebase_driver and rebase_driver != "ult"
        else rebase_driver
    )

    # TO DO - this should be in wrc. ?

    # Add percentage time column using rebase driver time basis
    rebase_time = stage_times_df.loc[
        stage_times_df["carNo"] == rebase_driver, "timeInS"
    ].iloc[0]
    stage_times_df["Rebase %"] = (100 * stage_times_df["timeInS"] / rebase_time).round(
        1
    )

    # Calculate rebased gap
    rebase_gap_col = "Rebase Gap (s)"
    stage_times_df[rebase_gap_col] = stage_times_df["Gap"].round(1)
    stage_times_df.loc[:, rebase_gap_col] = wrc.rebaseTimes(
        stage_times_df, rebase_driver, "carNo", rebase_gap_col
    )

    # Calculate rebased pace difference
    rebase_pace_df = stage_times_df.loc[
        stage_times_df["carNo"] == rebase_driver, "pace (s/km)"
    ]

    if not rebase_pace_df.empty:
        rebase_pace = rebase_pace_df.iloc[0]
        stage_times_df["Rebase pace diff (s/km)"] = (
            stage_times_df["pace (s/km)"] - rebase_pace
        ).round(2)
    else:
        stage_times_df["Rebase pace diff (s/km)"] = None

    return stage_times_df


## Start the data collection
wrc.seedDB()
update_year_select()
