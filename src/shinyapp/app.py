from shiny import render, reactive
from shiny.express import ui, input
from shiny import ui as uis
from wrc_rallydj.utils import enrich_stage_winners

from icons import question_circle_fill

from wrc_rallydj.livetiming_api2 import WRCLiveTimingAPIClientV2

wrc = WRCLiveTimingAPIClientV2(use_cache=True, backend="memory", expire_after=6000)


ui.panel_title("RallyDataJunkie WRC Results and Timing Browser", "WRC-RallyDJ")


def seasonInfo():
    return wrc.getSeasons()


with ui.sidebar(open="desktop"):
    # Create season selector
    # Currently offers a hard coded set of season year options
    ui.input_select(
        "year",
        "Year:",
        [str(i) for i in seasonInfo()["year"].unique()],
        selected="2025",
    )

    ui.input_select(
        "rally_seasonId",
        "Championship:",
        {},
    )

    ui.input_select(
        "season_round",
        "Rounds:",
        {},
    )

    # Create stages selector
    ui.input_select(
        "stage",
        "Stage:",
        {},
    )

# The accordion widget provides collapsible elements
with ui.accordion(open=False):
    with ui.accordion_panel("About"):

        @render.ui
        def about():
            txt = "This website was developed in order to explore the use of Shinylive-Python for developing in-browser reactive Python applications, and to support personal use (reasearch, analysis, visualisation, reporting) of timing and results data arising from WRC rally events.\n\nThe website may contain errors resulting from the processing of the data: please file an issue at https://github.com/RallyDataJunkie/wrc-shinylive/issues if you notice an error.\n\n*This website is unofficial and is not associated in any way with WRC Promoter GmbH. WRC WORLD RALLY CHAMPIONSHIP is a trade mark of the FEDERATION INTERNATIONALE DE L'AUTOMOBILE.*"
            return ui.markdown(txt)

    with ui.accordion_panel("Season info"):

        @render.data_frame
        def season_frame():
            season = wrc.getSeasonRounds()
            if season.empty:
                return
            retcols = [
                "order",
                "name",
                "country.name",
                "location",
                "startDate",
                "surfaces",
            ]
            return render.DataGrid(season[retcols])

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

        ui.input_checkbox(
            "display_latest_overall",
            "Display latest result",
            True,
        )

        @render.ui
        @reactive.event(input.stage, input.display_latest_overall)
        def rally_overview_latest_hero():
            setStageData()
            stagesInfo = wrc.getStageInfo().sort_values(by="number", ascending=True)
            stageId = None
            if input.display_latest_overall() and "status" in stagesInfo:
                completed_stages = stagesInfo[stagesInfo["status"] == "Completed"]
                if not completed_stages.empty:
                    stageId = completed_stages.iloc[-1]["stageId"]
            else:
                stageId = input.stage()

            if stageId and not stagesInfo.empty:
                stageId = int(stageId)
                overallResults = wrc.getStageResults(stageId=stageId)
                if not overallResults.empty:
                    return get_overall_result_hero(stageId, stagesInfo, overallResults)
            else:
                print("Missing stage results data?")

        with ui.card(class_="mt-3"):
            with ui.card_header():
                with ui.tooltip(placement="right", id="rally_stages_overview_tt"):
                    ui.span(
                        "Rally stages overview ",
                        question_circle_fill,
                    )
                    "Summary card for rally event."

            with ui.accordion(open=False, id="stage_accordion"):

                with ui.accordion_panel("Stages info"):

                    @render.data_frame
                    @reactive.event(input.stage)
                    def stages_frame():
                        setStageData()
                        stages = wrc.getStageInfo()
                        if stages.empty:
                            return
                        retcols = [
                            "code",
                            "name",
                            #"day",
                            "distance",
                            "stageType",
                            "status",
                        ]
                        return render.DataGrid(stages[retcols])

                with ui.accordion_panel("Stage winners"):

                    @render.data_frame
                    def stage_winners_short():
                        stagewinners = getStageWinners()
                        if stagewinners.empty:
                            return
                        # TO DO - need enrichers
                        retcols = [
                            # "stageType",
                            "stageName",
                            "abbvName",
                            "elapsedDuration",
                            # "day",
                            # "carNo",
                            # "driver",
                            # "coDriver",
                            # "time",
                            # "teamName",
                            # "eligibility",
                            # "wins_overall",
                            # "daily_wins",
                            # "timeInS",
                            # "distance",
                            # "pace (s/km)",
                            # "speed (km/h)",
                        ]
                        # TO DO have option to limit view of stages up to and including selected stage
                        return render.DataGrid(stagewinners[retcols])

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
                            "abbvName",
                            "code",
                            "reason",
                            "retirementDateTime",
                            "status",
                        ]
                        return render.DataGrid(retirements[retcols])

                with ui.accordion_panel("Penalties"):
                    # TO DO - ideally, this and retirements
                    # would only react when the accordion is opened?
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
                        retcols = ["abbvName", "code", "penaltyDuration", "reason"]
                        return render.DataGrid(penalties[retcols])


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


## Update UI widgets
@reactive.effect
@reactive.event(input.year)
def update_rally_seasonId_select():
    season_info = seasonInfo()
    if season_info.empty:
        return
    year = int(input.year())

    rally_seasonIds = (
        season_info[season_info["year"] == year].set_index("seasonId")["name"].to_dict()
    )

    ui.update_select("rally_seasonId", choices=rally_seasonIds)


@reactive.effect
@reactive.event(input.year, input.rally_seasonId)
def update_season_round_select():
    season_rounds = wrc.getSeasonRounds()
    seasonId = input.rally_seasonId()
    if season_rounds.empty or not seasonId:
        return

    seasonId = int(seasonId)

    season_rounds = (
        season_rounds[season_rounds["seasonId"] == seasonId]
        .set_index("eventId")["name"]
        .to_dict()
    )

    ui.update_select("season_round", choices=season_rounds)


@reactive.effect
@reactive.event(input.season_round)
def update_stage_select():
    eventId = input.season_round()
    if not eventId:
        return
    eventId = int(eventId)

    # Ensure the event data is loaded
    getEventData()

    stages = wrc.getItinerarySections(eventId=eventId)

    stages["label"] = stages.apply(lambda row: f"{row['code']} ({row['name']})", axis=1)
    stages = stages.set_index("stageId")["label"].to_dict()

    ui.update_select("stage", choices=stages)


def get_overall_result_hero(stageId, stages_data, overall_data):
    stage_ = stages_data[stages_data["stageId"] == stageId].iloc[0]
    stage_name = stage_["name"]
    stage_code = stage_["code"]
    # TO DO  - what if there is a joint position?
    def _get_hero_text(record):
        if not record.empty:
            record = record.iloc[0]
            return ui.markdown(
                f"""
                __{record["entryId"]}__  
                {record["stageTime"]}  
                """
            )

    # Positions are zero-indexed
    ##averaging = round(overall_df.loc[0, "speed (km/h)"], 1)
    # TO DO - if this is final result, we can use overall dist for speed
    # averaging = f"Averaging  \n  \n{averaging} km/h" if averaging else ""
    p1_ = overall_data[overall_data["position"] == 1]
    p1 = ui.value_box(
        title=stage_name,
        value=_get_hero_text(p1_),
        theme="text-green",
        # showcase=averaging,
        # showcase_layout="left center",
        full_screen=True,
    )

    if len(overall_data) > 1:
        # p2pace = round(times.loc[1, "pace diff (s/km)"], 2)
        # p2pace = p2pace if p2pace else ""
        # p2pace = f'(Pace: {p2pace} s/km slower)'
        p2_ = overall_data[overall_data["position"] == 2]
        p2 = ui.value_box(
            value= p2_.iloc[0]["diffFirst"],
            title=_get_hero_text(p2_),
            theme="text-blue",
            # showcase=p2pace,
            # showcase_layout="bottom",
            full_screen=True,
        )
        if len(overall_data) > 2:
            # p3pace = round(overall_df.loc[2, "pace diff (s/km)"], 2)
            # p3pace = p3pace if p3pace else ""
            # p3pace = f"(Pace: {p3pace} s/km slower)"
            p3_ = overall_data[overall_data["position"] == 3]
            p3 = ui.value_box(
                value=p3_.iloc[0]["diffFirst"],
                title=_get_hero_text(p3_),
                theme="text-purple",
                # showcase=p3pace,
                # showcase_layout="bottom",
                full_screen=True,
            )
            return ui.TagList(p1, uis.layout_columns(p2, p3))
        return ui.TagList(p1, p2)

    return p1


# TO DO


@reactive.calc
@reactive.event(input.season_round)
def getStageWinners():
    return wrc.getStageWinners(raw=False)

"""
@reactive.calc
@reactive.event(input.event, input.championship, input.stage)
def stage_winners_data():
    stagewinners = wrc.getStageWinners(update=True)
    stages = stages_data()
    stagewinners = enrich_stage_winners(stagewinners, stages)

    return stagewinners
"""
