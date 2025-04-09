from shiny import render, reactive
from shiny.express import ui, input
from shiny import ui as uis
from wrc_rallydj.utils import enrich_stage_winners, format_timedelta
from datetime import datetime
from icons import question_circle_fill
from pandas import DataFrame, melt
from matplotlib import pyplot as plt
from seaborn import barplot, boxplot, heatmap, lineplot
from matplotlib.colors import LinearSegmentedColormap
from adjustText import adjust_text

# from shinywidgets import render_widget
# from itables.widget import ITable


from wrc_rallydj.livetiming_api2 import WRCTimingResultsAPIClientV2

wrc = WRCTimingResultsAPIClientV2(
    use_cache=True, backend="memory", expire_after=30, liveCatchup=True
)


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

        # TO DO - overall report
        # TO DO - day report
        # TO DO - section/loop report

        @render.ui
        @reactive.event(input.stage, input.display_latest_overall)
        def rally_overview_latest_hero():
            setStageData()
            stagesInfo = wrc.getStageInfo(on_event=True).sort_values(
                by="number", ascending=True
            )
            stageId = None
            if input.display_latest_overall() and "status" in stagesInfo:
                completed_stages = stagesInfo[stagesInfo["status"] == "Completed"]
                if not completed_stages.empty:
                    stageId = completed_stages.iloc[-1]["stageId"]
            else:
                stageId = input.stage()

            if stageId and not stagesInfo.empty:
                stageId = int(stageId)
                overallResults = wrc.getStageOverallResults(stageId=stageId, raw=False)
                if not overallResults.empty:
                    return get_overall_result_hero(stageId, stagesInfo, overallResults)
            else:
                print("Missing stage results data?")

        ui.markdown("*Progress across stages.*")

        @render.data_frame
        @reactive.event(input.stage, input.event_day, input.event_section)
        def stage_progress_frame():
            overall_times_wide = get_overall_pos_wide()
            if overall_times_wide.empty:
                return
            # TO DO - this should have options for within and accumualted statge time views
            # as well as a driver rebase option
            return render.DataGrid(overall_times_wide.copy().drop(columns="entryId"))

        @render.plot(
            alt="Line chart of overall rally positions."
        )
        @reactive.event(
            input.splits_review_accordion, input.category, input.stage
        )
        def seaborn_linechart_stage_progress_positions():
            overall_times_wide = get_overall_pos_wide()
            if overall_times_wide.empty:
                return

            overall_cols = wrc.getOverallStageCols(overall_times_wide)
            overall_times_wide[overall_cols] = overall_times_wide[overall_cols].apply(
                lambda col: col.rank(method="min", ascending=True)
            )
            overall_times_pos_long = melt(
                overall_times_wide,
                id_vars=["carNo", "driverName"],
                value_vars=overall_cols,
                var_name="roundN",
                value_name="position",
            )
            ax = lineplot(
                data=overall_times_pos_long,
                x="roundN",
                y="position",
                hue="carNo",
                legend=False,
            )
            x_min, x_max = ax.get_xlim()
            for car in overall_times_pos_long["carNo"].unique():
                # Filter data for this car and get the last round's position
                first_point = overall_times_pos_long[
                    (overall_times_pos_long["carNo"] == car)
                    & (
                        overall_times_pos_long["roundN"]
                        == overall_times_pos_long["roundN"]
                    .iloc[0])
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
                    y_pos = last_point["position"].values[0]

                    # Add text slightly to the right of the maximum round
                    ax.text(
                        x_max + 0.05,
                        y_pos,
                        f"#{car}",
                        verticalalignment="center",
                        fontsize=9,
                    )
            ax.set(xlabel=None, ylabel="Overall Position")
            ax.invert_yaxis()
            plt.xlim(x_min - 0.5, x_max + 0.5)
            return ax

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
                    @reactive.event(input.stage, input.event_day, input.event_section)
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
                        startlist = startlist[startlist["name"] == input.startlist()]
                        return render.DataGrid(startlist[retcols])

                with ui.accordion_panel("Shakedown"):

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
                        shakedown_df["runTime"] = shakedown_df["runDurationMs"].apply(
                            format_timedelta
                        )
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
                        df = getStageWinners()
                        if df.empty:
                            return
                        # TO DO - make use of commented out elements
                        # which limit counts  up to and including current stage
                        # df["_stagenum"] = df["stageNo"].str.replace("SS", "")
                        # df["_stagenum"] = df["_stagenum"].astype(int)

                        # idx = df[df["stageId"] == input.stage()].index
                        # if len(idx) == 0:
                        #    return]

                        # Drop empty rows
                        # df = df[df["carNo"].str.strip() != ""]
                        # Get value counts and reset index to create a plotting dataframe
                        stage_counts = (
                            # df.iloc[: idx[0] + 1]
                            df.groupby("driverName")["code"]
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

            @render.ui
            def stageresult_hero():
                setStageData()
                stagesInfo = wrc.getStageInfo(on_event=True).sort_values(
                    by="number", ascending=True
                )
                stageId = input.stage()
                if stageId and not stagesInfo.empty:
                    stageId = int(stageId)
                stage_times_data = wrc.getStageTimes(raw=False)
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

                        stages_info = wrc.getStageInfo()
                        stage_info = stages_info[
                            stages_info["stageId"] == stageId
                        ].iloc[0]

                        # Remark on stage name
                        _md = f"""{stage_info["code"]} {stage_info["name"]} ({stage_info["distance"]}km)"""
                        md.append(_md)

                        # Remark on, or imply, the repeated run nature of this stage
                        # TO DO

                        # Remark on being the Nth stage of the day
                        # TO DO

                        # Remark on number of split points
                        # TO DO
                        # For example: There are three split points, at Pkm, Qkm and Rkm.

                        # Remark on being the longest stage of the rally
                        if stage_info["distance"] == stages_info["distance"].max():
                            _md = f"{_md} It is the longest stage on the rally."
                            md.append(f"{_md}\n\n")

                        # Remark on previous liaison stage
                        # TO DO

                        # Remark on stage status
                        # TO DO

                        # Remarks on stage result / items of interest
                        # TO DO
                        # Remark on stage winner and their overall position
                        # Remark on stage winner's stage win stats.
                        # Remark on rally leader at start of stage position if not stage winner.
                        # Remark on rally leader at end of stage position if not stage winner.
                        # Remark on person losing overall lead, if approppriate.
                        # ... ?

                        # Remark on following liason stage
                        # TO DO

                        return ui.markdown("\n\n".join(md))

                with ui.accordion_panel("Overall position"):

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

                with ui.accordion_panel("Stage times"):
                    # Create stage driver rebase selector
                    ui.input_select(
                        "stage_rebase_driver",
                        "Driver rebase:",
                        {},
                    )

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
                            return

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
                            if input.rebase_reverse_palette():
                                bar.set_color("#2ecc71" if bar.get_width() > 0 else "#e74c3c")
                            else:
                                bar.set_color("#2ecc71" if bar.get_width() <= 0 else "#e74c3c")
                        ax.invert_xaxis()
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

                        cols = [c for c in cols if c in stage_times_df.columns]
                        return render.DataGrid(stage_times_df[cols])

    with ui.accordion_panel(title="Splits Analysis"):

        with ui.accordion(open=False, id="splits_review_accordion"):

            with ui.accordion_panel("Split times summary"):

                ui.markdown("*Position / elapsed time across the split sections.*")

                @render.plot(
                    alt="Line chart of elapsed split positions."
                )
                @reactive.event(
                    input.splits_review_accordion, input.category, input.stage
                )
                def seaborn_linechart_split_positions():
                    split_times_wide = get_split_times_wide().copy()
                    split_cols = wrc.getSplitCols(split_times_wide)
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

                @render.data_frame
                @reactive.event(
                    input.splits_review_accordion, input.category, input.stage
                )
                def split_results_wide():
                    split_times_data = get_split_times_wide()
                    if split_times_data is None or split_times_data.empty:
                        return
                    return render.DataGrid(split_times_data)

            with ui.accordion_panel("Split times detail"):

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
                    @reactive.event(input.stage, input.splits_section_view)
                    def split_report_view():
                        view = input.splits_section_view()
                        typ = {
                            "time": "Time (s) within each split (*lower* is better).",
                            "speed": "Speed (km/h) within each split (*higher* is better).",
                            "pace": "Pace (s/km) within each split (*lower* is better.)",
                            "time_acc": "Accumulated time (s) across all splits (*lower* is better).",
                            "pos_within": "Rank position within split (*lower* is better).",
                            "pos_acc": "Rank position of accumulated time at each split (*lower* is better).",
                        }
                        return ui.markdown(typ[view])

                    with ui.tooltip(id="splits_section_view_tt"):
                        ui.input_select(
                            "splits_section_view",
                            "Section report view",
                            {
                                "time": "Section time (s)",
                                "pace": "Av. pace in section (s/km)",
                                "speed": "Av. speed in section (km/h)",
                                "time_acc": "Acc. time over sections (s)",
                                "pos_within": "Section time rank",
                                "pos_acc": "Acc. time rank",
                            },
                            selected="time",
                        ),
                        "Select split section report type; time (s), position within or across splits, or, if available, average Pace (s/km) or average Speed (km/h)."
                        # Scope the view if data available

                    # @render.table
                    @render.data_frame
                    @reactive.event(
                        input.splits_section_view, input.stage, input.category
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
                        @reactive.event(input.stage, input.splits_section_view)
                        def plot_split_dists():
                            scaled_splits_wide = get_scaled_splits()
                            if scaled_splits_wide is None or scaled_splits_wide.empty:
                                return

                            split_cols = wrc.getSplitCols(scaled_splits_wide)
                            scaled_splits_long = melt(
                                scaled_splits_wide,
                                id_vars=["carNo", "driverName"],
                                value_vars=split_cols,
                                var_name="roundN",
                                value_name="value",
                            )
                            ylabel = "Time in section (s)"
                            view = input.splits_section_view()
                            if view == "pace":
                                ylabel = "Pace (s/km)"
                            elif view == "speed":
                                ylabel = "Speed (km/h)"

                            ax = boxplot(data=scaled_splits_long, x="roundN", y="value")
                            ax.set(xlabel=None, ylabel=ylabel)
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

                        with ui.tooltip(id="rebase_reverse_palette_tt"):
                            ui.input_checkbox(
                                "rebase_reverse_palette",
                                "Reverse rebase palette",
                                False,
                            ),
                            "Reverse the rebase palette to show deltas relative to the rebased driver's perspective."

                    # Create driver rebase selector
                    with ui.tooltip(id="rebase_driver_tt"):
                        ui.input_select(
                            "rebase_driver",
                            "Driver rebase:",
                            {},
                        ),
                        '"Rebase" times relative to a nominated driver. The "ULTIMATE" driver is derived from the quickest times within each split sector .'

                    @render.ui
                    @reactive.event(input.stage, input.rebase_driver)
                    def rebase_driver_info():
                        stageId = input.stage()
                        if not stageId:
                            return
                        stageId = int(stageId)
                        rebase_driver = input.rebase_driver()
                        # TO DO: provide ult view if rebase_driver=="ult"
                        if not rebase_driver or rebase_driver == "ult":
                            return
                        rebase_driver = int(rebase_driver)
                        stages = wrc.getStageInfo(stage_code=stageId, raw=False)
                        times = wrc.getStageTimes(stageId=stageId, raw=False)
                        if stages.empty or times.empty:
                            return ui.markdown("*No data available.*")

                        stages_ = stages[stages["stageId"] == stageId].iloc[0]
                        stage_name = stages_["name"]
                        stage_code = stages_["code"]

                        times_ = times[times["carNo"] == rebase_driver]
                        if times.empty:
                            return
                        times_ = times_.iloc[0]

                        def _get_hero_text():
                            return ui.markdown(
                                f"""
                        __#{times_["carNo"]} {times_["driverName"]}__  
                        {format_timedelta(times_["elapsedDurationMs"])}  
                        """
                            )

                        def _get_showcase():
                            diffFirst = format_timedelta(
                                times_["diffFirstMs"], addplus=True
                            )
                            diffFirst = (
                                "" if times_["position"] == 1 else f"__*{diffFirst}s*__"
                            )
                            speed = times_["speed (km/h)"]
                            pace = times_["pace diff (s/km)"]
                            pace = (
                                f"""{times_["pace (s/km)"]} s/km"""
                                if times_["position"] == 1
                                else f"*{round(pace, 2)} s/km off-pace*"
                            )
                            return ui.markdown(
                                f"""
                __P{times_["position"]}__ {diffFirst}  
                
                {round(speed,1)} km/h  
                {pace}
                """
                            )

                        pr = ui.value_box(
                            title=f"{stage_code} {stage_name}",
                            value=_get_hero_text(),
                            theme="text-black",
                            showcase=_get_showcase(),
                            showcase_layout="left center",
                            full_screen=True,
                        )
                        return pr

                    with ui.accordion(open=False):
                        with ui.accordion_panel("Heatmap"):

                            with ui.tooltip(id="heatmap_outliers_tt"):
                                ui.input_checkbox(
                                    "heatmap_outliers",
                                    "Heatmap outliers",
                                    False,
                                ),
                                "Calculate diff to leader z-scores to identify outliers."

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
                                        return

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
                                        return
                                    output_, split_cols = wrc.rebase_splits_wide_with_ult(
                                        split_times_wide, rebase_driver
                                    )

                                    output_.set_index("carNo", inplace=True)
                                    output_.columns = [
                                        f"Split {i}"
                                        for i in range(1, output_.shape[1] + 1)
                                    ]  # [:-1] + ["Finish"]

                                    if input.heatmap_outliers():
                                        z_scores = (
                                            output_ - output_.mean()
                                        ) / output_.std()
                                        output_ = z_scores
                                        # A boolen throws an inconsistent type error
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

                        with ui.accordion_panel("Split times group barplots"):
                            with ui.tooltip(id="splits_section_plot_tt"):
                                ui.input_select(
                                    "splits_section_plot",
                                    "Section plot view",
                                    {
                                        "bysplit": "Split section groups",
                                        "bydriver": "Driver groups",
                                    },
                                    selected="time",
                                ),
                                "Select split section report group; view split section difference by split sections group or driver group. Split section group is good for showing strong/weak sections; driver grouping is good for showing split section comparisons relative to each other driver."
                                # Scope the view if data available

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
                                    # print(f"Rebasing on {rebase_driver}")
                                    if not rebase_driver:
                                        return

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
                                        return

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

                                    if input.splits_section_plot() == "bydriver":
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
                                        if input.rebase_reverse_palette():
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

                        with ui.accordion_panel("Split times linecharts"):
                            with ui.card(class_="mt-3"):
                                with ui.card_header():
                                    with ui.tooltip(
                                        placement="right",
                                        id="splits_in_sectionlineplot_tt",
                                    ):
                                        ui.span(
                                            "Time gained / lost within each section in seconds relative to rebase driver (stacked barplot) ",
                                            question_circle_fill,
                                        )
                                        "Delta times within each split section. Times are relative to rebased driver's time. Bright column: good/bad split section for rebased driver. Bright row: good/bad sections for (row) driver."

                                @render.plot(
                                    alt="Line chart of within split delta times."
                                )
                                def seaborn_linechart_splits():
                                    stageId = input.stage()
                                    if not stageId:
                                        return

                                    rebase_driver = input.rebase_driver()
                                    # print(f"Rebasing on {rebase_driver}")
                                    if not rebase_driver:
                                        return

                                    rebase_driver = (
                                        int(rebase_driver)
                                        if rebase_driver != "ult"
                                        else rebase_driver
                                    )

                                    # We don't want to modify the cached split times df
                                    split_times_wide = get_split_times_wide().copy()
                                    if (
                                        split_times_wide is None
                                        or split_times_wide.empty
                                    ):
                                        return

                                    insert_point = f"{wrc.SPLIT_PREFIX}1"
                                    insert_loc = None
                                    if insert_point in split_times_wide.columns:
                                        insert_loc = split_times_wide.columns.get_loc(insert_point)
                                    elif wrc.SPLIT_FINAL in split_times_wide.columns:
                                        insert_loc = split_times_wide.columns.get_loc(
                                            wrc.SPLIT_FINAL
                                        )
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

                                    split_dists_ = wrc.getStageSplitPoints(
                                        stageId=stageId, extended=True
                                    )
                                    split_dists = split_dists_.set_index("name")[
                                        "distance"
                                    ].to_dict()

                                    # Add start point
                                    split_dists[f"{wrc.SPLIT_PREFIX}0"] = 0

                                    split_times_long["distance"] = split_times_long[
                                        "roundN"
                                    ].map(split_dists)

                                    g = lineplot(
                                        data=split_times_long.sort_values(["carNo", ]),
                                        x="distance",
                                        y="time",
                                        hue="carNo",
                                    )

                                    if rebase_driver and rebase_driver != "ult":
                                        g.set_ylim(g.get_ylim()[::-1])

                                    texts = []
                                    for line, label in zip(
                                        g.get_lines(),
                                        split_times_long.sort_values("carNo")[
                                            "carNo"
                                        ].unique(),
                                    ):
                                        x_data, y_data = (
                                            line.get_xdata(),
                                            line.get_ydata(),
                                        )
                                        x_last, y_last = x_data[-1], y_data[-1]
                                        text = g.text(
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
                                        arrowprops=dict(
                                            arrowstyle="-", color="gray", lw=0.5
                                        ),
                                    )

                                    g.set_xlim(
                                        split_times_long["distance"].min(),
                                        split_times_long["distance"].max() * 1.15,
                                    )

                                    g.legend_.remove()
                                    return g


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

    season_rounds = (
        season_rounds[season_rounds["seasonId"] == seasonId]
        .set_index("eventId")["name"]
        .to_dict()
    )
    # TO DO Reset downstream selectors
    ui.update_select("category", choices={})
    ui.update_select("event_day", choices={})
    ui.update_select("stage", choices={})

    ui.update_select("season_round", choices=season_rounds)


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

    rebase_drivers["ult"] = "ULTIMATE"
    ui.update_select("rebase_driver", choices=rebase_drivers)


## Reactive calcs


@reactive.calc
@reactive.event(input.season_round, input.category)
def getStageWinners():
    # TO DO - this needs fixing to reflect category
    priority = input.category()
    # TO DO priority not yet handled below
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
@reactive.event(input.stage, input.category)
def get_overall_pos_wide():
    stageId = input.stage()
    if not stageId:
        return DataFrame()
    stageId = int(stageId)
    # TO DO - up to
    stageId=None
    priority = input.category()
    overall_times_wide = wrc.getStageOverallWide(stageId=stageId, priority=priority, completed=True, typ="position") # typ: position, totalTimeInS

    return overall_times_wide

@reactive.calc
@reactive.event(input.stage, input.category)
def get_split_times_wide():
    """Cache for split times wide data"""
    stageId = input.stage()
    if not stageId:
        return None
    stageId = int(stageId)
    priority = input.category()

    return wrc.getSplitTimesWide(stageId=stageId, priority=priority, extended=True)


@reactive.calc
@reactive.event(input.stage, input.category, input.splits_section_view)
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
    stage_times_df = wrc.getStageTimes(
        stageId=stageId, priority=priority, raw=False
    )
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
    rebase_driver = int(rebase_driver) if rebase_driver else rebase_driver

    # Return if no rebase driver selected
    if not rebase_driver:
        return stage_times_df

    # Add percentage time column using rebase driver time basis
    rebase_time = stage_times_df.loc[stage_times_df["carNo"] == rebase_driver, "timeInS"].iloc[0]
    stage_times_df["Rebase %"] = (100 * stage_times_df["timeInS"] / rebase_time).round(1)

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

## Heros and banners

from .app_heroes import get_overall_result_hero, get_stage_result_hero

## App utils


## Start the data collection
wrc.seedDB()
update_year_select()
