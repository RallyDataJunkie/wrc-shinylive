# import pandas as pd
from pandas import set_option, DataFrame, melt, to_numeric, concat
from seaborn import heatmap, lineplot, barplot, boxplot
import json
from pathlib import Path

# import functools

# import requests_cache
# from requests_cache.session import CachedSession

from shiny import render, reactive
from shiny import ui as uis
from shiny.express import ui, input

from matplotlib import pyplot as plt
from matplotlib.colors import LinearSegmentedColormap
from datetime import datetime
from adjustText import adjust_text

from wrc_rallydj.livetiming_api import (
    WRCLiveTimingAPIClient
)

from wrc_rallydj.utils import time_to_seconds, enrich_stage_winners, scaled_splits
from icons import question_circle_fill
from rules_processor import p, Nth, core_stage, process_rally_overall_rules
from symbolic_analysis import get_splits_symbols
import re

set_option("display.colheader_justify", "left")

ui.tags.script(
    src="//gc.zgo.at/count.js",
    **{
        "data-goatcounter": "https://rallydatajunkie.goatcounter.com/count",
        "async": "",
    },
)

# The cacheing is tricky:
# - we want to be able to force updates for live stages etc
# There is internal state in the class, which stores
# the last requested data unless we force an update
wrc = WRCLiveTimingAPIClient(use_cache=True, backend="memory", expire_after=600)

ui.panel_title("RallyDataJunkie WRC Results and Timing Browser", "WRC-RallyDJ")

# Shiny Express API
# https://shiny.posit.co/py/api/express/

with ui.sidebar(open="desktop"):
    # Create season selector
    # Currently offers a hard coded set of season year options
    ui.input_select(
        "season", "Season:", [str(i) for i in range(2024, 2026)], selected="2025"
    )

    # Create championship selector
    ui.input_select(
        "championship",
        "Championship:",
        {v: k for k, v in wrc.CATEGORY_MAP.items()},
        selected="wrc",
    )

    @reactive.effect
    @reactive.event(input.championship)
    def update_championship():
        wrc.championship = input.championship()

    # Create event selector
    # Dynamically populated using a list of events
    # based on the season selection
    ui.input_select("event", "Event:", {})

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
            season = season_data()
            if season.empty:
                return
            retcols = [
                "ROUND",
                "rallyTitle",
                "rallyCountry",
                "date",
                "driver",
                "coDriver",
                "teamName",
                "manufacturer",
            ]
            return render.DataGrid(season[retcols])

    with ui.accordion_panel("Event overview"):

        @render.ui
        @reactive.event(input.event)
        def rally_overview_hero():
            season = season_data()
            event = input.event()
            round = season.loc[season["rallyId"] == event, "ROUND"].iloc[0]
            so = ui.value_box(
                title=season.loc[season["rallyId"] == event, "date"].iloc[0],
                value=season.loc[season["rallyId"] == event, "rallyTitle"].iloc[0],
                theme="text-black",
                showcase=ui.markdown(f"__ROUND {round}__"),
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
            stages = stages_data()
            stageId = None
            if input.display_latest_overall() and "STATUS" in stages:
                completed_stages = stages[stages["STATUS"] == "Completed"]
                if not completed_stages.empty:
                    last_stage = completed_stages.iloc[-1]["stageNo"]
                    if last_stage == "FINAL" and len(completed_stages)>1:
                        last_stage = completed_stages.iloc[-2]["stageNo"]
                else:
                    last_stage = ""

                stageId = wrc.stage_codes[last_stage]
                return get_overall_result_hero(stageId, stages_data, wrc.getOverall(stageId=stageId))
                # TO DO  - this is currently stage result hero;
                # need overall hero

            stageId = input.stage()
            if stageId != "SHD":
                return get_overall_result_hero(stageId, stages_data, overall_data)

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
                    @reactive.event(input.event)
                    def stages_frame():
                        stages = stages_data()
                        if stages.empty:
                            return
                        retcols = [
                            "stageNo",
                            "name",
                            "day",
                            "distance",
                            "STAGE TYPE",
                            "STATUS",
                        ]
                        return render.DataGrid(stages[retcols])

                with ui.accordion_panel(title="Itinerary"):

                    @render.data_frame
                    def itinerary_frame():
                        itinerary = itinerary_data()
                        if itinerary.empty:
                            return

                        retcols = [
                            "stage",
                            "type",
                            "status",
                            "distance",
                            "firstCarDueDateTimeMs",
                            "location",
                            "targetDuration",
                            "timingPrecision",
                            "controlPenalties",
                        ]
                        return render.DataGrid(itinerary)

                with ui.accordion_panel("Startlist"):

                    # Create startlist type selector
                    # Dynamically populated using available startlists
                    ui.input_select("startlist", "Startlist:", {})

                    @render.data_frame
                    @reactive.event(input.stage, input.startlist)
                    def startlist_frame():
                        startlist = wrc.getStartlist()
                        if startlist.empty:
                            return
                        retcols = [
                            "order",
                            "startList",
                            "priority",
                            "carNo",
                            "driver",
                            "coDriver",
                            "teamName",
                            "team/car",
                            "eligibility",
                            "groupClass",
                        ]
                        startlist = startlist[
                            startlist["startList"] == input.startlist()
                        ]
                        return render.DataGrid(startlist[retcols])

                with ui.accordion_panel("Stage winners"):

                    @render.data_frame
                    def stage_winners_short():
                        stagewinners = stage_winners_data()
                        if stagewinners.empty:
                            return

                        retcols = [
                            "stageType",
                            "stageName",
                            "stageNo",
                            "day",
                            "carNo",
                            "driver",
                            "coDriver",
                            "time",
                            "teamName",
                            "eligibility",
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
                        df = stage_winners_data()
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
                        df = df[df["carNo"].str.strip() != ""]
                        # Get value counts and reset index to create a plotting dataframe
                        stage_counts = (
                            # df.iloc[: idx[0] + 1]
                            df.groupby("driver")["stageNo"]
                            .count()
                            .sort_values(ascending=False)
                            .reset_index()
                        )

                        # Create figure with larger size for better readability
                        plt.figure(figsize=(10, 6))

                        # Create horizontal bar plot
                        ax = barplot(
                            data=stage_counts,
                            y="driver",
                            x="stageNo",
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
                    @reactive.event(input.event, input.stage, input.stage_accordion)
                    def retirements_frame():
                        if "Retirements" not in input.stage_accordion():
                            return
                        retirements = wrc.getRetirements()
                        if retirements.empty:
                            return
                        retcols = [
                            "carNo",
                            "driver",
                            "team/car",
                            "teamName",
                            "eligibility",
                            "groupClass",
                            "control",
                            "reason",
                        ]
                        return render.DataGrid(retirements[retcols])

                with ui.accordion_panel("Penalties"):
                    # TO DO - ideally, this and retirements
                    # would only react when the accordion is opened?
                    @render.data_frame
                    @reactive.event(input.event, input.stage, input.stage_accordion)
                    def penalties_frame():
                        if "Penalties" not in input.stage_accordion():
                            return
                        penalties = wrc.getPenalties()
                        if penalties.empty:
                            return
                        retcols = [
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
                        return render.DataGrid(penalties[retcols])

    with ui.accordion_panel("Stage Review"):
        with ui.card(class_="mt-3"):

            @render.ui
            def stageresult_hero():
                stageId = input.stage()
                return get_stage_result_hero(stageId, stages_data, stage_times_data)

            with ui.accordion(open=False):

                with ui.accordion_panel("Stage notes"):

                    @render.ui
                    def stage_text_intro():
                        stageId = input.stage()
                        if stageId == "SHD":
                            return ui.markdown("Shakedown...")
                        # TO DO - there will likely be errors if there are joint stage winners
                        # TO DO - cope with two or more winners
                        # TO DO - find an example of a joint stage win for debugging purposes

                        md = []
                        itinerary_df = itinerary_data()
                        stage_info = stages_data()
                        if stage_info.empty or itinerary_df.empty:
                            return

                        stage_code = wrc.stage_ids[stageId]
                        stage_info_row = stage_info.loc[
                            stage_info["stageNo"] == stage_code
                        ]
                        stage_name = stage_info_row.iloc[0]["name"]

                        ss_index = itinerary_df[
                            itinerary_df["stage"] == stage_code
                        ].index[0]

                        _md = stage_name
                        # Remark on, or imply, the repeated run nature of this stage
                        repeated_run = re.match(r".*\s(\d+)\s+\(.*", stage_name)
                        if repeated_run:
                            _md = f"{_md}, the {Nth(int(repeated_run.group(1)))} run of this stage"

                        # Remark on being the Nth stage of the day
                        _md = f"""{_md}, the {Nth(stage_info_row.iloc[0]["stageInDay"])}"""
                        if (
                            stage_info_row.iloc[0]["stageInDay"]
                            == stage_info[
                                stage_info["day"] == stage_info_row.iloc[0]["day"]
                            ]["stageInDay"].max()
                        ):
                            _md = f"{_md}, and last,"
                        start_time = datetime.fromisoformat(
                            itinerary_df.iloc[ss_index]["firstCarDueDateTimeMs"]
                        )
                        time_str = (
                            start_time.strftime("starting at %I.%M%p")
                            .lower()
                            .replace(" 0", " ")
                        )

                        _md = f"""{_md} stage of the day {stage_code} ({stage_info_row.iloc[0]["day"]}), {time_str}."""

                        # Remark on being the longest stage of the rally
                        if (
                            stage_info_row.iloc[0]["distance"]
                            == stage_info["distance"].max()
                        ):
                            _md = f"{_md} It is the longest stage on the rally."
                        md.append(f"{_md}\n\n")

                        # Previous liaison
                        previous_tc = itinerary_df.iloc[ss_index - 1]
                        previous_out = itinerary_df.iloc[ss_index - 2]
                        previous_location = (
                            f"previous {previous_out['location']} stage"
                            if previous_out["type"] == "FlyingFinish"
                            else f'{previous_out["location"]} {previous_out["type"]}'
                        )
                        art_ = p.a(
                            p.number_to_words(float(previous_tc["distance"].split()[0]))
                        ).split()[0]
                        _md = f'Prior to the stage, {art_} {previous_tc["distance"]} liaison section to the {previous_tc["location"]} {previous_tc["type"]} from the {previous_location}.'
                        md.append(_md)

                        # End of stage
                        future_ = itinerary_df.iloc[ss_index + 1 :]
                        # Get indices of time controls
                        next_tc_idx = future_[
                            future_["stage"].str.startswith("T")
                        ].index[0]
                        next_tc = itinerary_df.iloc[next_tc_idx]
                        art_ = p.a(
                            p.number_to_words(float(next_tc["distance"].split()[0]))
                        ).split()[0]
                        arrival_time = datetime.fromisoformat(
                            next_tc["firstCarDueDateTimeMs"]
                        )
                        next_arrival_time = (
                            arrival_time.strftime("from %I.%M%p")
                            .lower()
                            .replace(" 0", " ")
                        )
                        _md_final = f'Following the stage, {art_} {next_tc["distance"]} liaison section to {next_tc["location"]} (stage running from {next_arrival_time}).'

                        # Stage status
                        state_status = itinerary_df.iloc[ss_index]["status"]
                        if state_status:
                            md.append(f"Stage status: *{state_status}*.")

                        times = stage_times_data()
                        overall_df = overall_data()

                        if times.empty or overall_df.empty:
                            md.append(_md_final)
                            return ui.markdown("\n\n".join(md))

                        overall_pos = overall_df.loc[
                            overall_df["carNo"] == times.iloc[0]["carNo"], "pos"
                        ].iloc[0]

                        _md = f"""{times.iloc[0]["driver"]} was in {Nth(1)} position on the stage and {Nth(overall_pos)} overall.
                        """
                        md.append(_md)

                        stagewinners = stage_winners_data()
                        if not stagewinners.empty:
                            winner_row = stagewinners.loc[
                                stagewinners["stageNo"] == wrc.stage_ids[stageId]
                            ]

                            _md = f"""This was his {Nth(winner_row.iloc[0]["daily_wins"])} stage win of the day and his {Nth(winner_row.iloc[0]["wins_overall"])} stage win overall."""

                            md.append(_md)

                        if times.iloc[0]["carNo"] != overall_df.iloc[0]["carNo"]:
                            leader_row = times.loc[
                                times["carNo"] == overall_df.iloc[0]["carNo"]
                            ]
                            leader = leader_row.iloc[0]["driver"]
                        else:
                            leader = ""
                            leader_row = DataFrame()

                        CLOSE_PACE = 0.1  # 0.05
                        on_the_pace = times[times["pace diff (s/km)"] < CLOSE_PACE]
                        leader_handled = False
                        if len(on_the_pace) > 1:
                            _md = "Also on the pace"
                            for _, r in on_the_pace[1:].iterrows():
                                if leader == r["driver"]:
                                    leader_handled = True
                                    leader_text = "rally leader "
                                else:
                                    leader_text = ""
                                _md = (
                                    _md
                                    + f""", {leader_text}{r["driver"]} was just {r["diffFirst"]}s behind ({round(r["pace diff (s/km)"], 2)} s/km off the stage winner)"""
                                )
                            md.append(_md + ".")

                        if (
                            not leader_row.empty and not leader_handled
                        ):  # Check if leader exists in times
                            leaderPos = leader_row.iloc[0]["pos"]
                            leaderDiff = leader_row.iloc[0]["diffPrev"]
                            _md = f"""Rally leader {overall_df.iloc[0]["driver"]} was {leaderDiff} seconds behind in {Nth(leaderPos)} position."""
                            md.append(_md)  # Properly append the string

                        # External rules test
                        _overall_diff = core_stage(
                            wrc,
                            stage_info,
                            stage_code,
                        )
                        remarks = process_rally_overall_rules(_overall_diff)

                        for remark in remarks:
                            md.append(remark[0])

                        md.append(_md_final)
                        return ui.markdown("\n\n".join(md))

                with ui.accordion_panel("Overall position"):

                    @render.data_frame
                    def overall_short():
                        overall_df = overall_data()
                        if overall_df.empty:
                            return
                        retcols = [
                            k
                            for k in overall_df.columns
                            if k
                            in [
                                "pos",
                                "carNo",
                                "driver",
                                "coDriver",
                                "teamName",
                                "stageTime",
                                "diffFirst",
                                "diffPrev",
                                "penaltyTime",
                                "totalTime",
                                "groupClass",
                                "eligibility",
                            ]
                            or k.startswith("round")
                        ]
                        return render.DataGrid(overall_df[retcols])

                with ui.accordion_panel("Stage times"):
                    # Create stage driver rebase selector
                    ui.input_select(
                        "stage_rebase_driver",
                        "Driver rebase:",
                        {},
                    )

                    @render.ui
                    @reactive.event(
                        input.event,
                        input.stage,
                        input.championship,
                        input.stage_rebase_driver,
                    )
                    def stage_times_short_frame():
                        # We are rebasing the data here so we should copy.
                        # TO DO: would it be worth also having a rebased times reactive?
                        stageId = input.stage()
                        stage_times = stage_times_data()
                        if stage_times.empty:
                            return
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
                        if stageId == "SHD":
                            core_cols += [
                                c for c in stage_times.columns if c.startswith("round")
                            ]
                        if "diffFirst" in stage_times.columns:
                            rebase_driver = input.stage_rebase_driver()

                            # Add percentage time column using rebase driver time basis
                            stage_times["percent"] = (
                                100
                                * stage_times["timeInS"]
                                / stage_times.loc[
                                    stage_times["carNo"] == rebase_driver, "timeInS"
                                ].iloc[0]
                            ).round(1)

                            rebase_gap_col = "Rebase Gap (s)"
                            stage_times[rebase_gap_col] = stage_times[
                                "diffFirst"
                            ].apply(time_to_seconds, retzero=True)

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
                                "percent",
                            ]
                            html = (
                                stage_times[
                                    [c for c in cols_order if c in stage_times.columns]
                                ]
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
                                stage_times[
                                    [c for c in core_cols if c in stage_times.columns]
                                ]
                                .style.format(precision=1)
                                .to_html()
                            )
                        return ui.HTML(html)

    with ui.accordion_panel(title="Splits Analysis"):
        with ui.accordion(open=False):

            with ui.accordion_panel("Splits notes"):

                @render.ui
                def splits_text_intro():
                    stageId = input.stage()
                    if stageId == "SHD":
                        return ui.markdown("Shakedown...")

                    (
                        split_times_wide,
                        split_times_long,
                        split_times_wide_numeric,
                    ) = split_times_data()

                    if split_times_wide.empty:
                        return DataFrame()

                    md = ""
                    split_cols = [
                        c
                        for c in split_times_wide_numeric.columns
                        if c.startswith("round")
                    ]

                    splits_symbols = get_splits_symbols(
                        split_times_wide_numeric, split_cols
                    )
                    print(splits_symbols)

            with ui.accordion_panel("Overall split times"):

                with ui.card(class_="mt-3"):
                    with ui.card_header():
                        with ui.tooltip(
                            placement="right", id="splits_times_original_tt"
                        ):
                            ui.span(
                                "WRC split times data ",
                                question_circle_fill,
                            )
                            "Original timing data from WRC live timing API."

                    @render.table
                    @reactive.event(input.stage)
                    def split_times_original():
                        (
                            split_times_wide,
                            split_times_long,
                            split_times_wide_numeric,
                        ) = split_times_data()
                        if split_times_wide.empty:
                            return DataFrame()

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
                        ] + [
                            c for c in split_times_wide.columns if c.startswith("round")
                        ]

                        return split_times_wide[display_cols]

            with ui.accordion_panel("Split times detail"):

                @render.ui
                @reactive.event(input.stage)
                def split_sections_details():
                    split_cumdists, split_dists = split_dists_for_stage()
                    return ui.markdown(f"Split section distances: {split_dists}")

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
                        split_cumdists, split_dists = split_dists_for_stage()
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
                    @reactive.event(input.splits_section_view, input.stage)
                    def split_report():
                        stageId = input.stage()
                        view = input.splits_section_view()
                        if stageId == "SHD":
                            return
                        (
                            split_times_wide,
                            split_times_long,
                            split_times_wide_numeric,
                        ) = split_times_data()
                        split_cumdists, split_dists = split_dists_for_stage()
                        split_cols = [
                            c
                            for c in split_times_wide_numeric.columns
                            if c.startswith("round")
                        ]
                        split_durations = wrc.get_split_duration(
                            split_times_wide_numeric,
                            split_cols,
                        )
                        output_ = scaled_splits(
                            split_times_wide_numeric,
                            split_times_wide,
                            split_dists,
                            split_cols,
                            split_durations,
                            view,
                            carNum2name(),
                        )
                        if not output_.empty:
                            return render.DataGrid(output_)

                    with ui.accordion(open=False):
                        with ui.accordion_panel(
                            "Split section speed/pace distributions"
                        ):

                            @render.plot(
                                alt="Box plot of split section speed/pace distributions."
                            )
                            @reactive.event(input.stage, input.splits_section_view)
                            def plot_split_dists():
                                stageId = input.stage()
                                view = input.splits_section_view()
                                if stageId == "SHD":
                                    return
                                (
                                    split_times_wide,
                                    split_times_long,
                                    split_times_wide_numeric,
                                ) = split_times_data()
                                if split_times_wide_numeric.empty:
                                    return
                                split_cols = [
                                    c
                                    for c in split_times_wide_numeric.columns
                                    if c.startswith("round")
                                ]
                                # We want within split times, not accumulated times
                                output_ = wrc.get_split_duration(
                                    split_times_wide_numeric,
                                    split_cols,
                                )
                                split_cumdists, split_dists = split_dists_for_stage()
                                newcol = "Time in section (s)"
                                if split_dists:
                                    if view == "pace":
                                        output_.update(
                                            output_.loc[:, split_dists.keys()].apply(
                                                lambda s: s / split_dists[s.name]
                                            )
                                        )
                                        newcol = "Pace (s/km)"
                                    elif view == "speed":
                                        output_.update(
                                            output_.loc[:, split_dists.keys()].apply(
                                                lambda s: 3600 * split_dists[s.name] / s
                                            )
                                        )
                                        newcol = "Speed (km/h)"
                                    output_.rename(columns={"timeInS": newcol})

                                output_long = melt(
                                    output_,
                                    id_vars=["carNo"],
                                    value_vars=split_cols,
                                    var_name="roundN",
                                    value_name=newcol,
                                )
                                output_long["roundN"] = output_long[
                                    "roundN"
                                ].str.replace("round", "s")
                                ax = boxplot(data=output_long, x="roundN", y=newcol)
                                ax.set(xlabel=None)
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
                        stages = stages_data()
                        times = stage_times_data()
                        stageId = input.stage()
                        rebase_driver = input.rebase_driver()
                        if (
                            stageId == "SHD"
                            or not rebase_driver
                            or rebase_driver == "NONE"
                        ):
                            return

                        # TO DO: provide ult view if rebase_driver=="ult"

                        if stages.empty or times.empty:
                            return ui.markdown("*No data available.*")

                        stage_name = stages.loc[
                            stages["stageId"] == stageId, "name"
                        ].iloc[0]

                        # pos is zero indexed
                        pos = (
                            int(
                                times.loc[
                                    times["carNo"] == rebase_driver, "pos"
                                ].values[0]
                            )
                            - 1
                        )

                        def _get_hero_text(pos):
                            return ui.markdown(
                                f"""
                        __{times.loc[pos, "driver"]}__  
                        {times.loc[pos, "stageTime"]}  
                        """
                            )

                        def _get_showcase(pos):
                            diffFirst = times.loc[pos, "diffFirst"]
                            diffFirst = "" if pos == 0 else f"__*{diffFirst}s*__"
                            speed = times.loc[pos, "speed (km/h)"]
                            pace = times.loc[pos, "pace diff (s/km)"]
                            pace = (
                                "" if pos == 0 else f"*{round(pace, 2)} s/km off-pace*"
                            )
                            return ui.markdown(
                                f"""
                __P{pos+1}__ {diffFirst}  
                
                {round(speed,1)} km/h  
                {pace}
                """
                            )

                        pr = ui.value_box(
                            title=stage_name,
                            value=_get_hero_text(pos),
                            theme="text-black",
                            showcase=_get_showcase(pos),
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
                                    stageId = input.stage()
                                    rebase_driver = input.rebase_driver()
                                    # print(f"Rebasing on {rebase_driver}")
                                    if (
                                        stageId == "SHD"
                                        or not rebase_driver
                                        or rebase_driver == "NONE"
                                    ):
                                        return
                                    (
                                        split_times_wide,
                                        split_times_long,
                                        split_times_wide_numeric,
                                    ) = split_times_data()
                                    if split_times_wide_numeric.empty:
                                        return
                                    split_times_wide_numeric = (
                                        split_times_wide_numeric.copy()
                                    )

                                    output_, split_cols = _reshape_splits_wide_with_ult(
                                        split_times_wide_numeric, rebase_driver
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
                                    stageId = input.stage()
                                    rebase_driver = input.rebase_driver()
                                    # print(f"Rebasing on {rebase_driver}")
                                    if (
                                        stageId == "SHD"
                                        or not rebase_driver
                                        or rebase_driver == "NONE"
                                    ):
                                        return
                                    (
                                        split_times_wide,
                                        split_times_long,
                                        split_times_wide_numeric,
                                    ) = split_times_data()
                                    if split_times_wide_numeric.empty:
                                        return
                                    split_times_wide_numeric = (
                                        split_times_wide_numeric.copy()
                                    )

                                    output_, split_cols = _reshape_splits_wide_with_ult(
                                        split_times_wide_numeric, rebase_driver
                                    )

                                    colors = (
                                        ["red", "white", "green"]
                                        if input.rebase_reverse_palette()
                                        else ["green", "white", "red"]
                                    )

                                    long_df = melt(
                                        output_,
                                        id_vars=["carNo"],
                                        var_name="roundN",
                                        value_name="time",
                                    )
                                    colors = [
                                        "red" if val >= 0 else "green"
                                        for val in long_df["time"]
                                    ]
                                    if input.splits_section_plot() == "bydriver":
                                        ax = barplot(
                                            long_df,
                                            orient="h",
                                            hue="roundN",
                                            x="time",
                                            y="carNo",
                                            palette=colors,
                                            legend=False,
                                        )
                                    else:
                                        ax = barplot(
                                            long_df,
                                            orient="h",
                                            y="roundN",
                                            x="time",
                                            hue="carNo",
                                            palette=colors,
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
                                    rebase_driver = input.rebase_driver()
                                    if stageId == "SHD":
                                        return
                                    (
                                        split_times_wide,
                                        split_times_long,
                                        split_times_wide_numeric,
                                    ) = split_times_data()
                                    if split_times_long.empty:
                                        return
                                    split_times_long = split_times_long[
                                        ["carNo", "roundN", "timeInS"]
                                    ].copy()

                                    # Add final stage times
                                    times = stage_times_data()
                                    if not times.empty:
                                        times = times[["carNo", "timeInS"]].copy()
                                        times["roundN"] = round(
                                            len(split_times_long["roundN"].unique()) + 1
                                        )

                                        split_times_long = concat(
                                            [split_times_long, times], ignore_index=True
                                        )

                                    # TO DO - need a function to rebase a long df by group
                                    ll2 = split_times_long.pivot(
                                        index="carNo",
                                        columns="roundN",
                                        values="timeInS",
                                    ).reset_index()

                                    cols = [
                                        c for c in ll2.columns if c.startswith("round")
                                    ]
                                    lw = wrc.rebaseManyTimes(
                                        ll2, rebase_driver, "carNo", cols
                                    )
                                    lw["round0"] = 0.0
                                    lw = lw[["carNo", "round0"] + cols]
                                    ll3 = melt(
                                        lw,
                                        id_vars=["carNo"],
                                        value_vars=["round0"] + cols,
                                        var_name="roundN",
                                        value_name="timeInS",
                                    )
                                    ll3["round"] = (
                                        ll3["roundN"]
                                        .str.replace("round", "")
                                        .astype(int)
                                    )
                                    split_cumdists, split_dists = (
                                        split_dists_for_stage()
                                    )
                                    plt.subplot()
                                    ll3["carNo"] = ll3["carNo"].map(carNum2name())
                                    if split_cumdists:
                                        split_cumdists["round0"] = 0.0
                                        ll3["dist"] = ll3["roundN"].map(split_cumdists)
                                        g = lineplot(
                                            data=ll3, x="dist", y="timeInS", hue="carNo"
                                        )

                                    else:
                                        g = lineplot(
                                            data=ll3,
                                            x="round",
                                            y="timeInS",
                                            hue="carNo",
                                        )
                                    if rebase_driver and rebase_driver != "NONE":
                                        g.set_ylim(g.get_ylim()[::-1])

                                    texts = []
                                    for line, label in zip(
                                        g.get_lines(), ll3["carNo"].unique()
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
                                        ll3["dist"].min(), ll3["dist"].max() * 1.15
                                    )

                                    g.legend_.remove()
                                    return g


## Utility functions


def _reshape_splits_wide_with_ult(split_times_wide_numeric, rebase_driver):
    split_cols = [c for c in split_times_wide_numeric.columns if c.startswith("round")]
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
        min_val = to_numeric(output_[col][output_[col] > 0], errors="coerce").min()
        ult_row[col] = min_val

    output_ = concat([output_, DataFrame([ult_row])], ignore_index=True)
    output_ = wrc.rebaseManyTimes(output_, rebase_driver, "carNo", split_cols)
    output_ = output_[output_["carNo"] != "ult"]
    output_["carNo"] = output_["carNo"].map(carNum2name())
    return output_, split_cols


## Reactive calculations


@reactive.calc
@reactive.event(input.event)
def rally_id_var():
    rallyId = input.event()
    if rallyId:
        wrc.eventId = wrc.rallyId2eventId[rallyId]
        wrc.rallyId = rallyId
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


@reactive.calc
@reactive.event(input.event, input.stage, input.championship)
def overall_data():
    return wrc.getOverall(update=True)


@reactive.calc
@reactive.event(input.event, input.championship, input.stage)
def stage_winners_data():
    stagewinners = wrc.getStageWinners(update=True)
    stages = stages_data()
    stagewinners = enrich_stage_winners(stagewinners, stages)

    return stagewinners


# TO DO - we should have a single reactive value for wrc.rallyId
@reactive.calc
@reactive.event(input.event)
def stages_data():
    wrc.rallyId = rally_id_var()
    wrc.eventId = wrc.rallyId2eventId[wrc.rallyId]
    # WRC API data fetch
    stages = wrc.getStageDetails(update=True)
    if "day" in stages:
        stages["stageInDay"] = stages.groupby(["day"]).cumcount() + 1
    return stages


@reactive.calc
def itinerary_data():
    wrc.rallyId = rally_id_var()
    wrc.eventId = wrc.rallyId2eventId[wrc.rallyId]
    # WRC API data fetch
    itinerary = wrc.getItinerary(update=True)
    # Get latest itinerary
    itinerary.drop_duplicates(subset=["stage"], keep="last", inplace=True)
    itinerary.reset_index(drop=True, inplace=True)
    return itinerary


@reactive.calc
def startlist_data():
    wrc.rallyId = rally_id_var()

    # WRC API data fetch
    startlist = wrc.getStartlist(update=True)
    return startlist


@reactive.calc
@reactive.event(input.event, input.stage)
def stage_times_data():
    stageId = input.stage()
    wrc.stageId = stageId
    # WRC API data fetch
    stage_times = wrc.getStageTimes()
    return stage_times


@reactive.calc
@reactive.event(input.stage, input.event, input.championship)
def split_times_data():
    stageId = input.stage()
    wrc.stageId = stageId
    # WRC API data fetch
    # TO DO remove try - errors should be caught elsewhere?
    try:
        split_times_wide = wrc.getSplitTimes()
        split_times_long = wrc.getSplitsLong(split_times_wide)
        split_times_wide_numeric = wrc.get_splits_as_numeric(
            split_times_wide, regularise=stageId != "SHD"
        )
        return split_times_wide, split_times_long, split_times_wide_numeric
    except:
        return (DataFrame(), DataFrame(), DataFrame())


@reactive.effect
def update_events_select():
    season = season_data()
    if season_data().empty:
        return
    # events = season["EventName"].to_list()
    events = (
        season[["rallyId", "rallyTitle"]].set_index("rallyId")["rallyTitle"].to_dict()
    )
    wrc.setEvent()
    ui.update_select("event", choices=events, selected=wrc.rallyId)


@reactive.effect
@reactive.event(input.stage)
def update_startlist_select():
    startlist = wrc.getStartlist()
    if startlist.empty:
        return
    # Need to select today
    startLists = startlist["startList"].unique().tolist()
    if startLists:
        # TO DO - this is a hack ?
        day_today = datetime.today().strftime("%A")
        if len(startLists) == 1 or day_today not in startLists:
            startwith = startLists[0]
        else:
            startwith = day_today
        ui.update_select("startlist", choices=startLists, selected=startwith)


@reactive.effect
@reactive.event(input.event, input.championship)
def update_stages_select():
    stages_df = stages_data()
    if stages_df.empty:
        ui.update_select("stage", choices={})
    else:
        stages = (
            stages_df[["stageNo", "stageId"]].set_index("stageId")["stageNo"].to_dict()
        )
        ui.update_select("stage", choices=stages)


@reactive.effect
@reactive.event(input.event, input.stage)
def update_stages_driver_rebase_select():
    if stage_times_data().empty:
        return
    rebase_drivers = (
        stage_times_data()[["carNo", "driver"]].set_index("carNo")["driver"].to_dict()
    )
    ui.update_select("stage_rebase_driver", choices=rebase_drivers)


@reactive.effect
@reactive.event(input.championship, input.event, input.stage)
def update_driver_rebase_select():
    stageId = input.stage()
    if stageId == "SHD" or stage_times_data().empty:
        return
    # rebase_drivers = {"NONE": ""}
    rebase_drivers = {}
    rebase_drivers.update(
        stage_times_data()[["carNo", "driver"]].set_index("carNo")["driver"].to_dict()
    )

    rebase_drivers["ult"] = "ULTIMATE"
    ui.update_select("rebase_driver", choices=rebase_drivers)


# @reactive.effect
# @reactive.event(input.event, input.stage)
# def update_splits_driver_rebase_select():
#     if stage_times_data().empty:
#         rebase_drivers = {}
#     else:
#         rebase_drivers = (
#             stage_times_data()[["carNo", "driver"]]
#             .set_index("carNo")["driver"]
#             .to_dict()
#         )
#         rebase_drivers["ult"] = "ULTIMATE"
#     ui.update_select("splits_rebase_driver", choices=rebase_drivers)


@reactive.calc
@reactive.event(input.event)
def getSplitDists():
    year = str(input.season())
    rallyId = str(rally_id_var())
    with open(Path(__file__).parent / "patches.json", "r") as file:
        patches_json = json.load(file)
        try:
            splits = patches_json["split_distances"][year][rallyId]
            splits = DataFrame.from_dict(splits, orient="index")
            splits.columns = [f"round{i}" for i in range(1, splits.shape[1] + 1)]
        except:
            splits = DataFrame()
    return splits


@reactive.calc
@reactive.event(input.event, input.stage)
def split_dists_for_stage():
    stageId = input.stage()
    split_dists = getSplitDists()
    try:
        split_cumdists = split_dists.loc[wrc.stage_ids[stageId]].dropna().to_dict()
        split_cumdists = {k: split_cumdists[k] for k in sorted(split_cumdists)}

        # Extract values in the sorted order of keys
        split_dists = {}
        prev = 0

        for k, v in split_cumdists.items():
            split_dists[k] = round(v - prev, 1)
            prev = v  # Update the previous value for the next iteration

    except:
        split_cumdists = {}
        split_dists = {}

    return split_cumdists, split_dists


def get_overall_result_hero(stageId, stages_data, overall_data):
    stages = stages_data()
    overall_df = overall_data if isinstance(overall_data, DataFrame) else overall_data()
    if stages.empty:
        return
    
    stage_name = stages.loc[stages["stageId"] == stageId, "name"].iloc[0]

    def _get_hero_text(pos):
        return ui.markdown(
            f"""
            __{overall_df.loc[pos, "driver"]}__  
            {overall_df.loc[pos, "stageTime"]}  
            """
        )

    # Positions are zero-indexed
    ##averaging = round(overall_df.loc[0, "speed (km/h)"], 1)
    # TO DO - if this is final result, we can use overall dist for speed
    # averaging = f"Averaging  \n  \n{averaging} km/h" if averaging else ""
    p1 = ui.value_box(
        title=stage_name,
        value=_get_hero_text(0),
        theme="text-green",
        # showcase=averaging,
        # showcase_layout="left center",
        full_screen=True,
    )

    if len(overall_df) > 1:
        # p2pace = round(times.loc[1, "pace diff (s/km)"], 2)
        # p2pace = p2pace if p2pace else ""
        # p2pace = f'(Pace: {p2pace} s/km slower)'
        p2 = ui.value_box(
            value=overall_df.loc[1, "diffFirst"],
            title=_get_hero_text(1),
            theme="text-blue",
            # showcase=p2pace,
            # showcase_layout="bottom",
            full_screen=True,
        )
        if len(overall_df) > 2:
            # p3pace = round(overall_df.loc[2, "pace diff (s/km)"], 2)
            # p3pace = p3pace if p3pace else ""
            # p3pace = f"(Pace: {p3pace} s/km slower)"
            p3 = ui.value_box(
                value=overall_df.loc[2, "diffFirst"],
                title=_get_hero_text(2),
                theme="text-purple",
                # showcase=p3pace,
                # showcase_layout="bottom",
                full_screen=True,
            )
            return ui.TagList(p1, uis.layout_columns(p2, p3))
        return ui.TagList(p1, p2)

    return p1


def get_stage_result_hero(stageId, stages_data, stage_times_data):
    stages = stages_data()

    if stageId == "SHD":
        return None

    times = (
        stage_times_data
        if isinstance(stage_times_data, DataFrame)
        else stage_times_data()
    )
    if times.empty:
        print(f"No stage times in stage_times_data() for {stageId}")
        return None

    stage_name = stages.loc[stages["stageId"] == stageId, "name"].iloc[0]

    def _get_hero_text(pos):
        return ui.markdown(
            f"""
            __{times.loc[pos, "driver"]}__  
            {times.loc[pos, "stageTime"]}  
            """
        )

    # Positions are zero-indexed
    averaging = round(times.loc[0, "speed (km/h)"], 1)
    # TO DO - if this is final result, we can use overall dist for speed
    averaging = f"Averaging  \n  \n{averaging} km/h" if averaging else ""
    p1 = ui.value_box(
        title=stage_name,
        value=_get_hero_text(0),
        theme="text-green",
        showcase=averaging,
        showcase_layout="left center",
        full_screen=True,
    )

    if len(times) > 1:
        p2pace = round(times.loc[1, "pace diff (s/km)"], 2)
        p2pace = p2pace if p2pace else ""
        p2pace = f"(Pace: {p2pace} s/km slower)"
        p2 = ui.value_box(
            value=times.loc[1, "diffFirst"],
            title=_get_hero_text(1),
            theme="text-blue",
            showcase=p2pace,
            showcase_layout="bottom",
            full_screen=True,
        )
        if len(times) > 2:
            p3pace = round(times.loc[2, "pace diff (s/km)"], 2)
            p3pace = p3pace if p3pace else ""
            p3pace = f"(Pace: {p3pace} s/km slower)"
            p3 = ui.value_box(
                value=times.loc[2, "diffFirst"],
                title=_get_hero_text(2),
                theme="text-purple",
                showcase=p3pace,
                showcase_layout="bottom",
                full_screen=True,
            )
            return ui.TagList(p1, uis.layout_columns(p2, p3))
        return ui.TagList(p1, p2)

    return p1
