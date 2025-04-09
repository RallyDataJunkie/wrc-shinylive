from shiny.express import ui
from shiny import ui as uis

from wrc_rallydj.utils import format_timedelta

def get_rebased_driver_hero(stageId, rebase_driver, stages, times):
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
                __#{record["carNo"]} {record["driverName"]}__  
                {format_timedelta(record["stageTimeMs"])}  
                """
            )

    # Positions are zero-indexed
    ##averaging = round(overall_df.loc[0, "speed (km/h)"], 1)
    # TO DO - if this is final result, we can use overall dist for speed
    # averaging = f"Averaging  \n  \n{averaging} km/h" if averaging else ""
    p1_ = overall_data[overall_data["position"] == 1]
    p1 = ui.value_box(
        title=f"{stage_code}: {stage_name}",
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
            value="+" + format_timedelta(p2_.iloc[0]["diffFirstMs"]),
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
                value=format_timedelta(p3_.iloc[0]["diffFirstMs"], addplus=True),
                title=_get_hero_text(p3_),
                theme="text-purple",
                # showcase=p3pace,
                # showcase_layout="bottom",
                full_screen=True,
            )
            return ui.TagList(p1, uis.layout_columns(p2, p3))
        return ui.TagList(p1, p2)

    return p1


def get_stage_result_hero(stageId, stages_data, stage_times_data):

    if stage_times_data.empty:
        print(f"No stage times in stage_times_data() for {stageId}")
        return None

    stage_ = stages_data[stages_data["stageId"] == stageId].iloc[0]
    stage_name = stage_["name"]
    stage_code = stage_["code"]

    def _get_hero_text(p):
        return ui.markdown(
            f"""
            __#{p["carNo"]} {p["driverName"]}__  
            {format_timedelta(p["timeInS"], units="s")}  
            """
        )

    stage_times_data_p1 = stage_times_data[stage_times_data["position"] == 1]
    if stage_times_data_p1.empty:
        return
    p1_ = stage_times_data_p1.iloc[0]

    # Positions are zero-indexed
    averaging = p1_["speed (km/h)"]
    pace = p1_["pace (s/km)"]
    # TO DO - if this is final result, we can use overall dist for speed
    averaging = f"Averaging  \n  \n{averaging} km/h  \n{pace} s/km" if averaging else ""
    p1 = ui.value_box(
        title=f"{stage_code}: {stage_name}",
        value=_get_hero_text(p1_),
        theme="text-green",
        showcase=averaging,
        showcase_layout="left center",
        full_screen=True,
    )

    if len(stage_times_data) > 1:
        p2_ = stage_times_data[stage_times_data["position"] == 2].iloc[0]
        p2speed = p2_["speed (km/h)"]
        p2pace = p2_["pace diff (s/km)"]
        p2speedpace = f"({p2speed} km/h, {p2pace} s/km off the pace)" if p2speed else ""
        p2 = ui.value_box(
            value=format_timedelta(p2_["diffFirstMs"], addplus=True),
            title=_get_hero_text(p2_),
            theme="text-blue",
            showcase=p2speedpace,
            showcase_layout="bottom",
            full_screen=True,
        )
        if len(stage_times_data) > 2:
            p3_ = stage_times_data[stage_times_data["position"] == 3].iloc[0]
            p3speed = p3_["speed (km/h)"]
            p3pace = p3_["pace diff (s/km)"]
            p3speedpace = (
                f"({p3speed} km/h, {p3pace} s/km off the pace)" if p3speed else ""
            )
            p3 = ui.value_box(
                value=format_timedelta(p3_["diffFirstMs"], addplus=True),
                title=_get_hero_text(p3_),
                theme="text-purple",
                showcase=p3speedpace,
                showcase_layout="bottom",
                full_screen=True,
            )
            return ui.TagList(p1, uis.layout_columns(p2, p3))
        return ui.TagList(p1, p2)

    return p1
