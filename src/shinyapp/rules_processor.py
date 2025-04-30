from inflect import engine
from pandas import merge, DataFrame

p = engine()

import random


def numToWords(n):
    return p.number_to_words(n)


def andList(l):
    if len(l) > 1:
        output_ = ", ".join(map(str, l[:-1])) + " and " + str(l[-1])
    else:
        output_ = str(l[0])
    return output_


def nth(n):
    return p.ordinal(n)


def Nth(n):
    return p.number_to_words(p.ordinal(n))


# Utility rule functions
def sometimes(t, p=0.5, prefix=""):
    """Sometimes return a string passed to the function."""
    if random.random() >= p:
        return f"{prefix}{t}"
    return ""


def occasionally(t):
    """Sometimes return a string passed to the function."""
    return sometimes(t, p=0.2)


def rarely(t):
    """Rarely return a string passed to the function."""
    return sometimes(t, p=0.05)


def pickone_equally(l, prefix="", suffix=""):
    """Return an item from a list,
    selected at random with equal probability."""
    t = random.choice(l)
    if t:
        return f"{prefix}{t}{suffix}"
    return suffix


def pickfirst_prob(l, p=0.5):
    """Select the first item in a list with the specified probability,
    else select an item, with equal probability, from the rest of the list."""
    if len(l) > 1 and random.random() >= p:
        return random.choice(l[1:])
    return l[0]


# ---


def core_stage(
    wrc,
    stage_details,
    stageId,
):
    retcols_stage = ["carNo", "driverName", "position", "Gap", "Diff", "Chase"]
    retcols_overall = [
        "carNo",
        "driverName",
        "position",
        "Gap",
        "Diff",
        "Chase",
    ]
    stage_details.reset_index(drop=True, inplace=True)

    curr_idx = stage_details.loc[stage_details["stageId"] == stageId].index[0]
    # Stages are ordered; if we are index 0 stage is first stage, SS1
    prevStageId = None if not curr_idx else stage_details.loc[curr_idx - 1, "stageId"]
    # print(prevStageNo, stageNo)
    _df_stage_curr = wrc.getStageTimes(stageId=stageId, raw=False)
    print("stagecols", _df_stage_curr.columns, retcols_stage)
    _df_stage_curr = _df_stage_curr[retcols_stage].copy()

    _df_overall_curr = wrc.getStageOverallResults(stageId=stageId, raw=False)[
        retcols_overall
    ].copy()
    _df_overall_curr.rename(columns={"position": "overallPos"}, inplace=True)
    if prevStageId:
        _df_overall_prev = wrc.getStageOverallResults(stageId=prevStageId, raw=False)[
            retcols_overall
        ].copy()
        _df_overall_prev.rename(columns={"position": "overallPos"}, inplace=True)
    else:
        _df_overall_prev = DataFrame()

    # The following only apply when we are in at least the second stage
    if not _df_overall_prev.empty:
        # The subtraction is this way to handle signs better
        # display(_df_overall_prev)
        # display(_df_overall_curr)
        # _df_overall_diff gives deltas between curr and prev
        _df_overall_diff = (
            _df_overall_prev.set_index(["carNo", "driverName"])
            - _df_overall_curr.set_index(["carNo", "driverName"])
        ).reset_index()

        _df_overall_diff.rename(
            columns={
                "overallPos": "overallPosDelta",
                "Gap": "overallGapDelta",
                "Diff": "overallDiffDelta",
                "Chase": "overallChaseDelta",
            },
            inplace=True,
        )

        _df_overall_curr["currPodium"] = _df_overall_curr["overallPos"] <= 3
        _df_overall = merge(
            _df_overall_diff, _df_overall_curr, on=["carNo", "driverName"]
        )
        _df_overall.rename(
            columns={
                "Gap": "overallGap",
                "Diff": "overallDiff",
                "Chase": "overallChase",
            },
            inplace=True,
        )

        _df_overall_prev["prevLeader"] = _df_overall_prev["overallPos"] == 1
        _df_overall_prev["prevLeaderName"] = _df_overall_prev[_df_overall_prev["overallPos"]==1]["driverName"].iloc[0]
        _df_overall_prev["prevOverallPos"] = _df_overall_prev["overallPos"]
        _df_overall_prev["prevPodium"] = _df_overall_prev["overallPos"] <= 3
        _df_overall = merge(
            _df_overall,
            _df_overall_prev[
                [
                    "carNo",
                    "driverName",
                    "prevLeader",
                    "prevOverallPos",
                    "prevPodium",
                    "prevLeaderName",
                ]
            ],
            on=["carNo", "driverName"],
        )
        _df_overall["overallPosDelta"] = _df_overall["overallPosDelta"].astype("int64")
        _df_overall["overallPosChange"] = _df_overall["overallPosDelta"] != 0
        _df_overall["currLeader"] = _df_overall["overallPos"] == 1
        _df_overall["onto_podium"] = (~_df_overall["prevPodium"]) & _df_overall[
            "currPodium"
        ]
        _df_overall["lost_podium"] = (_df_overall["prevPodium"]) & ~_df_overall[
            "currPodium"
        ]
        _df_overall["newLeader"] = (
            _df_overall["overallPosChange"] & _df_overall["currLeader"]
        )

        _df_overall["lead_changed"] = _df_overall["newLeader"].any()

        _df_overall = merge(
            _df_overall,
            _df_stage_curr,
            on=["carNo", "driverName"],
        )

        return _df_overall

    return _df_overall_curr


##---


def rule_onto_podium(row):
    remark = ""
    if row.get("onto_podium"):
        remark = f"""{row["driverName"]} moved into a podium position"""
    return (remark, 0.9)


def rule_into_first(row):
    remark = ""
    if row.get("newLeader"):
        big_jump = (
            f"""jumped { p.number_to_words(row["overallPosDelta"])} places"""
            if row["overallPosDelta"] > 1
            else f"""moved ahead of {row["prevLeaderName"]}"""
        )
        remark = f"""With his {Nth(row["position"])} place on stage, {row["driverName"]} {big_jump} into first place overall, taking a lead of {round(row["overallChase"], 1)}s"""
    return (remark, 1.0)


def rule_lost_first(row):
    remark = ""
    if row.get("prevLeader") and not row.get("currLeader"):
        remark = f"""Coming in at {Nth(row["position"])} on the stage, {row["Gap"]}s behind the stage winner, {row["driverName"]} lost the overall lead, falling back to {Nth(row["overallPos"])} place"""
        if row["overallPos"] >= 2:
            fell_back = f"""{remark}, {row["overallGap"]}s behind the new leader."""
        else:
            remark = f"{remark}."

    return (remark, 0.9)

def rule_move_into_second(row):
    remark = ""
    if row.get("overallPos") == 2 and row.get("overallPosChange"):
        remark = f"""{row["driverName"]} gained {numToWords(row["overallPosDelta"])} {p.plural("place", row.get("overallPosDelta"))}, moving into second overall, {row.get("overallGap")}s behind the leader. """
    return (remark, 0.8)

def rule_drop_from_second(row):
    remark = ""
    if row.get("prevOverallPos")==2 and row.get("overallPosDelta", 0) <0:
        remark = f"""{row["driverName"]} moved down {numToWords(-row.get("overallPosDelta"))} {p.plural("position", -row.get("overallPosDelta"))} to {numToWords(p.ordinal(row.get("overallPos")))}, {row.get("overallGap")}s off the lead, and {row.get("overallDiff")}s off {numToWords(p.ordinal(row.get("overallPos")-1))}."""
    return (remark, 0.79)

def rule_up_into_third(row):
    remark = ""
    if (
        row.get("overallPos") == 3
        and row.get("prevOverallPos", 3) < 3
        and row.get("overallPosDelta", 0)>0
    ):
        remark = f"""{row["driverName"]} moved into third overall, up {numToWords(row.get("overallPosDelta"))} {p.plural("place", row.get("overallPosDelta"))}, {row.get("overallGap")}s behind second and {row.get("overallDiff")}s off the lead."""
    return (remark, 0.73)

# TO DO - need a natural time for timeInS


def rule_leader_retained_lead(row):
    remark = ""
    if (
        row.get("currLeader")
        and not row.get("newLeader")
    ):
        if row.get("overallChaseDelta", 1) == 0:
            delta_change_= "keeping the gap at"
        else:
            if row.get("overallChaseDelta", 1) < 0:
                delta_change_ = "*__increasing__ the gap*"
            elif row.get("overallChaseDelta", 1) < 0:
                delta_change_ = "*__decreasing__ the gap*"
            delta_change_ = f"""{delta_change_} by {-row["overallChaseDelta"]}s to"""

        remark = f"""Overall, {row["driverName"]} __retained the lead__, {delta_change_} {row["overallChase"]}s."""

    return (remark, 0.6)


def rule_onto_podium(row):
    if row.get("onto_podium"):
        remark = ""


def process_rally_overall_rules(df):
    # Apply each rule to create new columns
    remarks_df = DataFrame(
        {
            "podium_remarks": df.apply(rule_onto_podium, axis=1),
            "into_first_remarks": df.apply(rule_into_first, axis=1),
            "lost_first_remarks": df.apply(rule_lost_first, axis=1),
            "retained_lead_remarks": df.apply(rule_leader_retained_lead, axis=1),
            "move_into_podium_remarks": df.apply(rule_onto_podium, axis=1),
            "move_into_second": df.apply(rule_move_into_second, axis=1),
            "drop_from_second": df.apply(rule_drop_from_second, axis=1),
            "move_up_into_third": df.apply(rule_up_into_third, axis=1),
        }
    )

    # Stack all remarks into a single series and filter out empty strings
    filtered_remarks = [
        (remark[0].replace("  ", " "), remark[1])
        for remark in remarks_df.stack()
        if remark[0] != ""
    ]
    filtered_remarks = [f for f in filtered_remarks if f]
    return filtered_remarks
