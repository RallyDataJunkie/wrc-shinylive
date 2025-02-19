import inflect
from pandas import merge, DataFrame

p = inflect.engine()

import random

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
    stage_details, stageNo,
):
    retcols_stage = ["carNo", "driver", "pos", "Gap", "Diff"]
    retcols_overall = [
        "carNo",
        "driver",
        "pos",
        "overallGap",
        "overallDiff",
        "timeToCarBehind",
    ]

    prev_idx = stage_details.loc[stage_details["stageNo"] == stageNo].index[0]
    prevStageNo = (
        stage_details.loc[prev_idx - 1, "stageNo"]
        if stageNo != "SS1" and stageNo != "SHD"
        else ""
    )
    # print(prevStageNo, stageNo)
    _df_stage_curr = wrc.getStageTimes(stageId=stageNo)[retcols_stage].copy()

    _df_overall_curr = wrc.getOverall(stageId=stageNo, update=True)[
        retcols_overall
    ].copy()
    _df_overall_curr.rename(columns={"pos": "overallPos"}, inplace=True)
    _df_overall_prev = wrc.getOverall(stageId=prevStageNo, update=True)[
        retcols_overall
    ].copy()
    _df_overall_prev.rename(columns={"pos": "overallPos"}, inplace=True)
    # The subtraction is this way to handle signs better
    # display(_df_overall_prev)
    # display(_df_overall_curr)
    _df_overall_diff = (
        _df_overall_prev.set_index(["carNo", "driver"])
        - _df_overall_curr.set_index(["carNo", "driver"])
    ).reset_index()
    _df_overall_diff.rename(
        columns={
            "overallPos": "overallPosDelta",
            "overallGap": "overallGapDelta",
            "overallDiff": "overallDiffDelta",
            "timeToCarBehind": "timeToCarBehindDelta",
        },
        inplace=True,
    )
    _df_overall_curr["currPodium"] = _df_overall_curr["overallPos"] <= 3
    _df_overall = merge(_df_overall_diff, _df_overall_curr, on=["carNo", "driver"])

    _df_overall_prev["prevLeader"] = _df_overall_prev["overallPos"] == 1
    _df_overall_prev["prevOverallPos"] = _df_overall_prev["overallPos"]
    _df_overall_prev["prevPodium"] = _df_overall_prev["overallPos"] <= 3
    _df_overall = merge(
        _df_overall,
        _df_overall_prev[
            ["carNo", "driver", "prevLeader", "prevOverallPos", "prevPodium"]
        ],
        on=["carNo", "driver"],
    )

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
        on=["carNo", "driver"],
    )

    return _df_overall


##---


def rule_onto_podium(row):
    remark = ""
    if row["onto_podium"]:
        remark = f"""{row["driver"]} moved into a podium position"""
    return (remark, 0.9)


def rule_into_first(row):
    remark = ""
    if row["newLeader"]:
        big_jump = (
            f"""jumped { p.number_to_words(row["overallPosDelta"])} places"""
            if row["overallPosDelta"] > 1
            else "moved"
        )
        remark = f"""Taking {Nth(row["pos"])} place on stage, {row["driver"]} {big_jump} into first place overall, taking a lead of {round(row["timeToCarBehind"], 1)}s"""
    return (remark, 1.0)


def rule_lost_first(row):
    remark = ""
    if row["prevLeader"] and not row["currLeader"]:
        remark = f"""Coming in at {Nth(row["pos"])} on the stage, {row["Gap"]}s behind the stage winner, {row["driver"]} lost the overall lead, falling back to {Nth(row["overallPos"])} place"""
        if row["overallPos"] >= 2:
            fell_back = f"""{remark}, {row["overallGap"]}s behind the new leader."""
        else:
            remark = f"{remark}."

    return (remark, 0.9)


# TO DO - need a natural time for timeInS


def rule_leader_increased_lead(row):
    remark = ""
    if row["overallGapDelta"] > 0 and row["currLeader"] and not row["newLeader"]:
        remark = f"""{row["driver"]} increased his the lead at the front of the rally, moving a furher {row["overallGapDelta"]}s ahead, to {row["overallGapDelta"]}s."""

    return (remark, 0.7)


def rule_leader_decreased_lead(row):
    remark = ""
    if row["overallGapDelta"] < 0 and row["currLeader"] and not row["newLeader"]:
        remark = f"""At the front, {row["driver"]}'s lead was reduced by {row["overallGapDelta"]}s to {row["overallGapDelta"]}s."""

    return (remark, 0.7)


def rule_onto_podium(row):
    if row["onto_podium"]:
        remark = ""


def process_rally_overall_rules(df):
    # Apply each rule to create new columns
    remarks_df = DataFrame(
        {
            "podium_remarks": df.apply(rule_onto_podium, axis=1),
            "into_first_remarks": df.apply(rule_into_first, axis=1),
            "lost_first_remarks": df.apply(rule_lost_first, axis=1),
            "lead_increased_remarks": df.apply(rule_leader_increased_lead, axis=1),
            "lead_reduced_remarks": df.apply(rule_leader_decreased_lead, axis=1),
            "move_into_podium_remarks": df.apply(rule_onto_podium, axis=1),
        }
    )

    # Stack all remarks into a single series and filter out empty strings
    filtered_remarks = [
        (remark[0].replace("  ", " "), remark[1])
        for remark in remarks_df.stack()
        if remark[0] != ""
    ]

    return filtered_remarks
