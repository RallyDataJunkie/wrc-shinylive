from parse import parse
from datetime import timedelta

# import pandas as pd
from pandas import to_datetime, date_range, DataFrame, concat, merge
import datetime
from numpy import nan


def format_timedelta(t, units="ms", addplus=False):
    """
    Convert seconds to a precise hh:mm:ss.tenth format string.

    Args:
    milliseconds (float): Total number of milliseconds

    Returns:
    str: Formatted time string in hh:mm:ss.tenth format
    """
    # Create timedelta
    t = round(t/1000, 1) if units.lower() in ["ms", "milliseconds"] else t

    td = timedelta(seconds=t)

    # Extract total seconds with high precision
    total_seconds = td.total_seconds()

    # Calculate hours, minutes, seconds, and tenths
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    # Split seconds and tenths
    whole_seconds = int(seconds)
    tenths = int((seconds - whole_seconds) * 10)

    addplus = "+" if addplus else ""

    # Conditional formatting based on hours and minutes
    if hours:
        return f"{addplus}{int(hours):02d}:{int(minutes):02d}:{whole_seconds:02d}.{tenths:01d}"
    elif minutes:
        return f"{addplus}{int(minutes):01d}:{whole_seconds:02d}.{tenths:01d}"
    else:
        return f"{addplus}{whole_seconds:01d}.{tenths:01d}"


# TO DO - refactor this into the WRCLiveTimingAPIClient class
def enrich_stage_winners(stagewinners, inplace=True):
    if "wins_overall" in stagewinners:
        # Already enriched
        return stagewinners

    if not inplace:
        stagewinners = stagewinners.copy()

    stagewinners["timeInS"] = stagewinners["elapsedDuration"].apply(
        time_to_seconds, retzero=True
    )

    stagewinners["wins_overall"] = stagewinners.groupby("carNo").cumcount() + 1

    stagewinners["section_wins"] = (
        stagewinners.groupby(["sectionName", "carNo"]).cumcount() + 1
    )

    stagewinners["daily_wins"] = stagewinners.groupby(["day", "carNo"]).cumcount() + 1

    stagewinners["speed (km/h)"] = round(
        stagewinners["distance"] / (stagewinners["timeInS"] / 3600), 2
    )
    stagewinners["pace (s/km)"] = round(
        stagewinners["timeInS"] / stagewinners["distance"], 2
    )

    return stagewinners


# TO DO - refactor this into the WRCLiveTimingAPIClient class
# or create an enrichment class
def scaled_splits(
    split_times_wide_numeric,
    split_times_wide,
    split_dists,
    split_cols,
    split_durations,
    view,
    carNum2Names,
):
    if split_times_wide_numeric.empty:
        return
    split_times_wide_numeric = split_times_wide_numeric.copy()

    if view in ["time_acc", "pos_acc"]:
        split_times_wide_numeric = merge(
            split_times_wide[["carNo", "teamName", "roadPos"]],
            split_times_wide_numeric,
            on="carNo",
        )
        split_times_wide_numeric["carNo"] = split_times_wide_numeric["carNo"].map(
            carNum2Names
        )
        # TO DO  precision number format formatting
        # styles = {c: "{0:0.1f}" for c in split_cols}
        # return split_times_wide_numeric.style.format(styles)
        split_times_wide_numeric.loc[:, split_cols] = split_times_wide_numeric[
            split_cols
        ].round(1)

        if view == "pos_acc":
            split_times_wide_numeric.loc[:, split_cols] = split_times_wide_numeric[
                split_cols
            ].rank(method="min", na_option="keep")

        split_times_wide_numeric.columns = (
            ["Driver", "TeamName", "RoadPos"]
            + [f"Split {i}" for i in range(1, len(split_cols))]
            + ["Finish"]
        )
        return split_times_wide_numeric

    # We want within split times, not accumulated times
    # Scope the view if data available
    output_ = split_durations.copy()
    if split_dists:
        if view == "pos_within":
            output_.loc[:, split_cols] = output_[split_cols].rank(
                method="min", na_option="keep"
            )
        elif view == "pace":
            output_.update(
                output_.loc[:, split_dists.keys()].apply(
                    lambda s: s / split_dists[s.name]
                )
            )
        elif view == "speed":
            output_.update(
                output_.loc[:, split_dists.keys()].apply(
                    lambda s: 3600 * split_dists[s.name] / s
                )
            )

    # styles = {c: "{0:0.1f}" for c in split_cols}

    if not view.startswith("pos_"):
        output_.loc[:, split_cols] = output_[split_cols].round(1)

    output_ = merge(
        split_times_wide[["carNo", "teamName", "roadPos"]],
        output_,
        on="carNo",
    )
    output_["carNo"] = output_["carNo"].map(carNum2Names)

    output_.columns = (
        ["Driver", "TeamName", "RoadPos"]
        + [f"Split {i}" for i in range(1, len(split_cols))]
        + ["Finish"]
    )
    return output_


def convert_date_range(date_range_str):
    """Convert date of from `19 - 22 JAN 2023` to date range."""
    r = parse("{start_day} - {end_day} {month} {year}", date_range_str)
    start_date = to_datetime(
        f"{r['start_day']} {r['month']} {r['year']}", format="%d %b %Y"
    )
    end_date = to_datetime(
        f"{r['end_day']} {r['month']} {r['year']}", format="%d %b %Y"
    )
    return date_range(start=start_date, end=end_date)


def timeify(df, col, typ=None):
    """Convert a column  to a datetime inplace."""
    if typ == "daterange":
        df[col] = df[col].apply(convert_date_range)
    else:
        df[col] = to_datetime(df[col].astype(int), unit="ms")


def tablify(json_data, subcolkey=None, addcols=None):
    """Generate table from separate colnames/values JSON."""
    # Note that the JSON may be a few rows short cf. provided keys
    if "fields" not in json_data:
        return DataFrame()
    fields = json_data["fields"]
    if subcolkey is None:
        values = json_data["values"]
        # Create a DataFrame
        df = DataFrame(columns=fields)
        _values = []
        _nvals = len(fields)
        for value in values:
            _nval = len(value)
            if _nval < _nvals:
                value += [""] * (_nvals - _nval)
            _values.append(value)
        df = DataFrame(_values, columns=fields)
    else:
        df = DataFrame(columns=fields)
        if "values" in json_data:
            values = json_data["values"]
            for value in values:
                _df = DataFrame(value[subcolkey])
                if len(_df.columns) < len(fields):
                    _df[fields[len(_df.columns) :]] = None
                _df.columns = fields
                if addcols:
                    for c in addcols:
                        _df[c] = value[c]
                for c in [k for k in value.keys() if k != subcolkey]:
                    _df[c] = value[c]
                    df = concat([df, _df])
    df.drop_duplicates(inplace=True)
    return df


def timeNow(typ="ms"):
    now = int(datetime.datetime.now().timestamp())
    if typ == "ms":
        now *= 1000
    return now

def time_to_seconds(time_str, retzero=False):
    if not time_str or not isinstance(time_str, str):
        return 0 if retzero else nan

    try:
        # Handle sign
        is_negative = time_str.startswith("-")
        time_str = time_str.lstrip("+-")

        # Split the time string into parts
        parts = time_str.split(":")

        # Depending on the number of parts, interpret hours, minutes, and seconds
        if len(parts) == 3:  # Hours, minutes, seconds.tenths
            hours, minutes, seconds = parts
            total_seconds = int(hours) * 3600 + int(minutes) * 60 + float(seconds)
        elif len(parts) == 2:  # Minutes, seconds.tenths
            minutes, seconds = parts
            total_seconds = int(minutes) * 60 + float(seconds)
        else:
            total_seconds = float(parts[0])

        # Apply negative sign if needed
        total_seconds = -total_seconds if is_negative else total_seconds

        # Round to 1 decimal place
        return round(total_seconds, 1)

    except (ValueError, TypeError):
        return 0 if retzero else nan


# Function to apply time delta
def apply_time_delta(base_time_str, delta_str):
    # Convert base time and delta time to seconds
    base_seconds = time_to_seconds(base_time_str)
    delta_seconds = time_to_seconds(delta_str)

    if base_seconds is None or delta_seconds is None:
        return None

    # If delta is positive, add, if negative, subtract
    if delta_str.startswith("-"):
        return round(base_seconds - delta_seconds, 1)
    else:
        return round(base_seconds + delta_seconds, 1)
