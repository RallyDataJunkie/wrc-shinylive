from parse import parse
from datetime import timedelta, datetime, date
import pytz


# import pandas as pd
from pandas import to_datetime, date_range, DataFrame, concat, merge, isna
from numpy import nan

def dateNow(weekend=False):
    today = date.today()
    current_date = today + timedelta(days=(6 - today.weekday())) if weekend else today

    return current_date.strftime("%Y-%m-%d")


def is_date_in_range(date_input, date_dict, tristate=False):
    """
    Check if a date is within the range of start and finish dates from an event dictionary.

    Args:
        date_input: Current date as datetime object or string (YYYY-MM-DD)
        date_dict: Dictionary containing event information with startDate and/or finishDate
        tristate: If True, returns:
                  1 if event is in future
                  0 if event is current/live
                  -1 if event is in past

    Returns:
        Boolean (if tristate=False) or int [-1, 0, 1] (if tristate=True)
    """
    from datetime import datetime
    import pytz

    # Check if date_input is a datetime object or a string
    if isinstance(date_input, datetime):
        # Make sure current_date doesn't already have timezone info
        if date_input.tzinfo is not None:
            current_date = date_input.replace(tzinfo=None)
        else:
            current_date = date_input
    else:
        # Convert current date string to datetime (assuming 'YYYY-MM-DD' format)
        current_date = datetime.strptime(date_input, "%Y-%m-%d")

    # Determine the timezone to use
    if "timeZoneId" in date_dict:
        try:
            event_timezone = pytz.timezone(date_dict["timeZoneId"])
        except:
            event_timezone = pytz.timezone("UTC")
    else:
        # Fallback to UTC if no timeZoneId is provided
        event_timezone = pytz.timezone("UTC")

    # Initialize variables for startDate and finishDate
    localized_start_date = None
    localized_finish_date = None

    # If startDate exists, convert and localize it to midnight (00:00:00) of that day
    if "startDate" in date_dict and date_dict["startDate"]:
        start_date = datetime.strptime(date_dict["startDate"], "%Y-%m-%d")
        # Set time to start of day (00:00:00)
        start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        localized_start_date = event_timezone.localize(start_date)

    # If finishDate exists, convert and localize it to end of day (23:59:59)
    if "finishDate" in date_dict and date_dict["finishDate"]:
        finish_date = datetime.strptime(date_dict["finishDate"], "%Y-%m-%d")
        # Set time to end of day (23:59:59)
        finish_date = finish_date.replace(
            hour=23, minute=59, second=59, microsecond=999999
        )
        localized_finish_date = event_timezone.localize(finish_date)

    # Add hour/minute/second to current_date if not already there, then localize it
    current_date_with_time = current_date
    localized_current_date = event_timezone.localize(current_date_with_time)

    # If both startDate and finishDate are missing, we can just check the current date
    if localized_start_date is None and localized_finish_date is None:
        return False  # No bounds, so current date is considered "out of range"

    # Compare current date to startDate and finishDate if they exist
    if not tristate:
        if localized_start_date and localized_finish_date:
            # Check if current date is within the range of startDate and finishDate
            return (
                localized_start_date <= localized_current_date <= localized_finish_date
            )
        elif localized_start_date:
            # Only startDate exists, check if current date is after startDate
            return localized_start_date <= localized_current_date
        elif localized_finish_date:
            # Only finishDate exists, check if current date is before finishDate
            return localized_current_date <= localized_finish_date
    else:
        # Tristate mode
        if localized_start_date and localized_current_date < localized_start_date:
            # Event is in future
            return 1
        elif localized_finish_date and localized_current_date > localized_finish_date:
            # Event is in past
            return -1
        else:
            # Event is current/live
            return 0


def format_timedelta(t, units="ms", addplus=False):
    """
    Convert seconds to a precise hh:mm:ss.tenth format string.

    Args:
    milliseconds (float): Total number of milliseconds

    Returns:
    str: Formatted time string in hh:mm:ss.tenth format
    """
    if isna(t):
        return "No time?"

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
    # Return if empty or already enriched
    if stagewinners.empty or "wins_overall" in stagewinners:
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
    now = int(datetime.now().timestamp())
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
