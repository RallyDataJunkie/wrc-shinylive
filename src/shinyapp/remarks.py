from pandas import to_datetime
from rules_processor import p, numToWords, andList, Nth, nth
from wrc_rallydj.utils import dateNow


def season_background_remarks(wrc, seasonId):
    """Generate a background report describing the current season."""

    season_rounds = wrc.getSeasonRounds(seasonId=int(seasonId))
    if season_rounds.empty:
        return "No season information available."

    seasons_info = wrc.getSeasons()
    season_info = seasons_info[seasons_info["seasonId"] == seasonId].iloc[0]

    # Creating a heading / banner of Championship and year
    banner_ = f"""{season_info["year"]} {season_info["name"]}"""

    # Report the number of rounds in, and time extent of, the season
    first_date = to_datetime(season_rounds["startDate"].iloc[0])
    last_date = to_datetime(season_rounds["startDate"].iloc[-1])
    months_ = (last_date.year - first_date.year) * 12 + (
        last_date.month - first_date.month
    )
    if first_date.month == last_date.month:
        months2_ = f"(*{first_date.strftime('%B')}*)"
    else:
        months2_ = f"(*{first_date.strftime('%B')}* to *{last_date.strftime('%B')}*)"

    md = f"""The complete *__{banner_}__* season takes in *__{numToWords(len(season_rounds))}__ rounds* over *__{numToWords(months_)} months__* {months2_}"""

    # Surfaces: we need two strategies: all surfaces available, some surfaces available.
    surfaces_available = season_rounds[
        ~(season_rounds["surfaces"].isna()) & ~(season_rounds["surfaces"] == "")
    ]
    surfaces_available_n = surfaces_available.shape[0]
    if surfaces_available_n == len(season_rounds):
        # All surfaces available
        surfaces_ = [
            f"*__{s}__*" for s in season_rounds["surfaces"].str.lower().unique()
        ]
        surfaces_ = andList(surfaces_)

        md = f"""{md} and *{numToWords(season_rounds["surfaces"].nunique())} surface types* ({surfaces_})."""
    elif surfaces_available_n:
        surfaces_ = [
            f"""*__{s.lower()}__*""" for s in season_rounds["surfaces"].unique() if s
        ]
        surfaces_ = ", ".join(surfaces_)
        md = f"""{md} and includes at least {numToWords(surfaces_available_n)} surface {p.plural("type", surfaces_available_n)} ({surfaces_})."""
    else:
        md = f"""{md}."""

    rallies_ = []
    for _, row in season_rounds.iterrows():
        startDate = to_datetime(row["startDate"])
        finishDate = to_datetime(row["finishDate"])

        if startDate.month == finishDate.month:
            month_ = startDate.strftime("%B")
        else:
            month_ = f"""{startDate.strftime('%B')}/{finishDate.strftime('%B')}"""
        rallies_.append(f"*{row['name']}* ({month_})")

    # A list of rallies in the season
    md = f"""{md}\n\nSpecifically, the *{banner_}* incorporates the following rallies: {andList(rallies_)}"""

    if surfaces_available_n == len(season_rounds):
        # Group by surface and collect the names
        surface_groups = season_rounds.groupby("surfaces")["name"].apply(list).to_dict()

        # For each surface, count the associated rallies and name them
        surface_groups_ = []
        for i, (surface, names) in enumerate(surface_groups.items()):
            count = numToWords(len(names))
            surface_ = ", ".join([f"*{n}*" for n in names])
            surface_groups_.append(
                f"*__{count} {surface.lower()}__* {p.plural("rally", len(names))} ({surface_})"
            )

        surface_rallies_ = f"""There {p.plural("is", len(surface_groups_))} {andList(surface_groups_)}"""

        md = f"""{md}\n\n{surface_rallies_}"""

    # A list of championships offered through the season
    championships = wrc.getChampionships()
    if not championships.empty:
        championships_ = [f"""*{c}*""" for c in championships["name"]]
        championships_ = ", ".join(championships_)
        championship_types_ = [f"""*{c}*""" for c in championships["type"].unique()]
        md = f"""{md}\n\n The season also incorporates *__{numToWords(len(championships))} championships__*, including championships for {andList(championship_types_)}.\n\nMore specifically, the championships are: {championships_}\n\n"""
    else:
        md = f"""{md}."""

    return md


def season_overview_remarks(wrc, seasonId):
    season_rounds = wrc.getSeasonRounds(seasonId=int(seasonId))
    if season_rounds.empty:
        return "No season information available."

    now_date = dateNow()
    md = ""

    previous_rallies = season_rounds[season_rounds["finishDate"] < now_date]
    future_rallies = season_rounds[season_rounds["startDate"] > now_date]
    # We should use an isLive style function to get this?
    current_rally = season_rounds[
        (season_rounds["startDate"] >= now_date)
        & (season_rounds["finishDate"] <= now_date)
    ]

    if previous_rallies.empty:
        md = "There are no prevoulsy run rallies for this season."
    else:
        completed_rallies_ = [f"*{r}*" for r in previous_rallies["name"].to_list()]
        completed_rallies_ = andList(completed_rallies_)
        md = f"""__{numToWords(previous_rallies.shape[0]).capitalize()}__ rallies have been completed to date: {completed_rallies_} """

    if future_rallies.empty:
        if not current_rally.empty:
            md = f"""{md}\n\n. *This is the last rally of the season.* There are no more rallies to run after this one."""
    else:
        num_future = future_rallies.shape[0]
        final_ = f""", and the final rally of the season,""" if num_future == 1 else ""
        future_rallies_ = [f"*{r}*" for r in future_rallies["name"].to_list()]
        future_rallies_ = andList(future_rallies_)
        md = f"""{md}\n\nThere {p.plural("is",num_future)} still __{numToWords(num_future)}__ more {p.plural("rally",num_future)} to run: {future_rallies_}"""

    if current_rally.empty and not future_rallies.empty:
        next_rally = future_rallies.iloc[0]
        startDate = to_datetime(next_rally["startDate"])
        finishDate = to_datetime(next_rally["finishDate"])
        from_ = f"""{startDate.strftime('%B')} {p.ordinal(startDate.month)}, {startDate.year}"""
        to_ = f"""{finishDate.strftime('%B')} {p.ordinal(startDate.month)}, {finishDate.year}"""
        md = f"""{md}\n\nThe next rally to run is __{next_rally["name"]}__, from {from_} to {to_}."""
    else:
        md = f"""{md}\n\n__{current_rally["name"].iloc[0]}__ is currently running."""

    return md


def event_background_remarks(wrc, eventId):

    season = wrc.getSeasonRounds()
    if season.empty:
        return "*No season information.*"

    season["surfaceCount"] = season.groupby("surfaces").cumcount() + 1
    event = season[season["eventId"] == int(eventId)]
    if event.empty:
        return "*No event information.*"

    md = []
    event = event.iloc[0]

    last_event_ = (
        f""", and the last event of the year"""
        if event["order"] == season["order"].max()
        else ""
    )
    _and = "and the " if not last_event_ else ""

    surface_ = (
        f""" {_and}{Nth(event["surfaceCount"])} {event["surfaces"].lower()} rally"""
    )
    md_ = f"""*{event["name"]}* ({event["country.name"]}, {event["country.iso3"]}), the {Nth(event["order"])} event of the season, {surface_}{last_event_}."""
    md.append(md_)
    start_date = to_datetime(event["startDate"])
    finish_date = to_datetime(event["finishDate"])
    date_now = to_datetime(dateNow())
    if start_date > date_now:
        run_state_ = "runs from"
    elif date_now <= finish_date:
        run_state_ = "running from"
    else:
        run_state_ = "ran from"
    start_month = to_datetime(start_date).strftime("%B")
    finish_month = to_datetime(finish_date).strftime("%B")
    same_month = start_month == finish_month
    if same_month:
        monthdates_ = f"""{nth(start_date.day)} to {nth(finish_date.day)} {start_month}, {start_date.year}"""
    else:
        monthdates_ = f"""{nth(start_date.day)} {start_month} to {nth(finish_date.day)} {finish_month}, {finish_month.year}"""
    md_ = f"""Based in  {event["location"]}, the event {run_state_} {monthdates_}."""
    md.append(md_)

    return "\n\n".join(md)

def stage_times_remarks(wrc):
    md = "*Additional remarks TO DO*"
    return md
