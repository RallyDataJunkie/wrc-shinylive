from urllib.parse import urljoin
from parse import parse
from jupyterlite_simple_cors_proxy.cacheproxy import CorsProxy, create_cached_proxy

# import pandas as pd
from pandas import to_datetime, date_range, to_numeric, DataFrame, concat, melt, merge
import datetime
from numpy import nan
from sqlite_utils import Database

SCHEMA_FULL_CALENDAR = {
    "id": str,
    "guid": str,
    "title": str,
    "location": str,
    "startDate": str,
    "endDate": str,
    "eventId": str,
    "rallyId": str,
    "description": str,
    "round": str,
    "cvpSeriesLink": str,
    "sponsor": str,
    "images": str,
    "season": str,
    "competition": str,
    "country": str,
    "asset": str,
    "__typename": str,
    "type": str,
    "uid": str,
    "seriesUid": str,
    "releaseYear": str,
    "availableOn": str,
    "availableTill": str,
    "startDateLocal": str,
    "endDateLocal": str,
    "finishDate": str,
    "championship": str,
    "championshipLogo": str,
}
SCHEMA_RESULTS_CALENDAR = {
    "id": str,
    "rallyTitle": str,
    "ROUND": str,
    "rallyCountry": str,
    "rallyCountryImage": str,
    "rallyId": str,
    "date": str,
    "startDate": str,
    "finishDate": str,
    "driverId": str,
    "driverCountryImage": str,
    "driver": str,
    "coDriverId": str,
    "coDriverCountryImage": str,
    "coDriver": str,
    "teamId": str,
    "teamLogo": str,
    "teamName": str,
    "manufacturer": str,
    "year": int,
}

# TO DO - refactor this into the WRCLiveTimingAPIClient class
# or create an enrichment class
def enrich_stage_winners(stagewinners, stages, inplace=True):
    if not inplace:
        stagewinners = stagewinners.copy()

    if not stages.empty:
        stagewinners = merge(
            stagewinners, stages[["stageNo", "day", "distance"]], on="stageNo"
        )
        stagewinners["wins_overall"] = stagewinners.groupby("carNo").cumcount() + 1

        stagewinners["daily_wins"] = (
            stagewinners.groupby(["day", "carNo"]).cumcount() + 1
        )

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
    split_durations, view, carNum2Names
):
    if split_times_wide_numeric.empty:
        return
    split_times_wide_numeric = (
        split_times_wide_numeric.copy()
    )

    if view in ["time_acc", "pos_acc"]:
        split_times_wide_numeric = merge(
            split_times_wide[
                ["carNo", "teamName", "roadPos"]
            ],
            split_times_wide_numeric,
            on="carNo",
        )
        split_times_wide_numeric["carNo"] = (
            split_times_wide_numeric["carNo"].map(
                carNum2Names
            )
        )
        # TO DO  precision number format formatting
        # styles = {c: "{0:0.1f}" for c in split_cols}
        # return split_times_wide_numeric.style.format(styles)
        split_times_wide_numeric.loc[:, split_cols] = (
            split_times_wide_numeric[split_cols].round(1)
        )

        if view == "pos_acc":
            split_times_wide_numeric.loc[:, split_cols] = (
                split_times_wide_numeric[split_cols].rank(
                    method="min", na_option="keep"
                )
            )

        split_times_wide_numeric.columns = (
            ["Driver", "TeamName", "RoadPos"]
            + [
                f"Split {i}"
                for i in range(1, len(split_cols))
            ]
            + ["Finish"]
        )
        return split_times_wide_numeric

    # We want within split times, not accumulated times
    # Scope the view if data available
    output_ = split_durations.copy()
    if split_dists:
        if view == "pos_within":
            output_.loc[:, split_cols] = output_[
                split_cols
            ].rank(method="min", na_option="keep")
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
        output_.loc[:, split_cols] = output_[
            split_cols
        ].round(1)

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
    return  output_


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
            if _nval <_nvals:
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
                    _df[fields[len(_df.columns):]] = None
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


class WRCLiveTimingAPIClient:
    """Client for accessing WRC live timing and results API data."""

    WRC_LIVETIMING_API_BASE = "https://api.wrc.com/content/{path}"

    CATEGORY_MAP = {
        "ALL": "all",
        "WRC": "wrc",
        "WRC2": "wrc2",
        "WRC3": "wrc3",
        "Junior WRC": "jwrc",
        "WRC2 Challenger": "wrc2c",
        "Master Cup": "mcup",
    }

    CATEGORY_MAP2 = {value: key for key, value in CATEGORY_MAP.items()}

    CHAMPIONSHIP_IDS = {
        2025: {
            "all": 287,
            "wrc": 289,
            "wrc2": 291,
            "wrc3": 298,
            "jwrc": 300,
            "wrc2c": 293,
            "mcup": 296,
        },
        2024: {
            "all": 245,
            "wrc": 247,
            "wrc2": 249,
            "wrc3": 256,
            "jwrc": 258,
            "wrc2c": 251,
            "mcup": 254,
        },
    }

    def __init__(
        self,
        year: int = datetime.date.today().year,
        championship: str = "wrc",
        group: str = "all",
        use_cache: bool = False,
        **cache_kwargs,
    ):
        """
        Initialize the WRC Live Timing and Results API client.

        Args:
            year: Default year for API requests
            category: Default category for API requests
            stage: Default stage for API requests
            use_cache: Whether to enable request caching
            **cache_kwargs: Cache configuration options passed to requests_cache. Examples: cache_name="WRCcache_2025", backend = sqlite|memory|filesystem;
            expire_after=SECONDS
        """
        self.year = year
        # We will auto update the championship based on
        # championshipId set separately as a class property
        self._championship = None #championship
        self.championshipId = None #self.getChampionshipId()
        self.championship = championship
        self.group = group
        self.full_calendar = DataFrame()
        self.results_calendar_df = DataFrame()

        self.rallyId2eventId = {}
        self.stage_codes = {}
        self.stage_ids = {}
        self.carNum2name = {}

        self.stage_details_df = DataFrame()
        self.startlist_df = DataFrame()
        self.itinerary_df = DataFrame()
        self.overall_df = DataFrame()
        self.stagewinners_df = DataFrame()
        self.retirements_df = DataFrame()
        self.penalties_df = DataFrame()

        self.seasonId = None
        self.eventId = None
        self.rallyId = None
        self._stageId = None
        self.stageId = None

        # Initialize the proxy with caching if requested
        if use_cache:
            self.proxy = create_cached_proxy(**cache_kwargs)
        else:
            self.proxy = CorsProxy()

        # DB utils
        self._db_filepath = f"wrc_TEST_results.db"
        self.db = Database(self._db_filepath)
        # TO DO - if we can persist the db, do not replace it
        self.db.create_table(
            "full_calendar", SCHEMA_FULL_CALENDAR, pk="rallyId", replace=True
        )
        self.db.create_table(
            "results_calendar", SCHEMA_RESULTS_CALENDAR, pk="rallyId", replace=True
        )

    def db_insert(self, table, df, pk=None):
        if not df.empty:
            self.db[table].insert_all(df.to_dict("records"), alter=True, pk=pk)

    def db_upsert(self, table, df, pk=None):
        if not df.empty:
            self.db[table].upsert_all(df.to_dict("records"), alter=True, pk=pk)

    @staticmethod
    def drop_ephemera_cols(df, keep=None, inplace=True):
        """We are working with unnormalised tables; this tidies them slightly."""
        if keep is None:
            keep = []
        else:
            keep = [keep] if isinstance(keep, str) else keep

        try:
            cols = [
                "driverCountry",
                "driverCountryImage",
                "coDriver",
                "coDriverId",
                "coDriverCountry",
                "coDriverCountryImage",
                "teamId",
                "team/car",
                "teamLogo",
            ]
            dropcols = [col for col in cols if col in df.columns and col not in keep]
            df.drop(columns=dropcols, inplace=True)
        except:
            print("Can't drop cols...")
        if not inplace:
            return df

    @property
    def championship(self):
        return self._championship

    @property
    def stageId(self):
        return self._stageId

    @property
    def db_results_calendar_df(self):
        """Dynamically fetches the latest data from the 'results_calendar' table."""
        return DataFrame(
            self.db.query(f"SELECT * FROM results_calendar WHERE year='{self.year}'")
        )

    @stageId.setter
    def stageId(self, value):
        # This will attempt to set a valid stage code
        # but will not validate the supplied value
        self._stageId = self.stage_codes.get(value, value) or self._stageId

    @championship.setter
    def championship(self, value):
        self._championship = value
        # Automatically update derived_value whenever championship changes
        self.championshipId = self.getChampionshipId()

    def getChampionshipId(self, championship=None):
        championship = self._championship if championship is None else championship
        return self.CHAMPIONSHIP_IDS[self.year][championship]

    def initialise(self, year=None, eventName=None):
        """Initialise with the calendar."""
        if year:
            self.year = year

        self.rallyId2eventId = {}
        self.stage_codes = {}
        self.stage_ids = {}

        self.results_calendar_df = DataFrame()
        self.stage_details_df = DataFrame()
        self.startlist_df = DataFrame()
        self.itinerary_df = DataFrame()
        self.overall_df = DataFrame()
        self.retirements_df = DataFrame()
        self.penalties_df = DataFrame()

        self.seasonId = None
        self.eventId = None
        self.rallyId = None
        self.stageId = None

        self.full_calendar = self.getFullCalendar()
        self.rallyId2eventId = (
            self.full_calendar[["rallyId", "eventId"]]
            .set_index("rallyId")["eventId"]
            .to_dict()
            if "rallyId" in self.full_calendar
            else {}
        )

        self.seasonId = (
            int(self.full_calendar["season"][0]["seasonId"])
            if not self.full_calendar.empty
            else None
        )

        self.setEvent(eventName=eventName)

    @staticmethod
    def subtract_from_rows(df, colsList, ignore_first_row=True):
        """
        Subtracts the values of specified columns in the first row from all rows except the first.
        Modifies the DataFrame in place.

        Parameters:
        df (DataFrame): The DataFrame to modify.
        colsList (list): List of column names to subtract.
        """
        df = df.copy()
        df.loc[int(ignore_first_row) :, colsList] -= df.loc[0, colsList].values.astype(
            float
        )  # Perform subtraction directly
        return df

    @staticmethod
    def rebaseTimes(times, rebaseId=None, idCol=None, rebaseCol=None):
        """Rebase times based on the time for a particular vehicle."""
        if not rebaseId or rebaseId == "NONE" or idCol is None or rebaseCol is None:
            return times
        return times[rebaseCol] - times.loc[times[idCol] == rebaseId, rebaseCol].iloc[0]

    @staticmethod
    def rebaseManyTimes(
        times, rebaseId=None, idCol=None, rebaseCols=None, inplace=False
    ):
        if not inplace:
            if not rebaseId or rebaseId == "NONE":
                return times
            times = times.copy()

        if rebaseId and rebaseId != "NONE":
            # Ensure rebaseCols is a list
            rebaseCols = [rebaseCols] if isinstance(rebaseCols, str) else rebaseCols

            # Fetch the reference values for the specified 'rebaseId'
            reference_values = times.loc[times[idCol] == rebaseId, rebaseCols].iloc[0]

            # Subtract only the specified columns
            times[rebaseCols] = times[rebaseCols].subtract(reference_values)

            if not inplace:
                return times

    @staticmethod
    def rebaseWithDummyValues(times, replacementVals, rebaseCols=None):
        """
        Add a dummy row, rebase the values, then remove the dummy row.

        :param times: DataFrame containing the data to be modified
        :param replacementVals: List of values to replace for each column in rebaseCols
        :param rebaseCols: List of columns to apply the rebase operation
        :return: Modified DataFrame with rebased values
        """
        if rebaseCols is None:
            return times
        times = times.copy()
        # TO DO:
        # should we have a generic checker that rebase cols are available
        # or subset to the ones that are?
        # If rebaseCols is not a list, make it a list
        rebaseCols = [rebaseCols] if isinstance(rebaseCols, str) else rebaseCols

        # Ensure replacementValsList is the same length as rebaseCols
        if len(replacementVals) != len(rebaseCols):
            raise ValueError(
                "replacementValsList must have the same length as rebaseCols"
            )

        # Create a dummy row with the replacement values
        dummy_row = {col: val for col, val in zip(rebaseCols, replacementVals)}

        # Append the dummy row to the DataFrame
        times = times.append(dummy_row, ignore_index=True)

        # Rebase using the dummy row (rebase the last row in the DataFrame)
        times[rebaseCols] = times[rebaseCols].subtract(
            times.loc[times.index[-1], rebaseCols]
        )

        # Remove the dummy row
        times = times.drop(times.index[-1])

        return times

    def setEvent(self, eventName=None):
        # If no event name provided, use current event
        if not eventName:
            # Set current_event as the last event in calendar before now
            self.eventId, self.rallyId = self.full_calendar[
                self.full_calendar["startDate"] < timeNow()
            ].iloc[-1][["eventId", "rallyId"]]

    def _WRC_json(self, path, base=None, retUrl=False):
        """Return JSON from API."""
        base = self.WRC_LIVETIMING_API_BASE if base is None else base
        url = urljoin(base, path)
        if retUrl:
            return url
        # print(f"Fetching: {url}")
        try:
            r = self.proxy.cors_proxy_get(url)
        except:
            print("Error trying to load data.")
            return {}
        # r = requests.get(url)
        rj = r.json()
        if "status" in rj and rj["status"] == "Not Found":
            return {}
        return r.json()

    @staticmethod
    def stage_id_annotations(df, eventId=None, rallyId=None, stageId=None):
        if "eventId" not in df.columns:
            df["eventId"] = eventId
        if "rallyId" not in df.columns:
            df["rallyId"] = rallyId
        if "stageId" not in df.columns:
            df["stageId"] = stageId

    def getFullCalendar(self, year=None, championship=None, size=20):
        year = self.year if year is None else year
        championship = self.championship if championship is None else championship

        stub = f"filters/calendar?language=en&size={size}&championship={championship}&origin=vcms&year={year}"
        json_data = self._WRC_json(stub)
        if not json_data:
            return DataFrame()
        # return json_data

        df_full_calendar = DataFrame(json_data["content"])
        self.full_calendar = df_full_calendar

        self.db_upsert("full_calendar", df_full_calendar, pk="rallyId")

        return df_full_calendar

    def getResultsCalendar(
        self, year=None, seasonId=None, championship="wrc", retUrl=False, update=False
    ):
        """Get the WRC Calendar for a given season ID as a JSON result."""
        if self.results_calendar_df.empty or update:
            seasonId = self.seasonId if seasonId is None else seasonId
            stub = (
                f"result/calendar?season={seasonId}&championship={championship.lower()}"
            )
            if retUrl:
                return stub
            json_data = self._WRC_json(stub)
            df_calendar = tablify(json_data)
            if not df_calendar.empty:
                if "date" in df_calendar.columns:
                    df_calendar["year"] = (
                        df_calendar["date"].str.extract(r"(\d{4})").astype("Int64")
                    )
            # timeify(df_calendar, "date", "daterange")
            # timeify(df_calendar, "startDate")
            # timeify(df_calendar, "finishDate")
            # df_calendar.set_index("id", inplace=True)
            self.results_calendar_df = df_calendar
            # TO DO - we don't need to update if the data is
            # already in the db?
            self.db_upsert("results_calendar", df_calendar, pk="rallyId")

        return self.results_calendar_df

    def getStageDetails(self, eventId=None, rallyId=None, update=False):
        if self.stage_details_df.empty or update:
            eventId = self.eventId if eventId is None else eventId
            rallyId = self.rallyId if rallyId is None else rallyId
            stub = f"result/stages?eventId={eventId}&rallyId={rallyId}&championship=wrc"
            json_data = self._WRC_json(stub)
            if not json_data:
                return DataFrame()
            df_stageDetails = tablify(json_data)
            if "STAGE" in df_stageDetails.columns:
                df_stageDetails.rename(columns={"STAGE": "stageNo"}, inplace=True)
            if df_stageDetails.empty:
                return DataFrame()
            self.stage_details_df = df_stageDetails
            self.stage_codes = (
                df_stageDetails[["stageNo", "stageId"]]
                .set_index("stageNo")["stageId"]
                .to_dict()
            )
            self.stage_ids = {v: k for k, v in self.stage_codes.items()}
            # Type mapping
            df_stageDetails["distance"] = to_numeric(
                df_stageDetails["distance"], errors="coerce"
            )

        return self.stage_details_df

    def getItinerary(self, latest=True, eventId=None, update=False):
        if self.itinerary_df.empty or update:
            eventId = self.eventId if eventId is None else eventId

            stub = f"result/itinerary?eventId={eventId}&extended=true"
            json_data = self._WRC_json(stub)
            if not json_data:
                return DataFrame()
            df_itinerary = tablify(json_data, "values")
            if latest:
                # Get latest itinerary
                df_itinerary.drop_duplicates(
                    subset=["stage"], keep="last", inplace=True
                )
                df_itinerary.reset_index(drop=True, inplace=True)

            self.itinerary_df = df_itinerary
        return self.itinerary_df

    def getStartlist(self, eventId=None, update=False):
        if self.startlist_df.empty or update:
            eventId = self.eventId if eventId is None else eventId
            stub = f"result/startLists?eventId={eventId}"
            json_data = self._WRC_json(stub)
            if not json_data:
                return DataFrame()
            # We
            df_startlist = tablify(
                json_data, "startListItems", addcols=["date", "startDateTimeLocal"]
            )
            self.startlist_df = df_startlist
            self.carNum2name = (
                self.startlist_df[["carNo", "driver"]]
                .set_index("carNo")["driver"]
                .str.split(" ")
                .apply(lambda x: x[-1][:3])
                .to_dict()
            )
        return self.startlist_df

    def getOverall(
        self,
        eventId=None,
        rallyId=None,
        stageId=None,
        championship=None,
        group=None,
        update=False,
    ):
        group = self.group if group is None else group
        # We need to invalidate update if we have changed the stageId
        if self.overall_df.empty or update:
            eventId = self.eventId if eventId is None else eventId
            rallyId = self.rallyId if rallyId is None else rallyId
            stageId = self.stage_codes.get(stageId, stageId) or self.stageId
            championship = self.championship if championship is None else championship

            stub = f"result/stageResult?eventId={eventId}&rallyId={rallyId}&stageId={stageId}&championshipId={self.getChampionshipId(championship)}&championship={championship}"
            json_data = self._WRC_json(stub)
            if not json_data:
                return DataFrame()
            df_overall = tablify(json_data)
            if "pos" in df_overall:
                df_overall["pos"] = df_overall["pos"].astype("Int64")
            # If we are in shakedown, then the times are in roundN
            if "totalTime" in df_overall:
                df_overall["totalTimeInS"] = df_overall["totalTime"].apply(
                    time_to_seconds, retzero=True
                )
                df_overall["timeToCarBehind"] = abs(df_overall["totalTimeInS"].diff(-1))
                if "diffFirst" in df_overall:
                    df_overall["overallGap"] = df_overall["diffFirst"].apply(
                        time_to_seconds, retzero=True
                    )
                if "diffPrev" in df_overall:
                    df_overall["overallDiff"] = df_overall["diffPrev"].apply(
                        time_to_seconds, retzero=True
                    )
            self.stage_id_annotations(df_overall, eventId, rallyId, stageId)
            self.overall_df = df_overall

        if group == "all":
            return self.overall_df
        else:
            return self.overall_df[self.overall_df["groupClass"] == group]

    def getSplitsLong(self, splits_wide__df):
        if splits_wide__df.empty:
            return DataFrame()
        splits_long_df = melt(
            splits_wide__df,
            id_vars={
                "carNo",
                "driver",
                "team/car",
                "teamName",
                "eligibility",
                "groupClass",
            }.intersection(splits_wide__df.columns),
            value_vars=[c for c in splits_wide__df.columns if c.startswith("round")],
            var_name="roundN",
            value_name="_time",
        ).dropna()
        splits_long_df["round"] = (
            splits_long_df["roundN"].str.replace("round", "").astype("Int64")
        )
        splits_long_df["timeInS_"] = splits_long_df["_time"].apply(time_to_seconds)

        # Normalise times
        def process_time(group):
            first_time = group["timeInS_"].iloc[0]
            group.loc[group.index[1:], "timeInS"] = (
                group["timeInS_"].iloc[1:] + first_time
            )
            group.loc[group.index[0], "timeInS"] = first_time
            return group

        splits_long_df = splits_long_df.groupby("round").apply(
            process_time
        )  # .reset_index(drop=True)

        return splits_long_df

    # We could look up the stageId for shakedown
    # but that's more API calls; a heuristic is cheaper...
    def get_shakedown_times(self, stage_times=None, shakedownId="SHD"):
        stage_times = (
            self.getStageTimes(stageId=shakedownId)
            if stage_times is None
            else stage_times
        )
        return self.getSplitsLong(stage_times=stage_times)

    def getStageTimes(
        self,
        eventId=None,
        rallyId=None,
        stageId=None,
        championship=None,
    ):
        eventId = self.eventId if eventId is None else eventId
        rallyId = self.rallyId if rallyId is None else rallyId
        # Try to be robust on stageId being incorrectly entered...
        stageId = self.stage_codes.get(stageId, stageId) or self.stageId
        championship = self.championship if championship is None else championship
        stub = f"result/stageTimes?eventId={eventId}&rallyId={rallyId}&stageId={stageId}&championshipId={self.getChampionshipId(championship)}&championship={championship}"
        json_data = self._WRC_json(stub)
        if not json_data:
            return DataFrame()
        df_stageTimes = tablify(json_data)
        if df_stageTimes.empty:
            return df_stageTimes

        self.stage_id_annotations(df_stageTimes, eventId, rallyId, stageId)

        if "pos" in df_stageTimes:
            df_stageTimes["pos"] = df_stageTimes["pos"].astype("Int64")

        if "diffFirst" in df_stageTimes:
            df_stageTimes["Gap"] = df_stageTimes["diffFirst"].apply(
                time_to_seconds, retzero=True
            )
        if "diffPrev" in df_stageTimes:
            df_stageTimes["Diff"] = df_stageTimes["diffPrev"].apply(
                time_to_seconds, retzero=True
            )
        if "stageTime" in df_stageTimes:
            df_stageTimes["timeInS"] = df_stageTimes["stageTime"].apply(
                time_to_seconds, retzero=True
            )
            df_stageTimes["timeToCarBehind"] = df_stageTimes["timeInS"].diff(-1)
            # Pace annotations
            df_stageDetails = self.getStageDetails()
            stage_dist = float(
                df_stageDetails.loc[
                    df_stageDetails["stageId"] == stageId, "distance"
                ].iloc[0]
            )
            df_stageTimes["speed (km/h)"] = stage_dist / (
                df_stageTimes["timeInS"] / 3600
            )
            # Use .loc[] to modify the original DataFrame in place
            df_stageTimes["pace (s/km)"] = df_stageTimes["timeInS"] / stage_dist
            df_stageTimes["pace diff (s/km)"] = (
                df_stageTimes["pace (s/km)"] - df_stageTimes.loc[0, "pace (s/km)"]
            )
            # A percent diff is always relative to something
            # In rebasing, we need to work with the actual times
            # so handle percentage diffs in the display logic for now?
            # df_stageTimes["percent"] = 100 * df_stageTimes["timeInS"] / df_stageTimes.loc[0,"timeInS"]

        return df_stageTimes

    def getSplitTimes(
        self,
        eventId=None,
        rallyId=None,
        stageId=None,
        championship=None,
    ):
        eventId = self.eventId if eventId is None else eventId
        rallyId = self.rallyId if rallyId is None else rallyId
        stageId = self.stage_codes.get(stageId, stageId) or self.stageId
        championship = self.championship if championship is None else championship
        if int(self.year) > 2023:
            stub = f"result/splitTime?championshipId={self.getChampionshipId(championship)}&eventId={eventId}&rallyId={rallyId}&stageId={stageId}&championship={championship}"
        else:
            stub = f"result/splitTime?eventId={eventId}&rallyId={rallyId}&stageId={stageId}&championship={championship}"

        json_data = self._WRC_json(stub)
        if not json_data:
            return DataFrame()
        df_splitTimes = tablify(json_data)
        self.stage_id_annotations(df_splitTimes, eventId, rallyId, stageId)
        df_splitTimes.dropna(how="all", axis=1, inplace=True)
        df_splitTimes.rename(columns={"pos": "roadPos"}, inplace=True)
        return df_splitTimes

    def get_splits_as_numeric(self, splits, regularise=True):
        """Convert the original split data to numerics."""

        split_cols = [c for c in splits.columns if c.startswith("round")]
        base_cols = list(
            {"carNo", "stageTime", "diffFirst"}.intersection(splits.columns)
        )
        sw_actual = splits[base_cols + split_cols].copy()
        # Convert string relative times to numeric relative times
        for c in split_cols:
            sw_actual[c] = sw_actual[c].apply(time_to_seconds)

        # The original data has a stage time in the first row
        # and the delta for the other rows
        # Recreate the actual times

        if len(split_cols) > 0 and regularise:
            if "stageTime" in sw_actual.columns:
                sw_actual["stageTime"] = sw_actual["stageTime"].apply(time_to_seconds)
                sw_actual[f"round{len(split_cols)+1}"] = sw_actual.apply(
                    lambda row: (row["stageTime"]),
                    axis=1,
                )
                sw_actual.drop(columns=["stageTime", "diffFirst"], inplace=True)

            sw_actual.loc[1:, split_cols] = sw_actual[split_cols][1:].add(
                sw_actual[split_cols].iloc[0]
            )
            sw_actual[split_cols] = sw_actual[split_cols].round(1)
        return sw_actual

    def get_split_duration(self, df, split_cols, ret_id=True, id_col="carNo"):
        """The time it takes a car to traverse a split section."""
        # Ensure split_cols are strings
        split_cols = [str(col) for col in split_cols]

        # Create a copy of the dataframe with selected columns
        df_ = df[split_cols].copy()

        # Calculate differences between consecutive columns
        diff_df = df_[split_cols[1:]].values - df_[split_cols[:-1]].values

        # Convert back to dataframe
        diff_df = DataFrame(diff_df, columns=split_cols[1:], index=df_.index)

        # Add first split column back
        diff_df[split_cols[0]] = df_[split_cols[0]]

        if ret_id:
            # Add entryId column
            diff_df[id_col] = df[id_col]

            # Reorder columns
            cols = [id_col] + split_cols
            return diff_df[cols]

        return diff_df

    def getStageWinners(self, eventId=None, championship=None, update=False):
        if self.stagewinners_df.empty or update:
            eventId = self.eventId if eventId is None else eventId
            championship = self.championship if championship is None else championship
            stub = f"result/stageWinners?eventId={eventId}&championshipId={self.getChampionshipId(championship)}"
            json_data = self._WRC_json(stub)
            if not json_data:
                return DataFrame()
            df_stageWinners = tablify(json_data)
            df_stageWinners["timeInS"] = df_stageWinners["time"].apply(time_to_seconds)
            self.stagewinners_df = df_stageWinners
        return self.stagewinners_df

    def getPenalties(self, eventId=None, update=False):
        if self.penalties_df.empty or update:
            eventId = self.eventId if eventId is None else eventId

            stub = f"result/penalty?eventId={eventId}"
            json_data = self._WRC_json(stub)
            if not json_data:
                return DataFrame()
            df_penalties = tablify(json_data)
            self.penalties_df = df_penalties
        return self.penalties_df

    def getRetirements(self, eventId=None, update=False):
        if self.penalties_df.empty or update:
            eventId = self.eventId if eventId is None else eventId

            stub = f"result/retirements?eventId={eventId}"
            json_data = self._WRC_json(stub)
            if not json_data:
                return DataFrame()
            df_retirements = tablify(json_data)
            self.retirements_df = df_retirements

        return self.retirements_df

    def getChampionship(
        self, seasonId=None, championship_type="driver", championship=None, retUrl=False
    ):
        seasonId = self.seasonId if seasonId is None else seasonId
        championship = self.championship if championship is None else championship
        """championship_tpe: driver | codriver | manufacturer"""

        stub = f"result/championshipresult?seasonId={seasonId}&championshipId={self.getChampionshipId(championship)}&type={championship_type}&championship={championship}"
        if retUrl:
            return stub
        json_data = self._WRC_json(stub)
        if (
            not json_data
            or "message" in json_data
            and "championship standing unavailble" in json_data["message"]
        ):
            return DataFrame()

        df_splitTimes = tablify(json_data)
        return df_splitTimes
