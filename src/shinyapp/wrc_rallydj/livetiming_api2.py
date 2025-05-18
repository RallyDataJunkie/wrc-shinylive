import logging

# Set a basic logging level
logging.basicConfig(level=logging.INFO)

# Logging for this package
logger = logging.getLogger(__name__)
# Set logging level for this package
logger.setLevel(logging.DEBUG)

from urllib.parse import urljoin
from datetime import datetime, timedelta, date
from sqlite_utils import Database
from jupyterlite_simple_cors_proxy.cacheproxy import CorsProxy, create_cached_proxy
import os
import re
import sqlite3
from wrc_rallydj.db_table_schemas import SETUP_V2_Q
from wrc_rallydj.utils import is_date_in_range, dateNow, timeNow
from pandas import (
    read_sql,
    DataFrame,
    json_normalize,
    merge,
    pivot,
    concat,
    to_datetime,
    notnull,
    to_numeric,
    isna,
)

from numpy import nan, where


class DatabaseManager:
    def __init__(self, dbname, newdb=False, dbReadOnly=False):
        self.dbname = dbname
        self.conn = self.setup_db(newdb=newdb)
        self.dbReadOnly = dbReadOnly

    def setup_db(self, newdb=False):
        logger.info("Initialising the database...")
        if os.path.isfile(self.dbname) and newdb:
            os.remove(self.dbname)

        if not os.path.isfile(self.dbname):
            newdb = True

        conn = sqlite3.connect(self.dbname, timeout=10)

        if newdb:
            self.initialize_db(conn)

        return conn

    def initialize_db(self, conn):
        logger.info("Creating new db tables...")
        c = conn.cursor()
        c.executescript(SETUP_V2_Q)

    def read_sql(self, query):
        return read_sql(query, self.conn)

    def dbfy(self, df, table, if_exists="upsert", pk=None, index=False, clear=False):
        if self.dbReadOnly:
            return

        if if_exists == "upsert" and not pk:
            return

        if if_exists == "replace":
            clear = True
            if_exists = "append"
        if clear:
            self.cleardbtable(table)

        cols = read_sql(f"PRAGMA table_info({table})", self.conn)["name"].tolist()
        if "" in df.columns:
            df.drop(columns="", inplace=True)

        for c in df.columns:
            if c not in cols:
                df.drop(columns=[c], inplace=True)

        if if_exists == "upsert":
            logger.info(f"Upserting {table}...")
            DB = Database(self.conn)
            DB[table].upsert_all(df.to_dict(orient="records"), pk=pk)
        else:
            logger.info(f"Inserting {table} (if_exists: {if_exists})...")
            df.to_sql(table, self.conn, if_exists=if_exists, index=index)

    def cleardbtable(self, table):
        c = self.conn.cursor()
        c.execute(f'DELETE FROM "{table}"')


# TO DO
# Introduce a DBMediatedAPIclient class which sits between
# and APIClient and WRCTimingResultsAPIClientV2
# and that checks livestate, running status and makes a decision
# about whether to call API (and maybe update db) or just use the db table
class APIClient:
    RED_BULL_LIVETIMING_API_BASE = (
        "https://p-p.redbull.com/rb-wrccom-lintegration-yv-prod/api/"
    )

    ITINERARY_REFRESH_PERIOD = 30

    def __init__(self, db_manager=None, use_cache=False, **cache_kwargs):
        self.db_manager = db_manager
        self.proxy = create_cached_proxy(**cache_kwargs) if use_cache else CorsProxy()
        self.lastreferenced = {}

    def dbfy(self, *args, **kwargs):
        self.db_manager.dbfy(*args, **kwargs)

    def _WRC_RedBull_json(self, path, base=None, retUrl=False):
        """Return JSON from API."""
        base = self.RED_BULL_LIVETIMING_API_BASE if base is None else base
        url = urljoin(base, path)
        print(url)
        if retUrl:
            return url
        # print(f"Fetching: {url}")
        try:
            r = self.proxy.cors_proxy_get(url)
        except:
            print("Error trying to load data.")
            return {}
        # r = requests.get(url)
        if r.status_code!=200:
            return()
        
        rj = r.json()
        if "status" in rj and rj["status"] == "Not Found":
            return {}
        return r.json()

    def _getSeasons(self, updateDB=False):
        """The seasons feed is regularly updated throughout the season."""
        stub = f"seasons.json"
        json_data = self._WRC_RedBull_json(stub)
        seasons_df = DataFrame(json_data)
        if seasons_df.empty:
            return DataFrame()

        if updateDB:
            self.dbfy(seasons_df, "seasons", pk="seasonId")

        return seasons_df

    def _getSeasonDetail(self, seasonId, updateDB=False):
        if not seasonId:
            return DataFrame(), DataFrame(), DataFrame()
         
        stub = f"season-detail.json?seasonId={seasonId}"
        json_data = self._WRC_RedBull_json(stub)
        if "championships" not in json_data:
            return DataFrame(), DataFrame(), DataFrame()

        championships_df = DataFrame(json_data["championships"])
        seasonRounds_df = DataFrame(json_data["seasonRounds"])
        _seasonRounds_df = json_normalize(seasonRounds_df["event"])
        _cols = [
            "eventId",
            "countryId",
            "name",
            "slug",
            "location",
            "startDate",
            "finishDate",
            "timeZoneId",
            "timeZoneName",
            "timeZoneOffset",
            "surfaces",
            "organiserUrl",
            "categories",
            "mode",
            "trackingEventId",
            "clerkOfTheCourse",
            "stewards",
            "templateFilename",
            "country.countryId",
            "country.name",
            "country.iso2",
            "country.iso3",
            "seasonId",
            "order",
        ]
        seasonRounds_df = merge(
            _seasonRounds_df, seasonRounds_df[["eventId", "seasonId", "order"]]
        )[_cols]
        # TO DO - improve cleaning
        seasonRounds_df["name"] = seasonRounds_df["name"].str.strip()

        eligibilities_df = json_data["eligibilities"]

        if updateDB:
            self.dbfy(championships_df, "championship_lookup", pk="championshipId")
            self.dbfy(seasonRounds_df, "season_rounds", pk="eventId")

        return championships_df, seasonRounds_df, eligibilities_df

    def _getChampionshipOverallResults(self, championshipId, seasonId, updateDB=False):
        if not championshipId or not seasonId:
            return DataFrame(), DataFrame()

        stub = f"championship-overall-results.json?championshipId={championshipId}&seasonId={seasonId}"
        json_data = self._WRC_RedBull_json(stub)
        if "entryResults" not in json_data:
            return DataFrame(), DataFrame()

        _championshipEntryResultsOverall = []
        _championshipEntryResultsByRound = []

        for championshipEntry in json_data["entryResults"]:
            _championshipEntry = {}
            for k in ["championshipEntryId", "overallPosition", "overallPoints"]:
                _championshipEntry[k] = championshipEntry[k]
            if championshipEntry["roundResults"]:
                _championshipEntry["championshipId"] = championshipEntry[
                    "roundResults"
                ][0]["championshipId"]
            _championshipEntry["Round"] = len(championshipEntry["roundResults"])
            _championshipEntryResultsOverall.append(_championshipEntry)

            _championshipEntryResultsByRound.extend(championshipEntry["roundResults"])

        championshipEntryResultsOverall_df = DataFrame(_championshipEntryResultsOverall)
        championshipEntryResultsByRound_df = DataFrame(_championshipEntryResultsByRound)

        championshipEntryResultsOverall_df["seasonId"] = seasonId
        championshipEntryResultsByRound_df["seasonId"] = seasonId

        # HACK - we should not assume the eventId is ordered by round.
        # Need a function to order rounds by date
        # The eventId gives the last completed event/championship score following that event
        championshipEntryResultsOverall_df["eventId"] = (
            championshipEntryResultsByRound_df["eventId"].max()
        )

        if updateDB:
            self.dbfy(
                championshipEntryResultsOverall_df,
                "championship_overall",
                # if_exists="replace",
                pk=["championshipEntryId", "championshipId", "eventId"],
            )
            self.dbfy(
                championshipEntryResultsByRound_df,
                "championship_results",
                pk=("championshipEntryId", "eventId"),
            )

        return championshipEntryResultsOverall_df, championshipEntryResultsByRound_df

    def _getChampionshipDetail(
        self, year, championship, championshipId=None, seasonId=None, updateDB=False
    ):
        # If championshipId is None, try to find a championship Id
        if not championshipId:
            print("|SNAFU - no championshipId")
            return DataFrame(), DataFrame(), DataFrame()
            # Use WRC drivers as the default
            # XX get rid of _getChampionshipName
            # _championship = self._getChampionshipName()

            # seasonId, championships_df, _, _ = self._getSeasonDetail()
            # championshipId = championships_df[
            #    championships_df["name"] == _championship
            # ].iloc[0]["championshipId"]

        if seasonId is None:
            seasonId = self._getSeasons(championship, year).iloc[0]["seasonId"]

        stub = f"championship-detail.json?championshipId={championshipId}&seasonId={seasonId}"
        json_data = self._WRC_RedBull_json(stub)
        if "championshipRounds" not in json_data:
            return DataFrame(), DataFrame(), DataFrame()

        rounds = [r["event"] for r in json_data["championshipRounds"]]
        championshipRounds_df = DataFrame(rounds)
        championshipRounds_df["year"] = to_datetime(
            championshipRounds_df["startDate"]
        ).dt.year
        championshipCountries_df = json_normalize(
            championshipRounds_df["country"]
        ).drop_duplicates()
        championshipRounds_df.drop(columns=["country"], inplace=True)

        renamers = {
            k.replace("Description", ""): json_data[k]
            for k in json_data.keys()
            if k.startswith("field") and k.endswith("Description")
        }

        _e = json_data["championshipEntries"]
        championshipEntries_df = DataFrame(_e)
        # renamers["tyreManufacturer"] = "tyreManufacturer_"
        championshipEntries_df.rename(columns=renamers, inplace=True)

        championshipRounds_df = championshipRounds_df.sort_values(
            by="startDate", ascending=True
        ).reset_index(drop=True)
        championshipRounds_df["Round"] = range(1, len(championshipRounds_df) + 1)

        if updateDB:
            self.dbfy(
                championshipRounds_df,
                "championship_rounds_detail",
                pk="eventId",
            )
            self.dbfy(
                championshipCountries_df,
                "championship_countries",
                pk="countryId",
            )
            self.dbfy(
                championshipEntries_df,
                "championship_entries",
                pk="championshipEntryId",
            )

        return championshipRounds_df, championshipEntries_df, championshipCountries_df

    def _getEvent(self, eventId, updateDB=False):
        stub = f"events/{eventId}.json"
        json_data = self._WRC_RedBull_json(stub)
        if "rallies" not in json_data:
            return DataFrame(), DataFrame(), DataFrame()

        eventRallies_df = DataFrame(json_data["rallies"])
        eventRallies_df.drop(columns=["eventClasses"], inplace=True)
        eventClasses_df = DataFrame(json_data["eventClasses"])
        _data = {
            k: json_data[k] for k in json_data if k not in ["rallies", "eventClasses"]
        }
        eventData_df = json_normalize(_data)

        if updateDB:
            self.dbfy(eventClasses_df, "event_classes", pk=("eventId", "eventClassId"))
            self.dbfy(eventRallies_df, "event_rallies", pk="itineraryId")
            self.dbfy(eventData_df, "event_date", pk="eventId")

        return eventData_df, eventRallies_df, eventClasses_df

    def _getEntries(self, eventId, rallyId, updateDB=False):
        if not eventId:
            return (
                DataFrame(),
                DataFrame(),
                DataFrame(),
                DataFrame(),
                DataFrame(),
                DataFrame(),
                DataFrame(),
            )
        stub = f"events/{eventId}/rallies/{rallyId}/entries.json"
        json_data = self._WRC_RedBull_json(stub)
        entries_df = DataFrame(json_data)
        if entries_df.empty:
            return (
                DataFrame(),
                DataFrame(),
                DataFrame(),
                DataFrame(),
                DataFrame(),
                DataFrame(),
                DataFrame(),
            )

        entries_df["rallyId"] = rallyId
        drivers_df = json_normalize(entries_df["driver"])
        codrivers_df = json_normalize(entries_df["codriver"])
        manufacturers_df = json_normalize(entries_df["manufacturer"]).drop_duplicates()
        entrants_df = json_normalize(entries_df["entrant"]).drop_duplicates()
        entryGroups_df = json_normalize(entries_df["group"]).drop_duplicates()
        eventClasses_df = json_normalize(
            entries_df.explode("eventClasses")["eventClasses"]
        ).drop_duplicates()

        entries_df.drop(
            columns=[
                "driver",
                "codriver",
                "manufacturer",
                "entrant",
                "group",
                "eventClasses",
                "tags",
            ],
            inplace=True,
        )

        if updateDB:
            self.dbfy(entries_df, "entries", pk="entryId")
            self.dbfy(drivers_df, "entries_drivers", pk="personId")
            self.dbfy(codrivers_df, "entries_codrivers", pk="personId")
            self.dbfy(entryGroups_df, "groups", pk="groupId")
            self.dbfy(manufacturers_df, "manufacturers", pk="manufacturerId")
            self.dbfy(entrants_df, "entrants", pk="entrantId")

        return (
            entries_df,
            drivers_df,
            codrivers_df,
            manufacturers_df,
            entrants_df,
            entryGroups_df,
            eventClasses_df,
        )

    def _getStartLists(self, eventId, startListId=None, updateDB=False):
        if not eventId or isna(startListId) or not startListId:
            return DataFrame()

        stub = f"events/{eventId}/startLists/{startListId}.json"
        json_data = self._WRC_RedBull_json(stub)
        if "startListItems" not in json_data:
            return DataFrame()

        startlist_df = json_normalize(json_data["startListItems"])

        startlist_df["eventId"] = json_data["eventId"]
        startlist_df["name"] = json_data["name"]

        if updateDB:
            self.dbfy(startlist_df, "startlists", pk="startListItemId")

        return startlist_df

    def _getEventGroups(self, eventId, updateDB=False):
        if not eventId:
            return DataFrame()
        
        stub = f"events/{eventId}/groups.json"
        json_data = self._WRC_RedBull_json(stub)

        eventGroups_df = DataFrame(json_data)
        if eventGroups_df.empty:
            return DataFrame()

        if updateDB:
            self.dbfy(eventGroups_df, "groups", pk="groupId")
        return eventGroups_df

    def _getEventItineraries(self, eventId, itineraryId, updateDB=False):
        # TO DO need to mark an itinerary as having been fetched
        # using something similar to _updateCompletedStagesStatus
        # OR maybe not - the itinerary has a lot of status information.
        # We can maybe set up an ad hoc cache
        # TO DO -if the event is completed,  can add this to completed...
        # In fact, we can have a generic test that if the event is completed
        # we can add to the db and not have to fetch again.
        if "itinerary_json" not in self.lastreferenced:
            self.itineraryLegs_df = DataFrame()
            self.itinerarySections2_df = DataFrame()
            self.itineraryControls_df = DataFrame()
            self.itineraryStages_df = DataFrame()
            self.lastreferenced["itinerary_json"] = {'key':None, "t": 0}
        _key=f"{eventId}_{itineraryId}"
        if self.lastreferenced["itinerary_json"]["key"]!=_key:
            self.lastreferenced["itinerary_json"]["key"] = _key
            self.lastreferenced["itinerary_json"]["t"] = 0
        if (timeNow(typ="s") - self.lastreferenced["itinerary_json"]["t"]) < (
            self.ITINERARY_REFRESH_PERIOD
        ):
            return (
                self.itineraryLegs_df,
                self.itinerarySections2_df,
                self.itineraryControls_df,
                self.itineraryStages_df,
            )
        else:
            self.lastreferenced["itinerary_json"]["t"] = timeNow(typ="s")
        if not eventId or not itineraryId:
            return DataFrame(), DataFrame(), DataFrame(), DataFrame()
        stub = f"events/{eventId}/itineraries/{itineraryId}.json"
        json_data = self._WRC_RedBull_json(stub)
        if "itineraryLegs" not in json_data:
            return DataFrame(), DataFrame(), DataFrame(), DataFrame()

        itineraryLegs_df = DataFrame(json_data["itineraryLegs"])
        itineraryLegs_df["eventId"] = eventId

        if "itinerarySections" not in itineraryLegs_df:
            return itineraryLegs_df, DataFrame(), DataFrame(), DataFrame()

        itinerarySections_df = itineraryLegs_df.explode("itinerarySections")

        itinerarySections2_df = json_normalize(
            itinerarySections_df["itinerarySections"]
        )

        itineraryControls_df = itinerarySections2_df.explode("controls").reset_index(
            drop=True
        )
        _itineraryControls_df = json_normalize(itineraryControls_df["controls"])
        itineraryControls_df = concat(
            [itineraryControls_df.drop("controls", axis=1), _itineraryControls_df],
            axis=1,
        )
        itineraryStages_df = itinerarySections2_df.explode("stages").reset_index(
            drop=True
        )

        itineraryStages_df.rename(columns={"name": "name_"}, inplace=True)
        _itineraryStages_df = json_normalize(itineraryStages_df["stages"])
        itineraryStages_df = concat(
            [itineraryStages_df.drop("stages", axis=1), _itineraryStages_df], axis=1
        )

        itinerarySections2_df["eventId"] = eventId

        itineraryLegs_df.drop(columns=["itinerarySections"], inplace=True)
        itinerarySections2_df.drop(columns=["controls", "stages"], inplace=True)
        itineraryControls_df.drop(columns=["stages"], inplace=True)
        itineraryStages_df.drop(columns=["controls"], inplace=True)

        if updateDB:
            self.dbfy(itineraryLegs_df, "itinerary_legs", pk="itineraryLegId")
            self.dbfy(itineraryStages_df, "itinerary_stages", pk="stageId")
            self.dbfy(
                itinerarySections2_df, "itinerary_sections", pk="itinerarySectionId"
            )
            self.dbfy(itineraryControls_df, "itinerary_controls", pk="controlId")
            itineraryLegs_df["startListId"] = itineraryLegs_df["startListId"].astype(
                "Int64"
            )
            for _, row in itineraryLegs_df.iterrows():
                startListId = row["startListId"]
                self._getStartLists(
                    eventId=eventId, startListId=startListId, updateDB=updateDB
                )

        self.itineraryLegs_df = itineraryLegs_df
        self.itinerarySections2_df = itinerarySections2_df
        self.itineraryControls_df = itineraryControls_df
        self.itineraryStages_df = itineraryStages_df

        return (
            itineraryLegs_df,
            itinerarySections2_df,
            itineraryControls_df,
            itineraryStages_df,
        )

    def _getControlTimes(self, eventId, controlId, updateDB=False):
        if not eventId or not controlId:
            return DataFrame()
        stub = f"events/{eventId}/controls/{controlId}/controlTimes.json"
        json_data = self._WRC_RedBull_json(stub)
        controlTimes_df = DataFrame(json_data)
        if controlTimes_df.empty:
            return DataFrame()

        if updateDB:
            self.dbfy(controlTimes_df, "controltimes", pk="controlTimeId")

        return controlTimes_df

    def _getEventShakeDownTimes(self, eventId, run=1, updateDB=False):
        if not eventId:
            return
        stub = f"events/{eventId}/shakedowntimes.json?shakedownNumber={run}"
        json_data = self._WRC_RedBull_json(stub)
        shakedownTimes_df = DataFrame(json_data)

        if updateDB:
            self.dbfy(shakedownTimes_df, "shakedown_times", pk="shakedownTimeId")

        return shakedownTimes_df

    def _getStages(self, eventId, updateDB=False):
        if "stages_json" not in self.lastreferenced:
            self.stages_df  = DataFrame()
            self.stage_split_points_df  = DataFrame()
            self.stage_controls_df = DataFrame()
            self.lastreferenced["stages_json"] = {"key": None, "t": 0}

        if self.lastreferenced["stages_json"]["key"]!=eventId:
            self.lastreferenced["stages_json"]["key"] = eventId
            self.lastreferenced["stages_json"]["t"] = 0

        if (timeNow(typ="s") - self.lastreferenced["stages_json"]["t"]) < (
            self.ITINERARY_REFRESH_PERIOD
        ):
            return (self.stages_df, self.stage_split_points_df, self.stage_controls_df)
        else:
            self.lastreferenced["stages_json"]["t"] = timeNow(typ="s")

        if not eventId:
            return DataFrame(), DataFrame(), DataFrame()
        
        stub = f"events/{eventId}/stages.json"
        json_data = self._WRC_RedBull_json(stub)
        stages_df = DataFrame(json_data)

        if stages_df.empty:
            return (DataFrame(), DataFrame(), DataFrame())

        stage_split_points_df = (
            stages_df[["splitPoints"]].explode("splitPoints").reset_index(drop=True)
        )
        stage_controls_df = json_normalize(
            stages_df[["controls"]]
            .explode("controls")
            .reset_index(drop=True)["controls"]
        )
        stage_split_points_df = json_normalize(
            stages_df[["splitPoints"]]
            .explode("splitPoints")
            .reset_index(drop=True)["splitPoints"]
        )

        stages_df.drop(columns=["splitPoints", "controls"], inplace=True)

        if updateDB:
            self.dbfy(stages_df, "stage_info", pk="stageId")
            self.dbfy(stage_split_points_df, "split_points", pk="splitPointId")
            self.dbfy(stage_controls_df, "stage_controls", pk="controlId")

        self.stages_df =stages_df
        self.stage_split_points_df = stage_split_points_df
        self.stage_controls_df =stage_controls_df

        return stages_df, stage_split_points_df, stage_controls_df

    def _getStageTimes(self, eventId, rallyId, stageId=None, updateDB=False):
        if not eventId or not stageId or not rallyId:
            return DataFrame()
        
        stub = f"events/{eventId}/stages/{stageId}/stagetimes.json?rallyId={rallyId}"     
        json_data = self._WRC_RedBull_json(stub)
        stagetimes_df = DataFrame(json_data)
        if stagetimes_df.empty:
            return DataFrame()

        stagetimes_df["eventId"] = eventId
        stagetimes_df["rallyId"] = rallyId

        if updateDB:
            self.dbfy(stagetimes_df, "stage_times", pk="stageTimeId")

        return stagetimes_df

    def _getSplitTimes(self, eventId, rallyId, stageId=None, updateDB=False):
        stageId = stageId if stageId else self.stageId
        if not eventId or not stageId or not rallyId:
            return DataFrame()
        
        stub = f"events/{eventId}/stages/{stageId}/splittimes.json?rallyId={rallyId}"
        json_data = self._WRC_RedBull_json(stub)
        splitTimes_df = DataFrame(json_data)
        if splitTimes_df.empty:
            return DataFrame()

        splitTimes_df["stageId"] = stageId
        splitTimes_df["eventId"] = eventId
        splitTimes_df["rallyId"] = rallyId

        if updateDB:
            self.dbfy(splitTimes_df, "split_times", pk="splitPointTimeId")

        return splitTimes_df

    def _getStageOverallResults(
        self,
        eventId,
        rallyId,
        stageId,
        championshipId=None,
        by_championship=False,
        updateDB=False,
    ):
        """This is the overall result at the end of the stage."""
        # TO DO: if we select by championship, are these different?
        # If so, do we need to set championship and championship Pos cols, maybe in a new table?
        # The rallyId is optional? Or does it filter somehow?
        # We have no self.stageId here
        # stageId = stageId if stageId else self.stageId
        if not eventId or not stageId or not rallyId:
            return DataFrame()
        
        stub = f"events/{eventId}/stages/{stageId}/results.json?rallyId={rallyId}"
        if by_championship and championshipId:
            stub = stub + f"&championshipId={championshipId}"
        json_data = self._WRC_RedBull_json(stub)
        stageResults_df = DataFrame(json_data)
        if stageResults_df.empty:
            return DataFrame()

        stageResults_df["stageId"] = stageId
        stageResults_df["eventId"] = eventId
        stageResults_df["rallyId"] = rallyId

        if updateDB:
            self.dbfy(stageResults_df, "stage_overall", pk=("stageId", "entryId"))

        return stageResults_df

    def _getStageWinners(self, eventId, rallyId, updateDB=False):
        stub = f"events/{eventId}/rallies/{rallyId}/stagewinners.json"
        json_data = self._WRC_RedBull_json(stub)
        stagewinners_df = DataFrame(json_data)
        if stagewinners_df.empty:
            return DataFrame()

        stagewinners_df["eventId"] = eventId
        stagewinners_df["rallyId"] = rallyId
        if updateDB:
            self.dbfy(stagewinners_df, "stagewinners", pk="stageId")

        return stagewinners_df

    def _getRetirements(self, eventId, updateDB=False):
        if not eventId:
            return
        stub = f"events/{eventId}/retirements.json"
        json_data = self._WRC_RedBull_json(stub)
        retirements_df = DataFrame(json_data)
        if retirements_df.empty:
            return DataFrame()

        retirements_df["eventId"] = eventId
        if updateDB:
            self.dbfy(retirements_df, "retirements", pk="retirementId")
        return retirements_df

    def _getPenalties(self, eventId, updateDB=False):
        if not eventId:
            return
        stub = f"events/{eventId}/penalties.json"
        json_data = self._WRC_RedBull_json(stub)
        penalties_df = DataFrame(json_data)
        if penalties_df.empty:
            return DataFrame()

        penalties_df["eventId"] = eventId
        if updateDB:
            self.dbfy(penalties_df, "penalties", pk="penaltyId")
        return penalties_df


# The WRCTimingResultsAPIClientV2() constructs state on a season basis
class WRCTimingResultsAPIClientV2:
    CHAMPIONSHIP_CODES = {
        "World Rally Championship": "wrc",
        "European Rally Championship": "erc",
    }
    STAGE_PREFIX = "SS"
    SPLIT_PREFIX = "SP"
    SPLIT_FINAL = "FINAL"
    STAGE_FINAL = "FINAL"

    def __init__(
        self,
        year: int = date.today().year,
        championship: str = "wrc",  # wrc | erc
        category: str = "Drivers",
        group: str = "all",
        dbname: str = "wrcRbAPITiming.db",
        dbReadOnly: bool = False,
        newDB: bool = False,
        liveCatchup: bool = False,
        use_cache: bool = False,
        **cache_kwargs,
    ):
        self.liveCatchup = liveCatchup

        # Initialize the proxy with caching if requested
        if use_cache:
            self.proxy = create_cached_proxy(**cache_kwargs)
        else:
            self.proxy = CorsProxy()

        self.year = year
        # TO DO - more logic yet surrounding championship
        self.championship = championship  # Informal: WRC, WRC2, WRC3, JWRC
        self.championshipLookup = {}
        self.category = category
        self.seasonId = None
        self.rallyId = None
        self.eventId = None
        self.eventName = None
        self.championshipId = None
        self.championshipName = None
        self.itineraryId = None
        self.stageId = None
        self.stageName = None
        self.stageCode = None

        # DB setup
        self.dbReadOnly = dbReadOnly
        self.db_manager = DatabaseManager(dbname, newdb=newDB, dbReadOnly=dbReadOnly)

        self.api_client = APIClient(
            db_manager=self.db_manager, use_cache=use_cache, **cache_kwargs
        )

        # DB initialise
        if newDB:
            self.seedDB()

    def seedDB(self):
        # Populate the database with seasons info
        # self._getSeasons(updateDB=True)
        # Initialise the seasonId
        _seasons = self._getSeasons(updateDB=True)
        self.seasonId = self._getSeasonsSubQuery(
            _seasons, self.championship, self.year
        ).iloc[0]["seasonId"]
        # Update the season detail
        self._getSeasonDetail(updateDB=True)

    def initialise(self, year=None, championship=None):
        if year:
            self.year = year
        if championship:
            self.championship = championship

        self.getChampionships(updateDB=True)

        # In the initialise phase, take the opportunity to update the season table
        # whilst also setting self.seasonId
        self.setSeason()

        # Set self.championshipId
        self.setChampionship()

    def dbfy(self, *args, **kwargs):
        self.db_manager.dbfy(*args, **kwargs)

    def _WRC_RedBull_json(self, *args, **kwargs):
        return self.api_client._WRC_RedBull_json(*args, **kwargs)

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
        if not rebaseId or rebaseId == "ult" or idCol is None or rebaseCol is None:
            return times
        rebase_times = (
            times[rebaseCol] - times.loc[times[idCol] == rebaseId, rebaseCol].iloc[0]
        )
        return rebase_times.round(1)

    @staticmethod
    def rebaseManyTimes(
        times, rebaseId=None, idCol=None, rebaseCols=None, inplace=False
    ):
        """Rebase times in several specified columns relative to a particular vehicle."""
        if not inplace:
            if not rebaseId:
                return times
            times = times.copy()
        rebaseId = int(rebaseId) if rebaseId and rebaseId != "ult" else rebaseId
        if rebaseId:
            # Ensure rebaseCols is a list
            rebaseCols = [rebaseCols] if isinstance(rebaseCols, str) else rebaseCols

            # Fetch the reference values for the specified 'rebaseId'

            reference_values = times.loc[times[idCol] == rebaseId, rebaseCols]
            if reference_values.empty:
                return times

            reference_values = reference_values.iloc[0]

            # Subtract only the specified columns
            times[rebaseCols] = times[rebaseCols].subtract(reference_values).round(1)

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

    def _getSeasonsSubQuery(self, seasons_df, championship=None, year=None):
        if championship is not None:
            if championship.lower() == "wrc":
                championship = "World Rally Championship"
            elif championship.lower() == "erc":
                championship = "European Rally Championship"
            seasons_df = seasons_df[seasons_df["name"] == championship]

        if year is not None:
            seasons_df = seasons_df[seasons_df["year"] == year]
        return seasons_df

    def _getSeasons(self, *args, **kwargs):
        return self.api_client._getSeasons(*args, **kwargs)

    def getSeasons(self, championship=None, year=None, updateDB=False):
        if updateDB:
            self._getSeasons(updateDB=updateDB)
        # TO DO need to filter with championship
        _on_year = f"AND year={year}" if year is not None else ""
        q = f"""SELECT s.* FROM seasons AS s WHERE 1=1 {_on_year};"""
        seasons_df = self.db_manager.read_sql(q)

        return self._getSeasonsSubQuery(seasons_df, championship, year)

    # TO DO Need to check this e.g. for WRC and ERC
    # TO DO if we set from seasonId,
    def setSeason(self, seasonId=None):
        _seasons = self._getSeasons(updateDB=True)
        if seasonId:
            self.seasonId = seasonId
        else:
            self.seasonId = self._getSeasonsSubQuery(
                _seasons, self.championship, self.year
            ).iloc[0]["seasonId"]

    # This datafeed is partial at the start of the season
    # and needs to be regularly updated
    def _getSeasonDetail(self, *args, **kwargs):
        kwargs["seasonId"] = self.seasonId
        return self.api_client._getSeasonDetail(*args, **kwargs)

    def getSeasonRounds(self, seasonId=None, on_season=True, updateDB=False):
        if updateDB:
            self._getSeasonDetail(updateDB=updateDB)
        if on_season:
            seasonId = seasonId if seasonId else self.seasonId
        seasonId_ = f"""AND seasonId={seasonId}""" if seasonId else ""

        q = f"""SELECT sr.* FROM season_rounds AS sr WHERE 1=1 {seasonId_};"""
        seasonRounds_df = self.db_manager.read_sql(q)
        if seasonRounds_df.empty:
            self._getSeasonDetail(updateDB=True)
            seasonRounds_df = self.db_manager.read_sql(q)

        return seasonRounds_df

    def _getChampionshipOverallResults(self, *args, **kwargs):
        kwargs["championshipId"] = self.championshipId
        kwargs["seasonId"] = self.seasonId
        return self.api_client._getChampionshipOverallResults(*args, **kwargs)

    def getChampionshipOverall(
        self,
        championshipId=None,
        eventId=None,
        on_event=False,
        on_championship=True,
        latest=True,
        raw=True,
        updateDB=False,
    ):
        if updateDB:
            self._getChampionshipOverallResults(updateDB=updateDB)
        if championshipId or on_championship:
            if on_championship and self.championshipId:
                championshipId = self.championshipId
            championship_ = f"""AND co.championshipId={championshipId}"""
        else:
            championship_ = ""
        # TO DO some logic for latest completed or running event.

        # TO DO the Teams championship is broken? What do we get team name on?

        if on_event or eventId:
            # TO DO what is best logic if both are set?
            if on_event and self.eventId:
                eventId = self.eventId
            event_ = f"AND co.eventId={eventId}"
        else:
            event_ = ""
        # HACK we are assuming eventId is incremental by round; TO DO sort by date
        if latest:
            latest_ = f"""AND co.eventId = (
    SELECT MAX(eventId)
    FROM championship_overall AS co
    WHERE 1=1 {championship_}
) """
        else:
            latest_ = ""
        if raw:
            q = f"""SELECT co.* FROM championship_overall AS co WHERE 1=1 {championship_} {event_} {latest_};"""
        else:
            _championship_results_join = f"INNER JOIN championship_results AS cr ON cr.championshipEntryId=co.championshipEntryId AND cr.eventId=co.eventId"
            _championship_entry_join = f"INNER JOIN championship_entries AS ce ON co.championshipEntryId=ce.championshipEntryId"
            q = f"""SELECT co.*, ce.LastName, ce.Name as Team, ce.Manufacturer, ce.TyreManufacturer, cr.entryId FROM championship_overall AS co {_championship_entry_join} {_championship_results_join} WHERE 1=1 {championship_} {event_} {latest_};"""

        championshipEntryResultsOverall_df = self.db_manager.read_sql(q)

        if not raw:
            if "for Manufacturers" in self.championshipName:
                championshipEntryResultsOverall_df.drop(
                    columns=["TyreManufacturer", "LastName", "Team"], inplace=True
                )
            elif "Drivers" in self.championshipName:
                championshipEntryResultsOverall_df.drop(
                    columns=["Manufacturer", "TyreManufacturer", "Team"], inplace=True
                )
            elif "Tyre" in self.championshipName:
                championshipEntryResultsOverall_df.drop(
                    columns=["Manufacturer", "LastName", "Team"], inplace=True
                )
            elif "Teams" in self.championshipName:
                championshipEntryResultsOverall_df.drop(
                    columns=["Manufacturer", "TyreManufacturer", "LastName"],
                    inplace=True,
                )
        return championshipEntryResultsOverall_df

    def getChampionshipByRound(
        self,
        championshipId=None,
        eventId=None,
        on_event=False,
        on_championship=False,
        on_season=False,
        raw=True,
        updateDB=False,
    ):
        if updateDB:
            self._getChampionshipOverallResults(updateDB=updateDB)
        if championshipId or on_championship:
            if on_championship and self.championshipId:
                championshipId = self.championshipId
            championship_ = f"""AND cr.championshipId={championshipId}"""
        else:
            championship_ = ""
        # TO DO some logic for latest completed or running event.
        if on_event or eventId:
            # TO DO what is best logic if both are set?
            if on_event and self.eventId:
                eventId = self.eventId
            event_ = f"AND cr.eventId={eventId}"
        else:
            event_ = ""
        on_season_ = f"""AND seasonId={self.seasonId}""" if on_season else ""
        if raw:
            q = f"""SELECT cr.* FROM championship_results AS cr WHERE 1=1 {championship_} {event_} {on_season_};"""

        else:
            _championship_entry_join = f"INNER JOIN championship_entries AS ce ON cr.championshipEntryId=ce.championshipEntryId"
            _championship_rounds_join = f"INNER JOIN championship_rounds_detail AS chd ON cr.eventId=chd.eventId"
            q = f"""SELECT cr.*, ce.LastName, ce.Manufacturer, ce.tyreManufacturer, ce.Name as Team, chd.name AS eventName, chd.startDate, chd.Round, chd.surfaces, chd.finishDate FROM championship_results AS cr {_championship_entry_join} {_championship_rounds_join} WHERE 1=1 {championship_} {event_} {on_season_} ORDER BY chd.startDate, position ASC;"""

        championshipEntryResultsByRound_df = self.db_manager.read_sql(q)
        if not raw:
            if "for Manufacturers" in self.championshipName:
                championshipEntryResultsByRound_df.drop(
                    columns=["TyreManufacturer", "LastName", "Team"], inplace=True
                )
            elif "Drivers" in self.championshipName:
                championshipEntryResultsByRound_df.drop(
                    columns=["Manufacturer", "TyreManufacturer", "Team"], inplace=True
                )
            elif "Tyre" in self.championshipName:
                championshipEntryResultsByRound_df.drop(
                    columns=["Manufacturer", "LastName", "Team"], inplace=True
                )
            elif "Teams" in self.championshipName:
                championshipEntryResultsByRound_df.drop(
                    columns=["Manufacturer", "TyreManufacturer", "LastName"],
                    inplace=True,
                )

        return championshipEntryResultsByRound_df

    def _getChampionshipDetail(self, *args, **kwargs):
        kwargs["year"] = self.year
        kwargs["championship"] = self.championship
        kwargs["championshipId"] = self.championshipId
        kwargs["seasonId"] = self.seasonId
        return self.api_client._getChampionshipDetail(*args, **kwargs)

    def getChampionshipRounds(self, year=None, raw=True, updateDB=False):
        if updateDB:
            self._getChampionshipDetail()

        year_ = f"""AND cd.year={year}""" if year else ""
        if raw:
            q = f"""SELECT cd.* FROM championship_rounds_detail AS cd WHERE 1=1 {year_};"""

        championshipRounds_df = self.db_manager.read_sql(q)

        return championshipRounds_df

    def getChampionshipEntries(
        self, championshipId=None, on_championship=False, raw=True, updateDB=False
    ):
        if updateDB:
            self._getChampionshipDetail()
        if championshipId or on_championship:
            if on_championship and self.championshipId:
                championshipId = self.championshipId
            championship_ = f"""ce.championshipId={championshipId}"""
        else:
            championship_ = ""
        if raw:
            q = f"""SELECT ce.* FROM championship_entries AS ce WHERE 1=1 {championship_};"""
        championshipEntries_df = self.db_manager.read_sql(q)

        return championshipEntries_df

    def getChampionshipCountries(self, raw=True, updateDB=False):
        if updateDB:
            self._getChampionshipDetail()

        if raw:
            q = "SELECT cc.* FROM championship_countries AS cc;"
        championships_df = self.db_manager.read_sql(q)

        return championships_df

    def getChampionships(self, seasonId=None, on_season=True, raw=True, updateDB=False):
        if updateDB:
            self._getSeasonDetail(updateDB=updateDB)
        if on_season and not seasonId:
            seasonId = self.seasonId if self.seasonId else seasonId
        seasonId_ = f"AND cl.seasonId={seasonId} " if seasonId else ""
        if raw:
            q = f"""SELECT cl.* FROM championship_lookup AS cl WHERE 1=1 {seasonId_};"""
        championships_df = self.db_manager.read_sql(q)

        return championships_df

    def _getEventGroups(self, *args, **kwargs):
        kwargs["eventId"] = self.eventId
        return self.api_client._getEventGroups(*args, **kwargs)

    def _getEventItineraries(self, *args, **kwargs):
        kwargs["eventId"] = self.eventId
        kwargs["itineraryId"] = self.itineraryId
        return self.api_client._getEventItineraries(*args, **kwargs)

    def getItineraryLegs(self, eventId=None, updateDB=False):
        if updateDB or self.liveCatchup:
            updateDB = updateDB or self.isRallyLive()
            self._getEventItineraries(updateDB=updateDB)

        eventId_ = f"AND eventId={int(eventId)}" if eventId else ""
        q = f"""SELECT il.* FROM itinerary_legs AS il WHERE 1=1 {eventId_};"""

        itineraryLegs_df = self.db_manager.read_sql(q)

        return itineraryLegs_df

    def getItineraryStages(
        self, eventId=None, itineraryLegId=None, itinerarySectionId=None, latest=True, updateDB=False
    ):
        if updateDB or self.liveCatchup:
            updateDB = updateDB or self.isRallyLive()
            self._getEventItineraries(updateDB=updateDB)

        if not eventId and not itineraryLegId and not itinerarySectionId:
            q = "SELECT * FROM itinerary_stages;"
        elif itinerarySectionId:
            q = f"SELECT * FROM itinerary_stages WHERE itinerarySectionId={int(itinerarySectionId)};"
        elif itineraryLegId:
            q = f"SELECT * FROM itinerary_stages WHERE itineraryLegId={int(itineraryLegId)};"
        elif eventId:
            q = f"SELECT * FROM itinerary_stages WHERE eventId={int(eventId)};"

        itineraryStages_df = self.db_manager.read_sql(q)
        if latest:
            # Get latest itinerary
            itineraryStages_df.drop_duplicates(subset=["code"], keep="last", inplace=True)
            itineraryStages_df.reset_index(drop=True, inplace=True)

        return itineraryStages_df

    def getItinerarySections(self, eventId=None, itineraryLegId=None, updateDB=False):
        if updateDB or self.liveCatchup:
            updateDB = updateDB or self.isRallyLive()
            self._getEventItineraries(updateDB=updateDB)

        if not eventId and not itineraryLegId:
            q = "SELECT * FROM itinerary_sections;"
        elif itineraryLegId:
            q = f"SELECT * FROM itinerary_sections WHERE itineraryLegId={int(itineraryLegId)};"
        elif eventId:
            q = f"SELECT * FROM itinerary_sections WHERE eventId={int(eventId)};"

        itinerarySections_df = self.db_manager.read_sql(q)

        return itinerarySections_df

    def getItineraryControls(
        self, eventId=None, itineraryLegId=None, itinerarySectionId=None, updateDB=False
    ):
        if updateDB or self.liveCatchup:
            updateDB = updateDB or self.isRallyLive()
            self._getEventItineraries(updateDB=updateDB)

        if not eventId and not itineraryLegId and not itinerarySectionId:
            q = "SELECT * FROM itinerary_controls;"
        elif itinerarySectionId:
            q = f"SELECT * FROM itinerary_controls WHERE itinerarySectionId={int(itinerarySectionId)};"
        elif itineraryLegId:
            q = f"SELECT * FROM itinerary_controls WHERE itineraryLegId={int(itineraryLegId)};"
        elif eventId:
            q = f"SELECT * FROM itinerary_controls WHERE eventId={int(eventId)};"

        itineraryControls_df = self.db_manager.read_sql(q)
        itineraryControls_df["startTime"] = to_datetime(
            itineraryControls_df["firstCarDueDateTime"]
        )

        # Extract day of week name
        itineraryControls_df["day"] = itineraryControls_df["startTime"].dt.strftime(
            "%A"
        )

        return itineraryControls_df

    def _getStartLists(self, *args, **kwargs):
        kwargs["eventId"] = self.eventId
        return self.api_client._getStartLists(*args, **kwargs)

    def getStartList(self, eventId=None, startListId=None, raw=True, updateDB=False):
        # TO DO  - offer e.g. a day instead of startListId

        if updateDB and startListId:
            self._getStartLists(startListId=startListId)

        eventId = eventId if eventId else self.eventId
        _on_event = f"AND sl.eventId={eventId}" if eventId else ""
        if raw:
            q = f"SELECT sl.* FROM startlists AS sl WHERE 1=1 {_on_event}"
            if startListId:
                q = f"{q} AND startListId={startListId}"
            q = f"{q};"
        else:
            _entry_join = f"INNER JOIN entries AS e ON sl.entryId=e.entryId"
            _driver_join = f"INNER JOIN entries_drivers AS d ON e.driverId=d.personId"
            _codriver_join = (
                f"INNER JOIN entries_codrivers AS cd ON e.codriverId=cd.personId"
            )
            _manufacturer_join = (
                f"INNER JOIN manufacturers AS m ON e.manufacturerId=m.manufacturerId"
            )
            _entrants_join = f"INNER JOIN entrants AS n ON e.entrantId=n.entrantId"
            q = f"SELECT d.code AS driverCode, d.fullName AS driverName, cd.fullName AS codriverName, m.name AS manufacturerName, n.name AS entrantName, e.identifier AS carNo, e.vehicleModel, e.priority, e.eligibility, sl.* FROM startlists AS sl {_entry_join} {_driver_join} {_codriver_join} {_manufacturer_join} {_entrants_join} WHERE 1=1 {_on_event}"
            if startListId:
                q = f"{q} AND startListId={startListId};"
            q = f"{q} ORDER BY sl.[order] ASC;"
            # We can also escape order (reserved word as column name)
            # using eg sl.[order] or sl."order" or  sl.`order`

        startlist_df = self.db_manager.read_sql(q)

        return startlist_df

    def _getEntries(self, *args, **kwargs):
        kwargs["eventId"] = self.eventId
        kwargs["rallyId"] = self.rallyId

        return self.api_client._getEntries(*args, **kwargs)

    def getEntries(self, on_event=True, updateDB=False):
        if updateDB:
            self._getEntries(updateDB=updateDB)
        _on_event = (
            f"AND eventId={self.eventId} AND rallyId={self.rallyId}"
            if on_event and self.eventId and self.rallyId
            else ""
        )
        q = f"SELECT e.* FROM entries AS e WHERE 1=1 {_on_event};"
        entries_df = self.db_manager.read_sql(q)

        return entries_df

    def getDrivers(
        self, on_event=True, by_championship=False, priority=None, updateDB=False
    ):
        if updateDB:
            self._getEntries(updateDB=updateDB)
        _on_event = (
            f"AND e.eventId={self.eventId} AND e.rallyId={self.rallyId}"
            if on_event and self.eventId and self.rallyId
            else ""
        )
        _entry_join = f"INNER JOIN entries AS e ON d.personId=e.driverId"
        # TO DO - handle priority
        priority = None if priority == "P0" else priority
        priority_ = f"""AND e.priority LIKE "%{priority}" """ if priority else ""

        # I'm not sure this is right? Is the championshipId persistent over years?
        # For WRC, use priority (P1) and / or eligibility includes M? Or groupId for Rally1?
        # We want people registered in the championship when they are competing in an event.
        # We can perhaps get IDs by using the championshipId filtered results.json?
        by_championship_ = ""
        if by_championship and self.championshipId:
            by_championship_ = f"AND e.driverId IN (SELECT personId FROM championship_entries WHERE championshipId={self.championshipId})"

        q = f"SELECT d.* FROM entries_drivers AS d {_entry_join} WHERE 1=1 {_on_event} {priority_} {by_championship_};"
        drivers_df = self.db_manager.read_sql(q)

        return drivers_df

    def getCoDrivers(self, on_event=True, updateDB=False):
        if updateDB:
            self._getEntries(updateDB=updateDB)
        _on_event = (
            f"INNER JOIN entries AS e ON cd.personId=e.codriverId WHERE e.eventId={self.eventId} AND e.rallyId={self.rallyId}"
            if on_event and self.eventId and self.rallyId
            else ""
        )

        q = f"SELECT cd.* FROM entries_codrivers AS cd {_on_event};"
        codrivers_df = self.db_manager.read_sql(q)

        return codrivers_df

    def getManufacturers(self, on_event=True, updateDB=False):
        if updateDB:
            self._getEntries(updateDB=updateDB)
        _on_event = (
            f"INNER JOIN entries AS e ON m.manufacturerId=e.manufacturerId WHERE e.eventId={self.eventId} AND e.rallyId={self.rallyId}"
            if on_event and self.eventId and self.rallyId
            else ""
        )

        q = f"SELECT DISTINCT m.* FROM manufacturers AS m {_on_event};"
        manufacturers_df = self.db_manager.read_sql(q)

        return manufacturers_df

    def getEntrants(self, on_event=True, updateDB=False):
        if updateDB:
            self._getEntries(updateDB=updateDB)
        _on_event = (
            f"INNER JOIN entries AS e ON n.entrantId=e.entrantId WHERE e.eventId={self.eventId} AND e.rallyId={self.rallyId}"
            if on_event and self.eventId and self.rallyId
            else ""
        )

        q = f"SELECT n.entrantId, n.name FROM entrants AS n {_on_event};"
        entrants_df = self.db_manager.read_sql(q)

        return entrants_df

    def getGroups(self, on_event=True, updateDB=False):
        if updateDB:
            self._getEntries(updateDB=updateDB)
        _on_event = f"" if on_event else ""

        q = f"SELECT * FROM groups;"
        entryGroups_df = self.db_manager.read_sql(q)

        return entryGroups_df

    def _getEventShakeDownTimes(self, eventId=None, *args, **kwargs):
        eventId = eventId if eventId else self.eventId
        kwargs["eventId"] = eventId

        return self.api_client._getEventShakeDownTimes(*args, **kwargs)

    def getEventShakeDownTimes(
        self,
        eventId=None,
        on_event=True,
        priority=None,
        run=1,
        raw=True,
        updateDB=False,
    ):
        # ERCC: Shakedown run=1; Qualifying stage/QS run=2
        if updateDB:
            self._getEventShakeDownTimes(run=run, updateDB=updateDB)
        if on_event and self.eventId and not eventId:
            eventId = self.eventId

        eventId_ = f"""AND sh.eventId={self.eventId}""" if eventId else ""

        if raw:
            sql = f"SELECT sh.* FROM shakedown_times AS sh WHERE 1=1 {eventId_};"
        else:
            _entry_join = f"INNER JOIN entries AS e ON sh.entryId=e.entryId"
            _driver_join = f"INNER JOIN entries_drivers AS d ON e.driverId=d.personId"
            _codriver_join = (
                f"INNER JOIN entries_codrivers AS cd ON e.codriverId=cd.personId"
            )
            _manufacturer_join = (
                f"INNER JOIN manufacturers AS m ON e.manufacturerId=m.manufacturerId"
            )
            _entrants_join = f"INNER JOIN entrants AS n ON e.entrantId=n.entrantId"
            priority = None if priority == "P0" else priority
            priority_ = f"""AND e.priority LIKE "%{priority}" """ if priority else ""
            sql = f"SELECT d.code AS driverCode, d.fullName AS driverName, cd.fullName AS codriverName, m.name AS manufacturerName, n.name AS entrantName, e.vehicleModel, e.identifier AS carNo, sh.* FROM shakedown_times AS sh {_entry_join} {_driver_join} {_codriver_join} {_manufacturer_join} {_entrants_join} WHERE 1=1 {eventId_} {priority_};"

            r = self.db_manager.read_sql(sql)
            # Hack to poll API if empty
            if r.empty:
                self._getEventShakeDownTimes(eventId=eventId, updateDB=True)
                r = self.db_manager.read_sql(sql)

        return r

    def _updateCompletedEventTableStatus(self, eventId, table):
        if table and isinstance(table, str):
            table = [table]
        for t in table:
            self.dbfy(
                DataFrame([{"eventId": int(eventId), "tableType": t}]),
                "meta_completed_event_tables",
                pk=["tableType", "eventId"],
            )

    def checkCompletedEventTableStatus(self, eventId, table):
        """Return a True flag if we have stored this table."""
        sql = f"""SELECT * FROM meta_completed_event_tables WHERE eventId={int(eventId)} AND tableType="{table}";"""
        _result = self.query(sql=sql)
        status = not _result.empty
        return status

    def _getStages(self, *args, **kwargs):
        eventId = self.eventId
        kwargs["eventId"] = eventId
        # if the event is finished and we have this already
        stage_info_completed = self.checkCompletedEventTableStatus(
            eventId, "stage_info"
        )
        split_points_completed = self.checkCompletedEventTableStatus(
            eventId, "split_points"
        )
        stage_controlsd_completed = self.checkCompletedEventTableStatus(
            eventId, "stage_controls"
        )
        if (
            stage_info_completed
            and split_points_completed
            and stage_controlsd_completed
        ):
            return self.getStageInfo(legacyCheck=True)
        stages_df, stage_split_points_df, stage_controls_df = (
            self.api_client._getStages(*args, **kwargs)
        )
        if not stages_df.empty:
            # Check the stages table as it currently stands
            # stages_info = self.getStageInfo(legacyCheck=True)["status"].str.lower().unique()
            stages_info = stages_df["status"].str.lower().unique()
            if "torun" not in stages_info and "running" not in stages_info:
                self._updateCompletedEventTableStatus(
                    eventId, ["stage_info", "split_points", "stage_controls"]
                )
        return stages_df, stage_split_points_df, stage_controls_df

    def getRepeatedStages(self, on_event=True, eventId=None):
        def extract_stage_repetition(text):
            match = re.search(r"\s(\d+)$", text)
            if match:
                return int(match.group(1))
            else:
                return 0

        stages_df = self.getStageInfo(on_event=on_event, eventId=eventId)
        stages_df["base"] = stages_df["name"].apply(lambda x: re.sub(r"\s\d+$", "", x))
        # The "repeat" column identifies repeated stages based on trailing "stage name 1" etc
        stages_df["run"] = stages_df["name"].apply(extract_stage_repetition)
        repeated_stages = stages_df.groupby("base")["stageCode"].apply(list).to_dict()
        stages_df["runs"] = stages_df["base"].map(repeated_stages)

        # Get the base stage name
        stages_df["_name"] = stages_df["name"].apply(lambda x: re.sub(r"\s\d+$", "", x))

    def getStageInfo(
        self,
        on_event=True,
        eventId=None,
        itineraryLegId=None,
        itinerarySectionId=None,
        stageId=None,
        stage_code=None,
        stage_name=None,
        completed=False,
        running=False,
        last=False,
        raw=True,
        updateDB=False,
        noLiveCheck=False,
        legacyCheck=False,
    ):
        if legacyCheck:
            pass
        elif updateDB or self.liveCatchup:
            # What is the sense of noLiveCheck?
            if not noLiveCheck:
                updateDB = updateDB or self.isStageLive(
                    stageId=stageId, stage_code=stage_code
                )
            self._getStages(updateDB=updateDB)

        on_event_ = f"""AND si.eventId={self.eventId}""" if on_event else ""
        on_event_ = f"""AND si.eventId={eventId}""" if eventId else on_event_
        on_leg_ = (
            f"""AND it_l.itineraryLegId={itineraryLegId}""" if itineraryLegId else ""
        )
        on_section_ = (
            f"""AND it_se.itinerarySectionId={itinerarySectionId}"""
            if itinerarySectionId
            else ""
        )
        on_stage_ = f"""AND si.stageId={stageId}""" if stageId else ""
        on_stage_name_ = f"""AND si.name LIKE "{stage_name}%" """ if stage_name else ""

        if stage_code and not isinstance(stage_code, list):
            stage_code = [stage_code]
        else:
            stage_code = []

        # completed_ = """AND si.status="Completed" """ if completed else ""
        running_ = ', "Running"' if running else ""
        completed_ = (
            f"""AND si.status IN ("Completed", "Cancelled", "Interrupted" {running_}) """
            if completed
            else ""
        )
        last_ = (
            f"""AND si.number = (SELECT MAX(number) FROM stage_info WHERE 1=1 {on_event_} {on_stage_})"""
            if last
            else ""
        )

        if raw:
            q = f"SELECT * FROM stage_info AS si WHERE 1=1 {on_event_} {on_stage_} {completed_} {last_};"
        else:
            _itinerary_stages_join = (
                f"INNER JOIN itinerary_stages AS it_st ON it_st.stageId=si.stageId"
            )
            _itinerary_sections_join = f"INNER JOIN itinerary_sections AS it_se ON it_se.itinerarySectionId=it_st.itinerarySectionId"
            _itinerary_legs_join = f"INNER JOIN itinerary_legs AS it_l ON it_l.itineraryLegId=it_st.itineraryLegId"
            q = f"SELECT it_se.name AS sectionName, it_l.name AS day, si.* FROM stage_info AS si {_itinerary_stages_join} {_itinerary_sections_join} {_itinerary_legs_join} WHERE 1=1 {on_event_} {on_leg_} {on_section_} {on_stage_} {on_stage_name_} {completed_} {last_};"

        stages_df = self.db_manager.read_sql(q)

        # TO DO move this into the SQL query
        if stage_code:
            # Try to be flexible with stage codes as code or stageID
            if isinstance(stage_code[0], int):
                stages_df = stages_df[stages_df["stageId"].isin(stage_code)]
            elif isinstance(stage_code[0], str) and stage_code[0].startswith("SS"):
                stages_df = stages_df[stages_df["code"].isin(stage_code)]

        return stages_df

    def getStageSplitPoints(
        self, stageId=None, raw=False, extended=False, updateDB=False
    ):
        # TO DO - also support eventId?
        # TO DO also support raw
        if updateDB:
            self._getStages(updateDB=updateDB)
        _on_stage = f"AND stageId={stageId}" if stageId else ""
        q = f"""SELECT * FROM split_points WHERE 1=1 {_on_stage};"""

        stage_split_points_df = self.db_manager.read_sql(q)

        if raw and not extended:
            return stage_split_points_df

        # Optionally add in the final stage distance
        final_ = len(stage_split_points_df) + 1
        if extended:
            stage_info_ = self.getStageInfo(stageId=stageId)
            if not stage_info_.empty:
                final_distance = stage_info_["distance"].iloc[0]
                final_row = DataFrame(
                    [{"stageId": stageId, "number": final_, "distance": final_distance}]
                )
                stage_split_points_df = concat(
                    [stage_split_points_df, final_row], ignore_index=True
                )

        stage_split_points_df = stage_split_points_df.sort_values("number")
        stage_split_points_df["distance_"] = (
            stage_split_points_df["distance"]
            .diff()
            .fillna(stage_split_points_df["distance"])
            .round(2)
        )

        stage_split_points_df["name"] = self.SPLIT_PREFIX + stage_split_points_df[
            "number"
        ].astype(str)

        if extended:
            stage_split_points_df["name"] = stage_split_points_df["name"].replace(
                f"{self.SPLIT_PREFIX}{final_}", "FINAL"
            )

        return stage_split_points_df

    def getStageControls(self, updateDB=False, raw=True):
        if updateDB:
            self._getStages(updateDB=updateDB)

        if raw:
            q = "SELECT sc.* FROM stage_controls AS sc;"
        # TO DO a query that gives a "pretty" result

        stage_controls_df = self.db_manager.read_sql(q)

        return stage_controls_df

    def _getEvent(self, *args, **kwargs):
        """This also sets self.rallyId, self.itineraryId"""
        kwargs["eventId"] = self.eventId

        eventData_df, eventRallies_df, eventClasses_df = self.api_client._getEvent(
            *args, **kwargs
        )

        _event_df = eventRallies_df[eventRallies_df["isMain"] == True].iloc[0]
        self.rallyId = int(_event_df["rallyId"])
        self.itineraryId = int(_event_df["itineraryId"])

        return eventData_df, eventRallies_df, eventClasses_df

    def _setEvent(self, r, updateDB=True):
        if not r.empty:
            self.eventId = int(r.iloc[0]["eventId"])
            self.eventName = r.iloc[0]["name"]
            # Get the event info
            self._getEvent(updateDB=updateDB)
            self._getStages(updateDB=updateDB)
            self._getEntries(updateDB=updateDB)
            self._getEventItineraries(updateDB=updateDB)

    def setEventLatest(self, seasonId=None, updateDB=True):
        seasonId = self.seasonId if not seasonId and self.seasonId else seasonId
        # TO DO can we be more agresseive guessing the seasonId?
        if not seasonId:
            return

        q = f"SELECT eventId, name FROM season_rounds WHERE startDate < {dateNow()} AND seasonId={seasonId} ORDER BY startDate DESC LIMIT 1;"
        r = self.db_manager.read_sql(q)
        # HACK TO DO this is a fudge
        self._setEvent(r, updateDB)

    def setEventById(self, eventId=None, updateDB=True):
        if not eventId:
            return
        # Do this as a fuzzy search?
        q = f'SELECT eventId, name FROM season_rounds WHERE eventId="{eventId}";'
        r = self.db_manager.read_sql(q)
        # HACK TO DO this is a fudge
        self._setEvent(r, updateDB)

    def setEventByName(self, name=None, updateDB=True):
        if not name:
            return
        # Do this as a fuzzy search?
        q = f'SELECT eventId, name FROM season_rounds WHERE name="{name}";'
        r = self.db_manager.read_sql(q)
        # HACK TO DO this is a fudge
        self._setEvent(r, updateDB)

    def setChampionship(self, championshipId=None):
        championships_df = self.getChampionships()
        if championshipId is None:
            # HACK
            championshipId = championships_df.loc[
                championships_df["name"].str.contains(
                    "for Drivers", case=True, na=False
                )
            ]["championshipId"].iloc[0]
        # TO DO the name setting below is broken:
        # - need to update from data;
        # - need to account for ERC
        championships_df_ = championships_df[
            championships_df["championshipId"] == int(championshipId)
        ]["name"]
        if championships_df_.empty:
            print("SNAFU — can't set championship")
            return
        self.championshipName = championships_df_.iloc[0]
        # TO DO - defend against brokenness here
        if championshipId:
            self.championshipId = championshipId
        else:
            self.championshipId = int(
                championships_df[
                    championships_df["name"] == self.championshipName
                ].iloc[0]["championshipId"]
            )
        if self.championshipId:
            self._getChampionshipOverallResults(updateDB=True)
            self._getChampionshipDetail(updateDB=True)

    def _setStage(self, r, updateDB=False):
        # HACK TO DO this is a fudge
        if not r.empty:
            r = r.iloc[0]
            self.stageId = int(r["stageId"])
            self.stageName = r["name"]
            self.stageCode = r["code"]
            # Get the event info
            self._getStages(updateDB=updateDB)

    def setStageById(self, stageId=None, updateDB=True):
        # TO DO - we need to clobber the update if the table is complete
        self._getStages(updateDB=updateDB)
        if not stageId:
            return
        # Do this as a fuzzy search?
        q = f'SELECT stageId, name, code FROM stage_info WHERE stageId="{int(stageId)}";'
        r = self.db_manager.read_sql(q)
        self._setStage(r, updateDB=updateDB)

    def setStageByCode(self, stageCode=None, updateDB=True):
        # TO DO - we need to clobber the update if the table is complete
        self._getStages(updateDB=updateDB)
        if not stageCode:
            return
        # Do this as a fuzzy search?
        q = f'SELECT stageId, name, code FROM stage_info WHERE code="{stageCode}";'
        r = self.db_manager.read_sql(q)
        self._setStage(r, updateDB=updateDB)

    # TO DO a way of setting self.controlId; also need strategies for invalidating Ids
    def _getControlTimes(self, *args, stageId=None, **kwargs):
        stageId = stageId if stageId else self.stageId
        kwargs["eventId"] = self.eventId
        kwargs["controlId"] = self.controlId

        return self.api_client._getControlTimes(*args, **kwargs)

    def _getStageTimes(self, *args, stageId=None, **kwargs):
        stageId = stageId if stageId else self.stageId
        kwargs["eventId"] = self.eventId
        kwargs["rallyId"] = self.rallyId
        kwargs["stageId"] = stageId

        return self.api_client._getStageTimes(*args, **kwargs)

    def getLiveStages(self, on_event=True):
        """Get a list of running stages."""
        # Stage statuses are: = "completed", "interrupted", "cancelled", "running", "to run".
        stage_info = self.getStageInfo(
            updateDB=False, noLiveCheck=True, on_event=on_event
        )
        stage_info = stage_info[
            stage_info["status"].str.lower().isin({"running", "interrupted"})
        ]
        return stage_info

    def isStageLive(self, stageId=None, stage_code=None):
        """Flag that shows a stage is live, so we need to keep updating stage related data."""
        stageId = self.stageId if not stageId and not stage_code else stageId
        # TO DO handle stagecode
        if stageId or stage_code:
            stage_info = self.getStageInfo(
                stageId=stageId, stage_code=stage_code, updateDB=False, noLiveCheck=True
            )
            if stage_info.empty and self.isRallyLive():
                updateDB = True
            elif not stage_info.empty and (
                self.isRallyLive()
                and stage_info.iloc[0]["status"].lower()
                not in ["completed", "cancelled"]  # ["running", "torun"]
            ):
                updateDB = True
            else:
                updateDB = False

            if updateDB:
                stage_info = self.getStageInfo(
                    stageId=stageId,
                    stage_code=stage_code,
                    updateDB=updateDB,
                    noLiveCheck=True,
                )
            if not stage_info.empty:
                stage_info = stage_info.iloc[0]
                status = stage_info["status"].lower()
                return status == "running"
        # ToRun, completed, cancelled; running??
        return False

    def isRallyInDate(self):
        """A quicker test than .isRallyLive(), just checks by date."""
        season = self.getSeasonRounds()
        event = season[season["eventId"] == self.eventId]
        if not event.empty:
            event = event.iloc[0].to_dict()
            is_date_in_range(datetime.now(), event)
            return is_date_in_range(datetime.now(), event)
        return False

    def isRallyLive(self):
        """Flag to show that rally is live."""
        season = self.getSeasonRounds()
        event_ = season[season["eventId"] == self.eventId]
        if not event_.empty:
            event = event_.iloc[0].to_dict()
            if is_date_in_range(datetime.now(), event):
                # TO DO - various itinerary controls report status
                # itinerarySections: status: ToRun, Running
                # itineraryControls: status: ToRun
                # itineraryStages: status: ToRun
                # itineraryLeg: status: ToRun
                _, _, _, itinerary_stages = self._getEventItineraries(updateDB=False)
                if not itinerary_stages.empty:
                    itinerary_stages["status"] = itinerary_stages["status"].str.lower()
                    _, _, _, itinerary_stages = self._getEventItineraries(updateDB=True)
                    if any(itinerary_stages["status"].isin(["running", "torun"])):
                        _, _, _, itinerary_stages = self._getEventItineraries(
                            updateDB=True
                        )
                if itinerary_stages.empty:
                    return False

                itinerary_stages["status"] = itinerary_stages["status"].str.lower()

                # TO DO also put date bounds on this
                return "running" in itinerary_stages["status"].tolist()

        return False

    def getStageTimes(
        self,
        stageId=None,
        completed=False,
        running=False,
        omitDNS=True,
        priority=None,
        rebaseToCategory=True,
        on_event=True,
        raw=True,
        updateDB=False,
    ):
        # The assumption below is for on_event
        stageIds = (
            self.getCompletedStages(
                stageId=stageId, running=running, completed=completed, on_event=on_event
            )
            if completed
            else {}  # TO DO map for the default stageId
        )
        if updateDB or self.liveCatchup:
            if completed or running:
                # Check availability of every stage required
                for stageId in stageIds:
                    updateDB = updateDB or self.isStageLive(stageId=stageId)
                    if not self.handleStageCompleted(stageId, tables="stage_times"):
                        self._getStageTimes(stageId=stageId, updateDB=updateDB)
            else:
                logger.debug(
                    f"getStageTimes updateDB: {updateDB}, liveCatchup: {self.liveCatchup}"
                )
                updateDB = updateDB or self.isStageLive(stageId=stageId)
                self._getStageTimes(stageId=stageId, updateDB=updateDB)

        stageId = stageId if stageId else self.stageId
        if stageId and self.eventId and self.rallyId:
            _entry_join = f"INNER JOIN entries AS e ON st.entryId=e.entryId"
            priority = None if priority == "P0" else priority
            on_event_ = f"AND st.eventId={self.eventId} AND st.rallyId={self.rallyId}"
            if completed and stageIds:
                stage_ids_str = ",".join(str(sid) for sid in stageIds)
                on_stage_ = f"AND st.stageId IN ({stage_ids_str})"
            else:
                on_stage_ = f"AND st.stageId={stageId}" if stageId else ""
            priority_ = f"""AND e.priority LIKE "%{priority}" """ if priority else ""
            omit_dns_ = """AND st.status!="DNS" """ if omitDNS else ""
            if raw:
                sql = f"""SELECT st.* FROM stage_times AS st {_entry_join} WHERE 1=1 {on_event_} {priority_} {on_stage_};"""
            else:
                _driver_join = (
                    f"INNER JOIN entries_drivers AS d ON e.driverId=d.personId"
                )
                _codriver_join = (
                    f"INNER JOIN entries_codrivers AS cd ON e.codriverId=cd.personId"
                )
                _stage_info_join = (
                    f"INNER JOIN stage_info AS si ON si.stageId=st.stageId"
                )
                _manufacturer_join = f"INNER JOIN manufacturers AS m ON e.manufacturerId=m.manufacturerId"
                _entrants_join = f"INNER JOIN entrants AS n ON e.entrantId=n.entrantId"
                sql = f"""SELECT d.code AS driverCode, d.fullName AS driverName, cd.fullName AS codriverName, m.name AS manufacturerName, n.name AS entrantName, e.vehicleModel, e.identifier AS carNo, e.priority, e.eligibility, si.code AS stageCode, st.* FROM stage_times AS st {_entry_join} {_driver_join} {_codriver_join} {_manufacturer_join} {_entrants_join} {_stage_info_join} WHERE 1=1 {omit_dns_} {on_event_} {on_stage_} {priority_};"""
                # TO DO have a query where we return DNS (did not start)
            r = self.db_manager.read_sql(sql)
            # Hack to poll API if empty
            if r.empty:
                logger.debug(f"getStageTimes empty read hack")
                self._getStageTimes(stageId=stageId, updateDB=True)
                r = self.db_manager.read_sql(sql)
        else:
            r = DataFrame()

        if r.empty:
            print(f"No getStageTimes? {self.eventId} {self.stageId} {self.rallyId}")
            return r

        df_stageTimes = r

        df_stageTimes["roadPos"] = range(1, len(df_stageTimes) + 1)
        df_stageTimes.sort_values("position", inplace=True)
        df_stageTimes["categoryPosition"] = range(1, len(df_stageTimes) + 1)
        df_stageTimes.sort_values("roadPos", inplace=True)

        # TO DO  - we need to rebase times to category / categoryPositionLeader
        if rebaseToCategory:
            pass
            # TO DO - need to initialise things back to category leader
        if "pos" in df_stageTimes:
            df_stageTimes["pos"] = df_stageTimes["pos"].astype("Int64")

        if "diffFirst" in df_stageTimes:
            df_stageTimes["Gap"] = df_stageTimes["diffFirstMs"].apply(
                lambda x: round(x / 1000, 1) if notnull(x) else nan
            )
        if "diffPrev" in df_stageTimes:
            df_stageTimes["Diff"] = df_stageTimes["diffPrevMs"].apply(
                lambda x: round(x / 1000, 1) if notnull(x) else nan
            )
            df_stageTimes["Chase"] = df_stageTimes["Diff"].shift(-1)
        if "elapsedDurationMs" in df_stageTimes:
            # df_stageTimes["timeInS"] = df_stageTimes["elapsedDurationMs"].apply(
            #    lambda x: x / 1000 if notnull(x) else nan
            # ).round(1)
            df_stageTimes["timeInS"] = (
                df_stageTimes["elapsedDurationMs"] / 1000
            ).round(1)
            # This duplicates Chase?
            df_stageTimes["timeToCarBehind"] = (
                df_stageTimes["timeInS"].diff(-1).round(1)
            )
            # Pace annotations

            df_stageDetails = self.getStageInfo()
            stage_dist = float(
                df_stageDetails.loc[
                    df_stageDetails["stageId"] == stageId, "distance"
                ].iloc[0]
            )
            df_stageTimes["speed (km/h)"] = (
                stage_dist / (df_stageTimes["timeInS"] / 3600)
            ).round(1)
            # Use .loc[] to modify the original DataFrame in place
            df_stageTimes["pace (s/km)"] = (
                df_stageTimes["timeInS"] / stage_dist
            ).round(2)

            p1_ = df_stageTimes[df_stageTimes["categoryPosition"] == 1].iloc[0]
            df_stageTimes["pace diff (s/km)"] = (
                df_stageTimes["pace (s/km)"] - p1_["pace (s/km)"]
            ).round(2)
            # A percent diff is always relative to something
            # In rebasing, we need to work with the actual times
            # so handle percentage diffs in the display logic for now?
            # df_stageTimes["percent"] = 100 * df_stageTimes["timeInS"] / df_stageTimes.loc[0,"timeInS"]
        return df_stageTimes

    def _getSplitTimes(self, *args, stageId=None, **kwargs):
        kwargs["eventId"] = self.eventId
        kwargs["rallyId"] = self.rallyId
        kwargs["stageId"] = stageId

        return self.api_client._getSplitTimes(*args, **kwargs)

    def getSplitTimes(self, stageId=None, priority=None, raw=True, updateDB=False):
        if updateDB or self.liveCatchup:
            updateDB = updateDB or self.isStageLive(stageId=stageId)
            self._getSplitTimes(stageId=stageId, updateDB=updateDB)
        stageId = stageId if stageId else self.stageId

        if stageId and self.eventId and self.rallyId:
            priority = None if priority == "P0" else priority
            on_event_ = f"AND spt.eventId={self.eventId} AND spt.stageId={stageId} AND spt.rallyId={self.rallyId}"
            priority_ = f"""AND e.priority LIKE "%{priority}" """ if priority else ""
            split_points_join = (
                "INNER JOIN split_points AS spp ON spp.splitPointId=spt.splitPointId"
            )

            if raw:
                sql = f"""SELECT spt.*, spp.number FROM split_times AS spt {split_points_join} WHERE 1=1 {on_event_} {priority_};"""
            else:
                _entry_join = f"INNER JOIN entries AS e ON spt.entryId=e.entryId"
                _driver_join = (
                    f"INNER JOIN entries_drivers AS d ON e.driverId=d.personId"
                )
                sql = f"""SELECT d.code AS driverCode, d.fullName AS driverName, e.identifier as carNo, e.vehicleModel, spt.*, ROUND(spt.elapsedDurationMs/1000, 2) AS elapsedTimeInS, spp.number FROM split_times AS spt {split_points_join} {_entry_join} {_driver_join} WHERE 1=1 {on_event_} {priority_};"""

            r = self.db_manager.read_sql(sql)
            # Hack to poll API if empty
            if r.empty:
                self._getSplitTimes(stageId=stageId, updateDB=True)
                r = self.db_manager.read_sql(sql)
        else:
            print(f"No getSplitTimes? {self.eventId} {self.stageId} {self.rallyId}")
            r = DataFrame()

        return r

    def getSplitTimesWide(
        self,
        stageId=None,
        priority=None,
        extended=False,
        timeInS=True,
        split_cols=None,
        updateDB=None,
    ):
        if updateDB or self.liveCatchup:
            updateDB = updateDB or self.isStageLive(stageId=stageId)
            self._getSplitTimes(stageId=stageId, updateDB=updateDB)

        split_times_df = self.getSplitTimes(
            stageId=stageId, priority=priority, raw=False
        )

        if split_times_df.empty:
            return DataFrame()

        split_times_df["number"] = self.SPLIT_PREFIX + split_times_df["number"].astype(
            str
        )

        # Road order given by startDateTime
        # road_order = split_times_df.sort_values("entryId")["driverNo"]
        split_times_wide = (
            pivot(
                split_times_df.dropna(subset=["number", "elapsedDurationMs"]),
                index=["carNo", "driverName", "entryId", "startDateTime"],
                columns="number",
                values="elapsedDurationMs",
            )
            .reset_index()
            .sort_values("startDateTime")
            .drop(columns="startDateTime")
        )

        # Optionally add in the final stage time
        if extended:
            stage_times = self.getStageTimes(stageId=stageId)[
                ["entryId", "elapsedDurationMs"]
            ]
            stage_times.rename(
                columns={"elapsedDurationMs": self.SPLIT_FINAL}, inplace=True
            )
            ## XX
            split_times_wide = merge(split_times_wide, stage_times, on="entryId")

        if timeInS:
            split_cols = (
                self.getSplitCols(split_times_wide) if not split_cols else split_cols
            )
            split_times_wide[split_cols] = (split_times_wide[split_cols] / 1000).round(
                1
            )

        split_times_wide.drop(columns=["entryId"], inplace=True)

        split_cols = self.getSplitCols(split_times_wide)
        cols_ = [c for c in split_times_wide if c not in split_cols]
        cols_ = cols_ + [c for c in split_cols if c in split_times_wide.columns]
        return split_times_wide[cols_]

    # TO DO below but one this as getStageWide and generalise names inside function
    # and maybe introduce a convenience getStageOverallWide
    def getStageTimesWide(
        self,
        stageId=None,
        priority=None,
        completed=False,
        running=False,
        typ="position",
        extent="stage",  # stage | overall
        updateDB=False,
    ):
        return self.getStageOverallWide(
            stageId=stageId,
            priority=priority,
            completed=completed, running=running,
            typ=typ,
            extent="stage",  # stage | overall
            updateDB=updateDB,
        )

    def getStageOverallWide(
        self,
        stageId=None,
        priority=None,
        completed=False,
        running=False,
        typ="position",
        extent="overall",  # stage | overall
        updateDB=False,
    ):
        # typ: position, totalTimeInS
        if self.eventId and self.rallyId and stageId:
            priority = None if priority == "P0" else priority
        if extent == "stage":
            overall_times = self.getStageTimes(
                stageId=stageId,
                completed=completed,
                running=running,
                priority=priority,
                raw=False,
                updateDB=updateDB,
            )
            # rebaseToCategory=True ??
        else:
            overall_times = self.getStageOverallResults(
                raw=False,
                stageId=stageId,
                priority=priority,
                completed=completed,
                running=running,
                updateDB=updateDB,
            )
        if overall_times.empty:
            return overall_times

        stage_order = overall_times["stageCode"].unique()
        # Optionally return just up to and including specified stageId
        # TO DO
        # if stageId:

        overall_times_wide = pivot(
            overall_times.dropna(subset=["position"]),
            index=["carNo", "driverName", "entryId"],
            columns="stageCode",
            values=typ,
        ).reset_index()
        cols_ = [c for c in overall_times_wide.columns if c not in stage_order]
        cols_ = cols_ + [c for c in stage_order if c in overall_times_wide.columns]
        return overall_times_wide[cols_]

    def getOverallStageCols(self, overall_times_wide):
        """Get special stage columns."""
        stage_cols = [
            c for c in overall_times_wide.columns if c.startswith(self.STAGE_PREFIX)
        ]
        return stage_cols

    def getSplitCols(self, split_times_wide):
        """Get the split time columns from the wide splits dataframe."""
        # TO DO - also provide an option for getting this from splitId?? values?
        split_cols = [
            c
            for c in split_times_wide.columns
            if c.startswith(self.SPLIT_PREFIX) or c == self.SPLIT_FINAL
        ]
        # Sort the split sections in increasing order
        split_cols = sorted(
            split_cols,
            key=lambda x: (
                float("inf")
                if x == self.SPLIT_FINAL
                else int(x[len(self.SPLIT_PREFIX) :])
            ),
        )

        return split_cols

    def getStageCols(self, stages_wide):
        """Get the stage columns from a wide stages dataframe."""
        # TO DO - also provide an option for getting this from stageId values?
        stage_cols = [
            c
            for c in stages_wide.columns
            if c.startswith(self.STAGE_PREFIX) or c == self.STAGE_FINAL
        ]
        return stage_cols

    def getSplitDuration(
        self, split_times_wide, split_cols=None, ret_id=True, id_col=None
    ):
        """The time it takes a car to traverse a split section (split_times_wide)."""

        id_col = ["carNo", "driverName"] if not id_col else id_col
        id_col = [id_col] if isinstance(id_col, str) else id_col
        id_col = [c for c in id_col if c in split_times_wide.columns]

        # Ensure split_cols are strings
        split_cols = (
            self.getSplitCols(split_times_wide) if not split_cols else split_cols
        )

        # Create a copy of the dataframe with selected columns
        df_ = split_times_wide[split_cols].copy()

        # Calculate differences between consecutive columns
        diff_df = df_[split_cols[1:]].values - df_[split_cols[:-1]].values

        # Convert back to dataframe
        diff_df = DataFrame(diff_df, columns=split_cols[1:], index=df_.index)

        # Add first split column back
        diff_df[split_cols[0]] = df_[split_cols[0]]

        if ret_id:
            # Add entryId column
            diff_df[id_col] = split_times_wide[id_col]

            # Reorder columns
            cols = id_col + split_cols
            return diff_df[cols]

        return diff_df

    def getScaledSplits(self, stageId, priority, view, id_col=None):
        id_col = ["carNo", "driverName"] if not id_col else id_col

        split_times_wide = self.getSplitTimesWide(
            stageId=stageId, priority=priority, extended=True, timeInS=True
        )

        if split_times_wide.empty:
            return DataFrame()

        split_cols = self.getSplitCols(split_times_wide)

        if view in ["time_acc", "pos_acc"]:
            # TO DO  precision number format formatting
            # styles = {c: "{0:0.1f}" for c in split_cols}
            # return split_times_wide.style.format(styles)

            split_times_wide.loc[:, split_cols] = split_times_wide[split_cols].round(1)

            if view == "pos_acc":
                split_times_wide.loc[:, split_cols] = split_times_wide[split_cols].rank(
                    method="min", na_option="keep"
                )

            # split_times_wide.columns = (
            #    ["Driver", "TeamName", "RoadPos"]
            #    + [f"Split {i}" for i in range(1, len(split_cols))]
            #    + ["Finish"]
            # )
            return split_times_wide

        split_durations = self.getSplitDuration(
            split_times_wide,
            id_col=id_col,
        )

        split_dists_ = self.getStageSplitPoints(stageId=stageId, extended=True)
        split_dists = split_dists_.set_index("name")["distance_"].to_dict()

        if split_dists:
            scaled_splits_wide = split_durations.copy()
            if view == "pos_within":
                scaled_splits_wide.loc[:, split_cols] = scaled_splits_wide[
                    split_cols
                ].rank(method="min", na_option="keep")
            elif view == "pace":
                scaled_splits_wide.update(
                    scaled_splits_wide.loc[:, split_dists.keys()].apply(
                        lambda s: s / split_dists[s.name]
                    )
                )
            elif view == "speed":
                scaled_splits_wide.update(
                    scaled_splits_wide.loc[:, split_dists.keys()].apply(
                        lambda s: 3600 * split_dists[s.name] / s
                    )
                )

        # styles = {c: "{0:0.1f}" for c in split_cols}

        if not view.startswith("pos_"):
            scaled_splits_wide.loc[:, split_cols] = scaled_splits_wide[
                split_cols
            ].round(1)

        return scaled_splits_wide

    def rebase_splits_wide_with_ult(
        self, split_times_wide, rebase_driver, use_split_durations=True
    ):
        split_cols = self.getSplitCols(split_times_wide)

        # output_ = split_times_wide_numeric
        # Use the split durations rather than split elapsed times
        output_ = (
            self.getSplitDuration(split_times_wide)
            if use_split_durations
            else split_times_wide
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
        output_ = self.rebaseManyTimes(output_, rebase_driver, "carNo", split_cols)
        output_ = output_[output_["carNo"] != "ult"]
        return output_, split_cols

    def _getStageOverallResults(
        self, *args, stageId=None, by_championship=False, **kwargs
    ):
        kwargs["eventId"] = self.eventId
        kwargs["rallyId"] = self.rallyId
        kwargs["championshipId"] = self.championshipId
        kwargs["stageId"] = stageId
        kwargs["by_championship"] = by_championship

        return self.api_client._getStageOverallResults(*args, **kwargs)

    # TO DO  - improve this; have a function that gets stages by status (Running, To run, completed, cancelled, postponed etc)
    def getCompletedStages(self, stageId=None, completed=True, running=False, last=False, stageMode="uptoincl", on_event=True):
        # stageId is the up to and including stageId, else all TO DO still

        completed_stages = (
            self.getStageInfo(raw=False, on_event=on_event, completed=completed, running=running, last=last)
            .sort_values("number")
            .reset_index(drop=True)
        )
        if stageId:
            # if we have a single stageId, get stages up to that
            if isinstance(stageId, int) and stageMode == "uptoincl":
                upto_idx = completed_stages[
                    completed_stages["stageId"] == stageId
                ].index.tolist()
                # Get frames up to an including the specified stage
                if upto_idx:
                    completed_stages = completed_stages[: upto_idx[0] + 1]
            elif isinstance(stageId, list):
                # if we have a list of stageIds, just get those
                completed_stages = completed_stages[
                    completed_stages["stageId"].isin(stageId)
                ]

        completed_stages = (
            completed_stages[["stageId", "code"]].set_index("stageId")["code"].to_dict()
        )
        return completed_stages

    def _updateCompletedStagesStatus(self, stageId, table, status):
        self.dbfy(
            DataFrame(
                [{"stageId": int(stageId), "tableType": table, "status": status}]
            ),
            "meta_completed_stage_tables",
            pk=["tableType", "stageId"],
        )

    def checkCompletedStageTableStatus(self, stageId, table):
        """Return a True flag if we have stored this table."""
        sql = f"""SELECT meta.* FROM meta_completed_stage_tables AS meta WHERE stageId={stageId} AND tableType="{table}";"""
        _result = self.query(sql=sql)
        status = not _result.empty
        return status

    def handleStageCompleted(self, stageId, tables=None):
        """Check to see if we have a completed stage."""

        def _isStageCompleted(stageId):
            """Check to see if the stage is listed as completed or cancelled."""
            stage_info = self.getStageInfo()
            status = stage_info[stage_info["stageId"] == int(stageId)]
            if status.empty:
                return False, None
            status = status["status"].iloc[0].lower()
            completed = status in ["completed", "cancelled"]
            return completed, status

        if tables is None:
            tables = ["stage_overall"]  # add splits etc
        if isinstance(tables, str):
            tables = [tables]
        stage_completed, stage_status = _isStageCompleted(stageId)
        if stage_completed:
            for table in tables:
                if not self.checkCompletedStageTableStatus(stageId, table):
                    # Update the db with the completed data
                    # completed also includes cancelled
                    if table == "stage_overall":
                        self._getStageOverallResults(stageId=stageId, updateDB=True)
                    elif table == "stage_times":
                        self._getStageTimes(stageId=stageId, updateDB=True)
                    # also other tables?
                    self._updateCompletedStagesStatus(stageId, table, stage_status)
            return True
        return False

    def getStageOverallResults(
        self, stageId=None, priority=None, completed=False, running=False, last=False, on_event=True, raw=True, updateDB=False
    ):
        # The assumption below is for on_event
        if completed or running or last:
            stageIds = self.getCompletedStages(
                stageId=stageId, completed=completed, running=running, last=last, on_event=on_event)
        elif stageId:
            stageIds = [stageId]
        else:
            stageIds = []  # TO DO map for the default stageId
        # print("stageIds - ", stageIds)
        if updateDB or self.liveCatchup:
            logger.debug(
                f"getStageOverallResults: completed: {completed} running: {running} updateDB:{updateDB} liveCatchup: {self.liveCatchup} isStageLive: {self.isStageLive(stageId=stageId)}"
            )
            if completed:
                # Check availability of every stage required
                for stageId in stageIds:
                    updateDB = updateDB or self.isStageLive(stageId=stageId)
                    # TO DO we only want to request data from API if we don't already
                    # have it in the db as completed
                    # Need a new table / completed_status table to say what completed datasets
                    # have been downloaded.
                    # If a stage status is completed, download that result then add
                    # a flag to the completed_status table
                    # CREATE TABLE "completed_tables" (
                    # "tableType" TEXT,
                    # "tableId" INTEGER,
                    #  PRIMARY KEY ("tableType", "tableId"),
                    # )
                    if not self.handleStageCompleted(stageId):
                        self._getStageOverallResults(stageId=stageId, updateDB=updateDB)
            else:
                for stageId in stageIds:
                    updateDB = updateDB or self.isStageLive(stageId=stageId)
                    self._getStageOverallResults(stageId=stageId, updateDB=updateDB)

        stageIds = stageIds if stageIds else [self.stageId]
        # TO DO if stageId and completed treat that as up to?
        if self.eventId and self.rallyId and stageIds: #(stageId or completed or running):
            priority = None if priority == "P0" else priority
            _entry_join = f"INNER JOIN entries AS e ON o.entryId=e.entryId"
            _stage_info_join = f"INNER JOIN stage_info AS si ON si.stageId=o.stageId"

            on_event_ = f"AND o.eventId={self.eventId} AND o.rallyId={self.rallyId}"
            stage_ids_str = ",".join(str(sid) for sid in stageIds)
            on_stage_ = f"AND o.stageId IN ({stage_ids_str})"
            # if completed and stageIds:
            #    stage_ids_str = ",".join(str(sid) for sid in stageIds)
            #    on_stage_ = f"AND o.stageId IN ({stage_ids_str})"
            # else:
            #    on_stage_ = f"AND o.stageId={stageId}" if stageId else ""
            priority_ = f"""AND e.priority LIKE "%{priority}" """ if priority else ""

            # completed_ = """AND si.status="Completed" """ if completed else ""
            running_ = ', "Running"' if running else ""
            completed_ = (
                f"""AND si.status IN ("Completed", "Cancelled", "Interrupted" {running_}) """
                if completed
                else ""
            )

            if raw:
                sql = f"""SELECT o.* FROM stage_overall AS o {_entry_join} {_stage_info_join} WHERE 1=1 {on_event_} {on_stage_} {priority_} {completed_};"""
            else:
                _driver_join = (
                    f"INNER JOIN entries_drivers AS d ON e.driverId=d.personId"
                )
                _codriver_join = (
                    f"INNER JOIN entries_codrivers AS cd ON e.codriverId=cd.personId"
                )
                _manufacturer_join = f"INNER JOIN manufacturers AS m ON e.manufacturerId=m.manufacturerId"
                _entrants_join = f"INNER JOIN entrants AS n ON e.entrantId=n.entrantId"
                sql = f"""SELECT d.code AS driverCode, d.fullName AS driverName, e.vehicleModel, e.identifier AS carNo, cd.fullName AS codriverName, m.name AS manufacturerName, n.name AS entrantName, e.priority, e.eligibility, si.code AS stageCode, si.number AS stageOrder, o.* FROM stage_overall AS o {_entry_join} {_driver_join} {_codriver_join} {_manufacturer_join} {_entrants_join} {_stage_info_join} WHERE 1=1 {on_event_} {on_stage_} {priority_} {completed_} ORDER BY stageOrder, o.position ASC;"""

            r = self.db_manager.read_sql(sql)

            # Hack to poll API if empty
            if r.empty or (not r.empty and
                completed and len(r["stageCode"].unique().tolist()) < len(stageIds)
            ):

                if completed:
                    for stageId in stageIds:
                        self._getStageOverallResults(stageId=stageId, updateDB=True)
                else:
                    self._getStageOverallResults(stageId=stageId, updateDB=True)
                r = self.db_manager.read_sql(sql)
        else:
            print(
                f"No getStageOverallResults? {self.eventId} {self.stageId} {self.rallyId}"
            )
            r = DataFrame()

        if r.empty:
            return r

        overall_df = r
        overall_df["roadPos"] = range(1, len(overall_df) + 1)

        sort_keys_ = ["stageOrder", "position"] if "stageOrder" in overall_df.columns else ["position"]
        overall_df.sort_values(sort_keys_, inplace=True)
        # overall_df["categoryPosition"] = range(1, len(overall_df) + 1)
        overall_df["categoryPosition"] = overall_df.groupby(["stageId"]).cumcount() + 1
        # overall_df.sort_values(sort_keys_, inplace=True)

        # if we were working wihtin groups, eg for class position
        # overall_df["Diff"] = overall_df.groupby("stageCode")["diffPrevMs"].apply(
        #     lambda group: group.apply(
        #         lambda x: round(x / 1000, 1) if notnull(x) else nan
        #     )
        # )
        if "diffFirstMs" in overall_df:
            overall_df["Gap"] = overall_df.groupby("stageId")[
                "diffFirstMs"
            ].transform(
                lambda group: where(notnull(group), group.div(1000).round(1), nan)
            )
        if "diffPrevMs" in overall_df:
            overall_df["Diff"] = overall_df.groupby("stageId")[
                "diffPrevMs"
            ].transform(
                lambda group: where(notnull(group), group.div(1000).round(1), nan)
            )
            overall_df["Chase"] = overall_df["Diff"].shift(-1)
        if "totalTimeMs" in overall_df:
            # df_stageTimes["timeInS"] = df_stageTimes["elapsedDurationMs"].apply(
            #    lambda x: x / 1000 if notnull(x) else nan
            # ).round(1)
            overall_df["timeInS"] = (overall_df["totalTimeMs"] / 1000).round(1)

        if completed:
            overall_df.rename(columns=stageIds, inplace=True)
        return r

    def _getStageWinners(self, *args, **kwargs):
        kwargs["eventId"] = self.eventId
        kwargs["rallyId"] = self.rallyId
        return self.api_client._getStageWinners(*args, **kwargs)

    def _getRetirements(self, *args, **kwargs):
        kwargs["eventId"] = self.eventId
        return self.api_client._getRetirements(*args, **kwargs)

    def _getPenalties(self, *args, **kwargs):
        kwargs["eventId"] = self.eventId
        return self.api_client._getPenalties(*args, **kwargs)

    def getStageWinnerAdvantages(self, on_event=True, eventId=None, stageId=None):
        # TO DO - get speed, pace and time advantage, pos 1 to pos2
        # get position=1 or position=2
        # dedupe on position, sort by position ascending=True
        # if len(2): the delta is the advantage
        pass

    def getEventWinnerAdvantages(self, on_championship=True, eventId=None):
        # TO DO - get speed, pace and time advantage, pos 1 to pos2
        # get position=1 or position=2
        # dedupe on position, sort by position ascending=True
        # if len(2): the delta is the advantage
        pass

    def _getStageWinsCount(
        self, on_event=True, eventId=None, entryId=None, eventEntry=None, raw=True
    ):
        # TO DO WIP : should we use entryId or entrantId ?
        if (not eventId and not eventEntry) or (
            on_event and (not self.eventId or not self.rallyId)
        ):
            return DataFrame()
        if eventEntry:
            if isinstance(eventEntry, tuple):
                eventEntry = [eventEntry]

            conditions = []
            for evt_id, ent_id in eventEntry:
                conditions.append(f"(eventId={evt_id} AND entryId={ent_id})")

            _on_event_entry = "AND (" + " OR ".join(conditions) + ")"
            _on_event = _on_entry = ""
        else:
            if eventId:
                eventIds = [eventId] if isinstance(eventId, str) else eventId
                eventId_str = ",".join(str(eid) for eid in eventIds)
                _on_event = f"AND eventId IN ({eventId_str})"
            elif on_event:
                _on_event = f"AND eventId={self.eventId} AND rallyId={self.rallyId}"
            else:
                _on_event = ""

            _on_entry = f"""AND entryId={entryId}""" if entryId else ""
            _on_event_entry = ""

        sql = f"""SELECT entryId, COUNT(*) AS stage_wins FROM stagewinners WHERE 1=1 {_on_event} {_on_entry} {_on_event_entry} GROUP BY entryId, eventId ORDER BY stage_wins DESC;"""
        r = self.db_manager.read_sql(sql)
        return r

    def getStageWinners(self, on_event=True, eventId=None, priority=None, raw=True):
        if not eventId and (on_event and (not self.eventId or not self.rallyId)):
            return DataFrame()
        # TO DO - do we need the rallyId ? Are there examples of multiple rallyId for eventId ?
        # TO DO - handle priority; maybe create a category_stagewinners db table?
        if eventId:
            _on_event = f"AND w.eventId={eventId}"
        elif on_event:
            _on_event = f"AND w.eventId={self.eventId} AND w.rallyId={self.rallyId}"
        else:
            _on_event = ""

        if raw:
            sql = f"""SELECT w.* FROM stagewinners AS w WHERE 1=1 {_on_event};"""
        else:
            _entry_join = f"INNER JOIN entries AS e ON w.entryId=e.entryId"
            _stages_join = f"INNER JOIN stage_info AS st ON st.stageId=w.stageId"
            _driver_join = f"INNER JOIN entries_drivers AS d ON e.driverId=d.personId"
            _codriver_join = (
                f"INNER JOIN entries_codrivers AS cd ON e.codriverId=cd.personId"
            )
            _manufacturer_join = (
                f"INNER JOIN manufacturers AS m ON e.manufacturerId=m.manufacturerId"
            )
            _entrants_join = f"INNER JOIN entrants AS n ON e.entrantId=n.entrantId"
            _itinerary_stages_join = (
                f"INNER JOIN itinerary_stages AS it_st ON it_st.stageId=w.stageId"
            )
            _itinerary_sections_join = f"INNER JOIN itinerary_sections AS it_se ON it_se.itinerarySectionId=it_st.itinerarySectionId"
            _itinerary_legs_join = f"INNER JOIN itinerary_legs AS it_l ON it_l.itineraryLegId=it_st.itineraryLegId"
            sql = f"""SELECT d.code AS driverCode, d.fullName AS driverName, cd.fullName AS codriverName, m.name AS manufacturerName, n.name AS entrantName, e.identifier AS carNo, e.vehicleModel, it_st.code, it_se.name AS sectionName, it_l.name AS day, st.distance, e.entryId, w.* FROM stagewinners AS w {_entry_join} {_stages_join} {_driver_join} {_codriver_join} {_manufacturer_join} {_entrants_join} {_itinerary_stages_join} {_itinerary_sections_join} {_itinerary_legs_join} WHERE 1=1 {_on_event};"""

        r = self.db_manager.read_sql(sql)
        # Hack to poll API if empty
        if r.empty:
            self._getStageWinners(updateDB=True)
            r = self.db_manager.read_sql(sql)
        return r

    # TO DO - offer more search limits
    def getRetirements(self, on_event=True, raw=True, updateDB=False):
        if updateDB or self.liveCatchup:
            updateDB = updateDB or self.isRallyLive()
            self._getRetirements(updateDB=updateDB)

        _on_event = f"AND r.eventId={self.eventId}" if on_event and self.eventId else ""

        if raw:
            sql = f"""SELECT r.* FROM retirements r WHERE 1=1 {_on_event};"""
        else:
            # Need to merge entryId and controlId
            _entry_join = f"INNER JOIN entries AS e ON r.entryId=e.entryId"
            _driver_join = f"INNER JOIN entries_drivers AS d ON e.driverId=d.personId"
            _codriver_join = (
                f"INNER JOIN entries_codrivers AS cd ON e.codriverId=cd.personId"
            )
            _manufacturer_join = (
                f"INNER JOIN manufacturers AS m ON e.manufacturerId=m.manufacturerId"
            )
            _entrants_join = f"INNER JOIN entrants AS n ON e.entrantId=n.entrantId"
            _control_join = f"INNER JOIN stage_controls AS c ON r.controlId=c.controlId"

            sql = f"""SELECT d.code AS driverCode, d.fullName AS driverName, cd.fullName AS codriverName, m.name AS manufacturerName, n.name AS entrantName, e.identifier AS carNo, e.vehicleModel, c.code, r.reason, c.location, c.type, r.retirementDateTime, r.status FROM retirements r {_entry_join} {_driver_join} {_codriver_join} {_manufacturer_join} {_entrants_join} {_control_join} WHERE 1=1 {_on_event};"""

        r = self.db_manager.read_sql(sql)
        # Hack to poll API if empty
        if r.empty:
            self._getRetirements(updateDB=True)
            r = self.db_manager.read_sql(sql)
        return r

    # TO DO - offer more search limits
    def getPenalties(self, on_event=True, raw=True, updateDB=False):
        if updateDB or self.liveCatchup:
            updateDB = updateDB or self.isRallyLive()
            self._getPenalties(updateDB=updateDB)

        _on_event = f"AND p.eventId={self.eventId}" if on_event and self.eventId else ""

        if raw:
            sql = f"""SELECT p.* FROM penalties AS p WHERE 1=1 {_on_event};"""
        else:
            # Need to merge entryId and controlId
            _entry_join = f"INNER JOIN entries AS e ON p.entryId=e.entryId"
            _driver_join = f"INNER JOIN entries_drivers AS d ON e.driverId=d.personId"
            _codriver_join = (
                f"INNER JOIN entries_codrivers AS cd ON e.codriverId=cd.personId"
            )
            _manufacturer_join = (
                f"INNER JOIN manufacturers AS m ON e.manufacturerId=m.manufacturerId"
            )
            _entrants_join = f"INNER JOIN entrants AS n ON e.entrantId=n.entrantId"
            _control_join = f"INNER JOIN stage_controls AS c ON p.controlId=c.controlId"
            sql = f"""SELECT d.code AS driverCode, d.fullName AS driverName, cd.fullName AS codriverName, m.name AS manufacturerName, n.name AS entrantName, e.identifier AS carNo, e.vehicleModel, c.code, p.penaltyDuration, p.Reason, c.location, c.type FROM penalties AS p {_entry_join} {_driver_join} {_codriver_join} {_manufacturer_join} {_entrants_join} {_control_join} WHERE 1=1 {_on_event};"""

        r = self.db_manager.read_sql(sql)
        # Hack to poll API if empty
        if r.empty:
            self._getPenalties(updateDB=True)
            r = self.db_manager.read_sql(sql)
        return r

    def query(self, sql):
        r = self.db_manager.read_sql(sql)
        return r
