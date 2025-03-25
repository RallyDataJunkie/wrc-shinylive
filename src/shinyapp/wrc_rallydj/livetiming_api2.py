import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

from urllib.parse import urljoin
import datetime
from sqlite_utils import Database
from jupyterlite_simple_cors_proxy.cacheproxy import CorsProxy, create_cached_proxy
import os
import sqlite3
from wrc_rallydj.db_table_schemas import SETUP_V2_Q
from pandas import read_sql, DataFrame, json_normalize, merge, concat


# The WRCLiveTimingAPIClientV2() constructs state on a season basis
class WRCLiveTimingAPIClientV2:
    RED_BULL_LIVETIMING_API_BASE = (
        "https://p-p.redbull.com/rb-wrccom-lintegration-yv-prod/api/"
    )

    def __init__(
        self,
        year: int = datetime.date.today().year,
        championship: str = "wrc",
        category: str = "Drivers",
        group: str = "all",
        dbname: str = "wrcRbAPITiming.db",
        newDB: bool = False,
        use_cache: bool = False,
        **cache_kwargs,
    ):
        # Initialize the proxy with caching if requested
        if use_cache:
            self.proxy = create_cached_proxy(**cache_kwargs)
        else:
            self.proxy = CorsProxy()

        self.year = year
        # TO DO - more logic yet surrounding championship
        self.championship = championship  # Informal: WRC, WRC2, WRC3, JWRC

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
        self.conn = None
        self.dbname = dbname
        # In the DB initialise phase also set self.seasonId
        self.setup_db(newdb=newDB)

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

    # db utils
    def cleardbtable(self, table, conn=None):
        """Clear the table whilst retaining the table definition"""
        conn = conn if conn else self.conn

        c = conn.cursor()
        c.execute('DELETE FROM "{}"'.format(table))

    def dbfy(
        self,
        df,
        table,
        if_exists="upsert",
        pk=None,
        index=False,
        clear=False,
        conn=None,
        **kwargs,
    ):
        """Save a dataframe as a SQLite table.
        Clearing or replacing a table will first empty the table of entries but retain the structure.
        """

        conn = conn if conn else self.conn

        # Upsert requires a PK
        if if_exists == "upsert" and not pk:
            return

        # print('{}: {}'.format(table,df.columns))
        if if_exists == "replace":
            clear = True
            if_exists = "append"
        if clear:
            self.cleardbtable(table)

        # Get columns
        q = "PRAGMA table_info({})".format(table)
        cols = read_sql(q, conn)["name"].tolist()
        for c in df.columns:
            if c not in cols:
                print(
                    "Hmmm... column name `{}` appears in data but not {} table def?".format(
                        c, table
                    )
                )
                df.drop(columns=[c], inplace=True)

        logger.info(f"Updating {table}...")

        if if_exists == "upsert":
            DB = Database(conn)
            DB[table].upsert_all(df.to_dict(orient="records"), pk=pk)
        else:
            df.to_sql(table, conn, if_exists=if_exists, index=index)

    def setup_db(self, dbname=None, newdb=False):
        """Setup a database, if required, and return a connection."""
        logger.info("Initialising the database...")

        if dbname is not None:
            self.dbname = dbname
        dbname = self.dbname

        # In some situations, we may want a fresh start
        if os.path.isfile(dbname) and newdb:
            os.remove(dbname)

        if not os.path.isfile(dbname):
            # No db exists, so we need to create and populate one
            newdb = True

        # Open database connection
        self.conn = conn = sqlite3.connect(dbname, timeout=10)

        if newdb:
            logger.info("Creating new db tables...")
            # Setup database tables
            c = conn.cursor()
            c.executescript(SETUP_V2_Q)
            # c.executescript(SETUP_VIEWS_Q)

            # Populate the database with seasons info
            # self._getSeasons(updateDB=True)
            # Initialise the seasonId
            self.seasonId = self._getSeasons(
                self.championship, self.year, updateDB=True
            ).iloc[0]["seasonId"]
            # Update the season detail
            self._getSeasonDetail(updateDB=True)

            # Populate the database with event metadata
            # self.dbfy(conn, getEventMetadata(), "event_metadata", if_exists="replace")

            # Get geo bits
            # kml_processor(meta["event_meta"])

            # Save the entry list, initial itinerary etc
            # _save_rally_base(meta, conn)

        return conn

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
        rj = r.json()
        if "status" in rj and rj["status"] == "Not Found":
            return {}
        return r.json()

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
        """Rebase times in several specified columns relative to a particular vehicle."""
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

    def _getSeasons(self, championship=None, year=None, updateDB=False):
        """The seasons feed is regularly updated throughout the season."""
        stub = f"seasons.json"
        json_data = self._WRC_RedBull_json(stub)
        seasons_df = DataFrame(json_data)
        if seasons_df.empty:
            return DataFrame()

        if updateDB:
            self.dbfy(seasons_df, "seasons", pk="seasonId")

        seasons_df = self._getSeasonsSubQuery(seasons_df, championship, year)
        return seasons_df

    def getSeasons(self, championship=None, year=None, updateDB=False):
        if updateDB:
            self._getSeasons(championship, year, updateDB)

        q = "SELECT * FROM seasons;"
        seasons_df = read_sql(q, self.conn)

        return self._getSeasonsSubQuery(seasons_df, championship, year)

    def setSeason(self):
        self.seasonId = self._getSeasons(
            self.championship, self.year, updateDB=True
        ).iloc[0]["seasonId"]

    # This datafeed is partial at the start of the season
    # and needs to be regularly updated
    def _getSeasonDetail(self, updateDB=False):
        stub = f"season-detail.json?seasonId={self.seasonId}"
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

    def getSeasonRounds(self, updateDB=False):
        if updateDB:
            self._getSeasonDetail(updateDB)

        q = "SELECT * FROM season_rounds;"
        seasonRounds_df = read_sql(q, self.conn)

        return seasonRounds_df

    def _getChampionshipName(self):
        championship = self.championship
        categories = ["Drivers", "Co-Drivers"]
        if self.championship.lower() in ["wrc", "wrc2"]:
            categories.append("Teams")
        if self.championship.lower() in ["wrc"]:
            categories.append("Manufacturers")
        category = self.category
        # Category: Drivers, Co-Drivers, Manufacturers, Teams
        if category not in categories:
            return None

        _championship = "FIA World Rally Championship for Drivers"
        if championship.lower() == "wrc":
            _championship = f"FIA World Rally Championship for {category}"
        elif championship.lower() == "wrc2":
            _championship = f"FIA WRC2 Championship for {category}"
        elif championship.lower() == "wrc3":
            _championship = f"FIA WRC3 Championship for {category}"
        elif championship.lower() == "jwrc":
            _championship = f"FIA Junior WRC Championship for {category}"
        elif championship.lower() == "challenger":
            _championship = f"FIA WRC2 Challenger Championship for {category}"
        elif championship.lower() == "masters":
            _championship = f"FIA WRC Masters Cup for {category}"
        return _championship

    def _getChampionshipOverallResults(self, updateDB=False):
        if not self.championshipId or not self.seasonId:
            return DataFrame(), DataFrame()

        stub = f"championship-overall-results.json?championshipId={self.championshipId}&seasonId={self.seasonId}"
        json_data = self._WRC_RedBull_json(stub)
        if "entryResults" not in json_data:
            return DataFrame(), DataFrame()

        _championshipEntryResultsOverall = []
        _championshipEntryResultsByRound = []

        for championshipEntry in json_data["entryResults"]:
            _championshipEntry = {}
            for k in ["championshipEntryId", "overallPosition", "overallPoints"]:
                _championshipEntry[k] = championshipEntry[k]
            _championshipEntryResultsOverall.append(_championshipEntry)
            _championshipEntryResultsByRound.extend(championshipEntry["roundResults"])

        championshipEntryResultsOverall_df = DataFrame(_championshipEntryResultsOverall)
        championshipEntryResultsByRound_df = DataFrame(_championshipEntryResultsByRound)

        if updateDB:
            self.dbfy(
                championshipEntryResultsOverall_df,
                "championship_overall",
                if_exists="replace",
            )
            self.dbfy(
                championshipEntryResultsByRound_df,
                "championship_results",
                pk=("championshipEntryId", "eventId"),
            )

        return championshipEntryResultsOverall_df, championshipEntryResultsByRound_df

    def getChampionshipOverall(self, updateDB=False):
        if updateDB:
            self._getChampionshipOverallResults(updateDB)

        q = "SELECT * FROM championship_overall;"
        championshipEntryResultsOverall_df = read_sql(q, self.conn)

        return championshipEntryResultsOverall_df

    def getChampionshipByRound(self, updateDB=False):
        if updateDB:
            self._getChampionshipOverallResults(updateDB)

        q = "SELECT * FROM championship_results;"
        championshipEntryResultsByRound_df = read_sql(q, self.conn)

        return championshipEntryResultsByRound_df

    def _getChampionshipDetail(self, updateDB=False):
        # If championshipId is None, try to find a championship Id
        if not self.championshipId:

            # Use WRC drivers as the default
            _championship = self._getChampionshipName()

            seasonId, championships_df, _, _ = self._getSeasonDetail()
            championshipId = championships_df[
                championships_df["name"] == _championship
            ].iloc[0]["championshipId"]

        if self.seasonId is None:
            seasonId = self._getSeasons(self.championship, self.year).iloc[0][
                "seasonId"
            ]

        stub = f"championship-detail.json?championshipId={self.championshipId}&seasonId={self.seasonId}"
        json_data = self._WRC_RedBull_json(stub)
        if "championshipRounds" not in json_data:
            return DataFrame(), DataFrame(), DataFrame()

        rounds = [r["event"] for r in json_data["championshipRounds"]]
        championshipRounds_df = DataFrame(rounds)
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
        renamers["tyreManufacturer"] = "tyreManufacturerId"
        championshipEntries_df.rename(columns=renamers, inplace=True)

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

    def getChampionShipRounds(self, updateDB=False):
        if updateDB:
            self._getChampionshipDetail()

        q = "SELECT * FROM championship_rounds_detail;"
        championshipRounds_df = read_sql(q, self.conn)

        return championshipRounds_df

    def getChampionshipEntries(self, updateDB=False):
        if updateDB:
            self._getChampionshipDetail()

        q = "SELECT * FROM championship_entries;"
        championshipEntries_df = read_sql(q, self.conn)

        return championshipEntries_df

    def getChampionshipCountries(self, updateDB=False):
        if updateDB:
            self._getChampionshipDetail()

        q = "SELECT * FROM championship_countries;"
        championships_df = read_sql(q, self.conn)

        return championships_df

    def getChampionships(self, updateDB=False):
        if updateDB:
            self._getSeasonDetail(updateDB)

        q = "SELECT * FROM championship_lookup;"
        championshipCountries_df = read_sql(q, self.conn)

        return championshipCountries_df

    def _getEventGroups(self, updateDB=False):
        stub = f"events/{self.eventId}/groups.json"
        json_data = self._WRC_RedBull_json(stub)

        eventGroups_df = DataFrame(json_data)
        if eventGroups_df.empty:
            return DataFrame()

        if updateDB:
            self.dbfy(eventGroups_df, "groups", pk="groupId")
        return eventGroups_df

    def _getEventItineraries(self, updateDB=False):
        stub = f"events/{self.eventId}/itineraries/{self.itineraryId}.json"
        json_data = self._WRC_RedBull_json(stub)
        if "itineraryLegs" not in json_data:
            return DataFrame(), DataFrame(), DataFrame(), DataFrame()

        itineraryLegs_df = DataFrame(json_data["itineraryLegs"])
        itineraryLegs_df["eventId"] = self.eventId

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
            for _, row in itineraryLegs_df.iterrows():
                startListId = row["startListId"]
                self._getStartLists(startListId=startListId, updateDB=updateDB)

        return (
            itineraryLegs_df,
            itinerarySections2_df,
            itineraryControls_df,
            itineraryStages_df,
        )

    def getItineraryLegs(self, eventId=None, updateDB=False):
        if updateDB:
            self._getEventItineraries(updateDB)

        if eventId is None:
            q = "SELECT * FROM itinerary_legs;"
        else:
            q = f"SELECT * FROM itinerary_legs WHERE eventId={int(eventId)};"

        itineraryLegs_df = read_sql(q, self.conn)

        return itineraryLegs_df

    def getItineraryStages(
        self, eventId=None, itineraryLegId=None, itinerarySectionId=None, updateDB=False
    ):
        if updateDB:
            self._getEventItineraries(updateDB)

        if not eventId and not itineraryLegId and not itinerarySectionId:
            q = "SELECT * FROM itinerary_stages;"
        elif itinerarySectionId:
            q = f"SELECT * FROM itinerary_stages WHERE itinerarySectionId={int(itinerarySectionId)};"
        elif itineraryLegId:
            q = f"SELECT * FROM itinerary_stages WHERE itineraryLegId={int(itineraryLegId)};"
        elif eventId:
            q = f"SELECT * FROM itinerary_stages WHERE eventId={int(eventId)};"

        itinerarySections_df = read_sql(q, self.conn)

        return itinerarySections_df

    def getItinerarySections(self, eventId=None, itineraryLegId=None, updateDB=False):
        if updateDB:
            self._getEventItineraries(updateDB)

        if not eventId and not itineraryLegId:
            q = "SELECT * FROM itinerary_sections;"
        elif itineraryLegId:
            q = f"SELECT * FROM itinerary_sections WHERE itineraryLegId={int(itineraryLegId)};"
        elif eventId:
            q = f"SELECT * FROM itinerary_sections WHERE eventId={int(eventId)};"

        itinerarySections_df = read_sql(q, self.conn)

        return itinerarySections_df

    def getItineraryControls(
        self, eventId=None, itineraryLegId=None, itinerarySectionId=None, updateDB=False
    ):
        if updateDB:
            self._getEventItineraries(updateDB)

        if not eventId and not itineraryLegId and not itinerarySectionId:
            q = "SELECT * FROM itinerary_controls;"
        elif itinerarySectionId:
            q = f"SELECT * FROM itinerary_controls WHERE itinerarySectionId={int(itinerarySectionId)};"
        elif itineraryLegId:
            q = f"SELECT * FROM itinerary_controls WHERE itineraryLegId={int(itineraryLegId)};"
        elif eventId:
            q = f"SELECT * FROM itinerary_controls WHERE eventId={int(eventId)};"

        itineraryControls_df = read_sql(q, self.conn)

        return itineraryControls_df

    def _getStartLists(self, startListId=None, updateDB=False):
        if not startListId:
            return DataFrame()

        stub = f"events/{self.eventId}/startLists/{startListId}.json"
        json_data = self._WRC_RedBull_json(stub)

        startlist_df = json_normalize(json_data["startListItems"])
        startlist_df["eventId"] = json_data["eventId"]
        startlist_df["name"] = json_data["name"]

        if updateDB:
            self.dbfy(startlist_df, "startlists", pk="startListItemId")

        return startlist_df

    def getStartList(self, eventId=None, startListId=None, raw=True, updateDB=False):
        # TO DO  - offer e.g. a day instead of startListId

        if updateDB and startListId:
            self._getStartLists(startListId=startListId)

        eventId = eventId if eventId else self.eventId
        _on_event = f"sl.eventId={eventId}" if eventId else "1=1"
        if raw:
            q = f"SELECT * FROM startlists AS sl WHERE {_on_event}"
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
            q = f"SELECT d.fullName AS driverName, cd.fullName AS codriverName, m.name AS manufacturerName, n.name AS entrantName, e.identifier AS carNo, e.vehicleModel, e.priority, e.eligibility, sl.* FROM startlists AS sl {_entry_join} {_driver_join} {_codriver_join} {_manufacturer_join} {_entrants_join} WHERE {_on_event}"
            if startListId:
                q = f"{q} AND startListId={startListId};"
            q = f"{q} ORDER BY sl.[order] ASC;"
            # We can also escape order (reserved word as column name)
            # using eg sl.[order] or sl."order" or  sl.`order`

        startlist_df = read_sql(q, self.conn)

        return startlist_df

    def _getEntries(self, updateDB=False):
        stub = f"events/{self.eventId}/rallies/{self.rallyId}/entries.json"
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

        entries_df["rallyId"] = self.rallyId
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

    def getEntries(self, on_event=True, updateDB=False):
        if updateDB:
            self._getEntries(updateDB)
        _on_event = (
            f"eventId={self.eventId} AND rallyId={self.rallyId}"
            if on_event and self.eventId and self.rallyId
            else "1=1"
        )
        q = f"SELECT * FROM entries AS e WHERE {_on_event};"
        entries_df = read_sql(q, self.conn)

        return entries_df

    def getDrivers(self, on_event=True, by_championship=False, updateDB=False):
        if updateDB:
            self._getEntries(updateDB)
        _on_event = (
            f"INNER JOIN entries AS e ON d.personId=e.driverId WHERE e.eventId={self.eventId} AND e.rallyId={self.rallyId}"
            if on_event and self.eventId and self.rallyId
            else ""
        )

        # I'm not sure this is right? Is the championshipId persistent over years?
        # For WRC, use priority (P1) and / or eligibility includes M? Or groupId for Rally1?
        # We want people registered in the championship when they are competing in an event.
        # We can perhaps get IDs by using the championshipId filtered results.json?
        _by_championship = ""
        if by_championship and self.championshipId:
            _by_championship = f"AND e.driverId IN (SELECT personId FROM championship_entries WHERE championshipId={self.championshipId})"
            if not on_event:
                # If not joining with entries, we need to join with entries first
                _on_event = (
                    f"INNER JOIN entries AS e ON d.personId=e.driverId WHERE 1=1"
                )

        where_clause = ""
        if _on_event:
            where_clause = _on_event + " " + _by_championship

        q = f"SELECT d.* FROM entries_drivers AS d {where_clause};"
        drivers_df = read_sql(q, self.conn)

        return drivers_df

    def getCoDrivers(self, on_event=True, updateDB=False):
        if updateDB:
            self._getEntries(updateDB)
        _on_event = (
            f"INNER JOIN entries AS e ON cd.personId=e.codriverId WHERE e.eventId={self.eventId} AND e.rallyId={self.rallyId}"
            if on_event and self.eventId and self.rallyId
            else ""
        )

        q = f"SELECT c.* FROM entries_codrivers AS cd {_on_event};"
        codrivers_df = read_sql(q, self.conn)

        return codrivers_df

    def getManufacturers(self, on_event=True, updateDB=False):
        if updateDB:
            self._getEntries(updateDB)
        _on_event = (
            f"INNER JOIN entries AS e ON m.manufacturerId=e.manufacturerId WHERE e.eventId={self.eventId} AND e.rallyId={self.rallyId}"
            if on_event and self.eventId and self.rallyId
            else ""
        )

        q = f"SELECT DISTINCT m.* FROM manufacturers AS m {_on_event};"
        manufacturers_df = read_sql(q, self.conn)

        return manufacturers_df

    def getEntrants(self, on_event=True, updateDB=False):
        if updateDB:
            self._getEntries(updateDB)
        _on_event = (
            f"INNER JOIN entries AS e ON n.entrantId=e.entrantId WHERE e.eventId={self.eventId} AND e.rallyId={self.rallyId}"
            if on_event and self.eventId and self.rallyId
            else ""
        )

        q = f"SELECT n.entrantId, n.name FROM entrants AS n {_on_event};"
        entrants_df = read_sql(q, self.conn)

        return entrants_df

    def getGroups(self, on_event=True, updateDB=False):
        if updateDB:
            self._getEntries(updateDB)
        _on_event = f"" if on_event else ""

        q = f"SELECT * FROM groups;"
        entryGroups_df = read_sql(q, self.conn)

        return entryGroups_df

    def _getEventShakeDownTimes(self, updateDB=False):
        if not self.eventId:
            return
        stub = f"events/{self.eventId}/shakedowntimes.json?shakedownNumber=1"
        json_data = self._WRC_RedBull_json(stub)
        shakedownTimes_df = DataFrame(json_data)

        if updateDB:
            self.dbfy(shakedownTimes_df, "shakedown_times", pk="shakedownTimeId")

        return shakedownTimes_df

    def _getStages(self, updateDB=False):
        stub = f"events/{self.eventId}/stages.json"
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

        return stages_df, stage_split_points_df, stage_controls_df

    def getStageInfo(self, on_event=True, raw=True, updateDB=False):
        if updateDB:
            self._getStages(updateDB)

        on_event_ = f"""eventId={self.eventId}""" if on_event else "1=1"
        if raw:
            q = f"SELECT * FROM stage_info AS i WHERE {on_event_};"
        else:
            q = f"SELECT * FROM stage_info AS i WHERE {on_event_};"

        stages_df = read_sql(q, self.conn)

        return stages_df

    def getStageSplitPoints(self, updateDB=False):
        if updateDB:
            self._getStages(updateDB)

        q = "SELECT * FROM split_points;"
        stage_split_points_df = read_sql(q, self.conn)

        return stage_split_points_df

    def getStageControls(self, updateDB=False, raw=True):
        if updateDB:
            self._getStages(updateDB)

        if raw:
            q = "SELECT * FROM stage_controls;"
        # TO DO a query that gives a "pretty" result

        stage_controls_df = read_sql(q, self.conn)

        return stage_controls_df

    def _getEvent(self, updateDB=False):
        """This also sets self.rallyId, self.itineraryId"""
        stub = f"events/{self.eventId}.json"
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
        _event_df = eventRallies_df[eventRallies_df["isMain"] == True].iloc[0]

        self.rallyId = int(_event_df["rallyId"])
        self.itineraryId = int(_event_df["itineraryId"])

        if updateDB:
            self.dbfy(eventClasses_df, "event_classes", pk=("eventId", "eventClassId"))
            self.dbfy(eventRallies_df, "event_rallies", pk="itineraryId")
            self.dbfy(eventData_df, "event_date", pk="eventId")

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

    def setEventById(self, eventId=None, updateDB=True):
        if not eventId:
            return
        # Do this as a fuzzy search?
        q = f'SELECT eventId, name FROM season_rounds WHERE eventId="{eventId}";'
        r = read_sql(q, self.conn)
        # HACK TO DO this is a fudge
        self._setEvent(r, updateDB)

    def setEventByName(self, name=None, updateDB=True):
        if not name:
            return
        # Do this as a fuzzy search?
        q = f'SELECT eventId, name FROM season_rounds WHERE name="{name}";'
        r = read_sql(q, self.conn)
        # HACK TO DO this is a fudge
        self._setEvent(r, updateDB)

    def setChampionship(self):
        championships_df = self.getChampionships()
        self.championshipName = self._getChampionshipName()
        # TO DO - defend against brokenness here
        self.championshipId = int(
            championships_df[championships_df["name"] == self.championshipName].iloc[0][
                "championshipId"
            ]
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
        r = read_sql(q, self.conn)
        self._setStage(r, updateDB=updateDB)

    def setStageByCode(self, stageCode=None, updateDB=True):
        # TO DO - we need to clobber the update if the table is complete
        self._getStages(updateDB=updateDB)
        if not stageCode:
            return
        # Do this as a fuzzy search?
        q = f'SELECT stageId, name, code FROM stage_info WHERE code="{stageCode}";'
        r = read_sql(q, self.conn)
        self._setStage(r, updateDB=updateDB)

    # TO DO a way of setting self.controlId; also need strategies for invalidating Ids
    def _getControlTimes(self, updateDB=False):
        stub = f"events/{self.eventId}/controls/{self.controlId}/controlTimes.json"
        json_data = self._WRC_RedBull_json(stub)
        controlTimes_df = DataFrame(json_data)
        if controlTimes_df.empty:
            return DataFrame()

        if updateDB:
            self.dbfy(controlTimes_df, "controltimes", pk="controlTimeId")

        return controlTimes_df

    def _getStageTimes(self, stageId=None, updateDB=False):
        stageId = stageId if stageId else self.stageId
        stub = f"events/{self.eventId}/stages/{stageId}/stagetimes.json?rallyId={self.rallyId}"
        json_data = self._WRC_RedBull_json(stub)
        stagetimes_df = DataFrame(json_data)
        if stagetimes_df.empty:
            return DataFrame()

        stagetimes_df["eventId"] = self.eventId
        stagetimes_df["rallyId"] = self.rallyId

        if updateDB:
            self.dbfy(stagetimes_df, "stage_times", pk="stageTimeId")

        return stagetimes_df

    def getStageTimes(self, stageId=None, raw=True, updateDB=False):
        if updateDB:
            self._getStageTimes(stageId=stageId)

        stageId = stageId if stageId else self.stageId
        if stageId and self.eventId and self.rallyId:
            on_event_ = f"st.eventId={self.eventId} AND st.stageId={stageId} AND st.rallyId={self.rallyId}"
            if raw:
                sql = f"""SELECT * FROM stage_times AS st WHERE {on_event_};"""
            else:
                _entry_join = f"INNER JOIN entries AS e ON st.entryId=e.entryId"
                _driver_join = (
                    f"INNER JOIN entries_drivers AS d ON e.driverId=d.personId"
                )
                sql = f"SELECT d.fullName AS driverName, e.vehicleModel, st.* FROM stage_times AS st {_entry_join} {_driver_join} WHERE {on_event_};"

            r = read_sql(sql, self.conn)
            # Hack to poll API if empty
            if r.empty:
                self._getStageTimes(stageId=stageId, updateDB=True)
                r = read_sql(sql, self.conn)
        else:
            print(f"No getStageTimes? {self.eventId} {self.stageId} {self.rallyId}")
            r = DataFrame()
        return r

    def _getSplitTimes(self, stageId=None, updateDB=False):
        stageId = stageId if stageId else self.stageId
        stub = f"events/{self.eventId}/stages/{stageId}/splittimes.json?rallyId={self.rallyId}"
        json_data = self._WRC_RedBull_json(stub)
        splitTimes_df = DataFrame(json_data)
        if splitTimes_df.empty:
            return DataFrame()
        
        splitTimes_df["stageId"] = stageId
        splitTimes_df["eventId"] = self.eventId
        splitTimes_df["rallyId"] = self.rallyId

        if updateDB:
            self.dbfy(splitTimes_df, "split_times", pk="splitPointTimeId")

        return splitTimes_df

    def getSplitTimes(self, stageId=None, raw=True, updateDB=False):
        if updateDB:
            self._getSplitTimes(stageId=stageId)
        if stageId and self.eventId and self.rallyId:
            on_event_ = f"sp.eventId={self.eventId} AND sp.stageId={stageId} AND sp.rallyId={self.rallyId}"
            if raw:
                sql = f"""SELECT * FROM split_times AS sp WHERE {on_event_};"""
            else:
                _entry_join = f"INNER JOIN entries AS e ON sp.entryId=e.entryId"
                _driver_join = (
                    f"INNER JOIN entries_drivers AS d ON e.driverId=d.personId"
                )
                sql = f"SELECT d.fullName AS driverName, e.vehicleModel, sp.* FROM split_times AS sp {_entry_join} {_driver_join} WHERE {on_event_};"

            r = read_sql(sql, self.conn)
            # Hack to poll API if empty
            if r.empty:
                self._getSplitTimes(stageId=stageId, updateDB=True)
                r = read_sql(sql, self.conn)
        else:
            print(f"No getSplitTimes? {self.eventId} {self.stageId} {self.rallyId}")
            r = DataFrame()

        return r

    def _getStageOverallResults(
        self, stageId=None, by_championship=False, updateDB=False
    ):
        """This is the overall result at the end of the stage. TO DO CHECK"""
        # TO DO: if we select by championship, are these different?
        # If so, do we need to set championship and championship Pos cols, maybe in a new table?
        # The rallyId is optional? Or does it filter somehow?
        stageId = stageId if stageId else self.stageId
        stub = f"events/{self.eventId}/stages/{stageId}/results.json?rallyId={self.rallyId}"
        if by_championship and self.championshipId:
            stub = stub + f"&championshipId={self.championshipId}"
        json_data = self._WRC_RedBull_json(stub)
        stageResults_df = DataFrame(json_data)
        if stageResults_df.empty:
            return DataFrame()

        stageResults_df["stageId"] = stageId
        stageResults_df["eventId"] = self.eventId
        stageResults_df["rallyId"] = self.rallyId

        if updateDB:
            self.dbfy(stageResults_df, "stage_overall", pk=("stageId", "entryId"))

        return stageResults_df

    def getStageOverallResults(self, stageId=None, raw=True, updateDB=False):
        if updateDB:
            self._getStageOverallResults(stageId=stageId)

        stageId = stageId if stageId else self.stageId
        if self.eventId and stageId and self.rallyId:
            on_event_ = f"o.eventId={self.eventId} AND o.stageId={stageId} AND o.rallyId={self.rallyId}"
            if raw:
                sql = f"""SELECT * FROM stage_overall AS o WHERE {on_event_};"""
            else:
                _entry_join = f"INNER JOIN entries AS e ON o.entryId=e.entryId"
                _driver_join = (
                    f"INNER JOIN entries_drivers AS d ON e.driverId=d.personId"
                )
                sql = f"SELECT d.fullName AS driverName, e.vehicleModel, o.* FROM stage_overall AS o {_entry_join} {_driver_join} WHERE {on_event_};"
            r = read_sql(sql, self.conn)
            # Hack to poll API if empty
            if r.empty:
                self._getStageOverallResults(stageId=stageId, updateDB=True)
                r = read_sql(sql, self.conn)
        else:
            print(
                f"No getStageOverallResults? {self.eventId} {self.stageId} {self.rallyId}"
            )
            r = DataFrame()
        return r

    def _getStageWinners(self, updateDB=False):
        stub = f"events/{self.eventId}/rallies/{self.rallyId}/stagewinners.json"
        json_data = self._WRC_RedBull_json(stub)
        stagewinners_df = DataFrame(json_data)
        if stagewinners_df.empty:
            return DataFrame()

        stagewinners_df["eventId"] = self.eventId
        stagewinners_df["rallyId"] = self.rallyId
        if updateDB:
            self.dbfy(stagewinners_df, "stagewinners", pk="stageId")

        return stagewinners_df

    def _getRetirements(self, updateDB=False):
        if not self.eventId:
            return
        stub = f"events/{self.eventId}/retirements.json"
        json_data = self._WRC_RedBull_json(stub)
        retirements_df = DataFrame(json_data)
        if retirements_df.empty:
            return DataFrame()

        retirements_df["eventId"] = self.eventId
        if updateDB:
            self.dbfy(retirements_df, "retirements", pk="retirementId")
        return retirements_df

    def _getPenalties(self, updateDB=False):
        if not self.eventId:
            return
        stub = f"events/{self.eventId}/penalties.json"
        json_data = self._WRC_RedBull_json(stub)
        penalties_df = DataFrame(json_data)
        if penalties_df.empty:
            return DataFrame()

        penalties_df["eventId"] = self.eventId
        if updateDB:
            self.dbfy(penalties_df, "penalties", pk="penaltyId")
        return penalties_df

    def getStageWinners(self, on_event=True, raw=True):
        if not self.eventId or not self.rallyId:
            return DataFrame()

        _on_event = (
            f"""w.eventId={self.eventId} AND w.rallyId={self.rallyId}"""
            if on_event
            else ""
        )

        if raw:
            sql = f"""SELECT * FROM stagewinners AS w WHERE {_on_event};"""
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
            sql = f"""SELECT d.fullName AS driverName, cd.fullName AS codriverName, m.name AS manufacturerName, n.name AS entrantName, e.identifier AS carNo, e.vehicleModel, it_st.code, it_se.name AS sectionName, it_l.name AS day, st.distance, w.* FROM stagewinners AS w {_entry_join} {_stages_join} {_driver_join} {_codriver_join} {_manufacturer_join} {_entrants_join} {_itinerary_stages_join} {_itinerary_sections_join} {_itinerary_legs_join} WHERE {_on_event};"""

        r = read_sql(sql, self.conn)
        # Hack to poll API if empty
        if r.empty:
            self._getStageWinners(updateDB=True)
            r = read_sql(sql, self.conn)
        return r

    # TO DO - offer more search limits
    def getRetirements(self, on_event=True, raw=True, updateDB=False):
        if updateDB:
            self._getRetirements(updateDB=updateDB)

        _on_event = f"r.eventId={self.eventId}" if on_event and self.eventId else ""

        if raw:
            _on_event = f"WHERE {_on_event}" if _on_event else _on_event
            sql = f"""SELECT * FROM retirements r {_on_event};"""
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
            _on_event = f"WHERE {_on_event}" if _on_event else _on_event
            sql = f"""SELECT d.fullName AS driverName, cd.fullName AS codriverName, m.name AS manufacturerName, n.name AS entrantName, e.identifier AS carNo, e.vehicleModel, c.code, r.reason, c.location, c.type, r.retirementDateTime, r.status FROM retirements r {_entry_join} {_driver_join} {_codriver_join} {_manufacturer_join} {_entrants_join} {_control_join} {_on_event};"""

        r = read_sql(sql, self.conn)
        # Hack to poll API if empty
        if r.empty:
            self._getRetirements(updateDB=True)
            r = read_sql(sql, self.conn)
        return r

    # TO DO - offer more search limits
    def getPenalties(self, on_event=True, raw=True, updateDB=False):
        if updateDB:
            self._getPenalties(updateDB=updateDB)

        _on_event = f"p.eventId={self.eventId}" if on_event and self.eventId else ""

        if raw:
            _on_event = f"WHERE {_on_event}" if _on_event else _on_event
            sql = f"""SELECT * FROM penalties AS p {_on_event};"""
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
            _on_event = f"WHERE {_on_event}" if _on_event else _on_event
            sql = f"""SELECT d.fullName AS driverName, cd.fullName AS codriverName, m.name AS manufacturerName, n.name AS entrantName, e.identifier AS carNo, e.vehicleModel, c.code, p.penaltyDuration, p.Reason, c.location, c.type FROM penalties AS p {_entry_join} {_driver_join} {_codriver_join} {_manufacturer_join} {_entrants_join} {_control_join} {_on_event};"""

        r = read_sql(sql, self.conn)
        # Hack to poll API if empty
        if r.empty:
            self._getPenalties(updateDB=True)
            r = read_sql(sql, self.conn)
        return r

    def query(self, sql):
        r = read_sql(sql, self.conn)
        return r