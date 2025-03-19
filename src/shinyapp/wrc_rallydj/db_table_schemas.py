## V2 SCHEMA

# TO DO - check FK references

SETUP_V2_Q = """

CREATE TABLE "seasons" (
  "seasonId" INTEGER PRIMARY KEY,
  "name" TEXT,
  "year" INTEGER
);

CREATE TABLE "championship_lookup" (
  "championshipId" INTEGER PRIMARY KEY,
  "name" TEXT,
  "seasonId" INTEGER,
  "type" TEXT,
  "fieldOneDescription" TEXT,
  "fieldTwoDescription" TEXT,
  "fieldThreeDescription" TEXT,
  "fieldFiveDescription" TEXT,
  "fieldFourDescription" TEXT
);

CREATE TABLE "season_rounds" (
  "categories" TEXT,
  "clerkOfTheCourse" TEXT,
  "country.countryId" INTEGER,
  "country.iso2" TEXT,
  "country.iso3" TEXT,
  "country.name" TEXT,
  "countryId" INTEGER,
  "eventId" INTEGER PRIMARY KEY,
  "finishDate" TEXT,
  "location" TEXT,
  "mode" TEXT,
  "name" TEXT,
  "organiserUrl" TEXT,
  "slug" TEXT,
  "startDate" TEXT,
  "stewards" TEXT,
  "surfaces" TEXT,
  "templateFilename" TEXT,
  "timeZoneId" TEXT,
  "timeZoneName" TEXT,
  "timeZoneOffset" INTEGER,
  "trackingEventId" INTEGER ,
  "order" INTEGER,
  "seasonId" INTEGER,
  FOREIGN KEY ("eventId") REFERENCES "itinerary_event" ("eventId")
);


CREATE TABLE "stage_controls" (
  "code" TEXT,
  "controlId" INTEGER PRIMARY KEY,
  "controlPenalties" TEXT,
  "distance" REAL,
  "eventId" INTEGER,
  "firstCarDueDateTime" TEXT,
  "firstCarDueDateTimeLocal" TEXT,
  "location" TEXT,
  "stageId" INTEGER,
  "status" TEXT,
  "targetDuration" TEXT,
  "targetDurationMs" INTEGER,
  "timingPrecision" TEXT,
  "type" TEXT,
  "bogey" TEXT,
  "bogeyMs" INTEGER,
  "roundingPolicy" TEXT
);

CREATE TABLE "split_points" (
  "splitPointId" INTEGER PRIMARY KEY,
  "stageId" INTEGER,
  "number" INTEGER,
  "distance" REAL
);

CREATE TABLE "stage_info" (
  "code" TEXT,
  "distance" REAL,
  "eventId" INTEGER,
  "name" TEXT,
  "number" INTEGER,
  "stageId" INTEGER PRIMARY KEY,
  "stageType" TEXT,
  "status" TEXT,
  "timingPrecision" TEXT,
  "locked" TEXT
);

CREATE TABLE "shakedown_times" (
  "shakedownTimeId" INTEGER PRIMARY KEY,
  "eventId" INTEGER,
  "entryId" INTEGER,
  "runNumber" INTEGER,
  "shakedownNumber" INTEGER,
  "runDuration" TEXT,
  "runDurationMs" INTEGER
);

CREATE TABLE "stage_overall" (
  "diffFirst" TEXT,
  "diffFirstMs" INTEGER,
  "diffPrev" TEXT,
  "diffPrevMs" INTEGER,
  "entryId" INTEGER,
  "penaltyTime" TEXT,
  "penaltyTimeMs" INTEGER,
  "position" INTEGER,
  "stageTime" TEXT,
  "stageTimeMs" INTEGER,
  "totalTime" TEXT,
  "totalTimeMs" INTEGER,
  "stageId" INTEGER,
  "eventId" INTEGER,
  PRIMARY KEY ("stageId", "entryId"),
  FOREIGN KEY ("stageId") REFERENCES "itinerary_stages" ("stageId"),
  FOREIGN KEY ("entryId") REFERENCES "startlists" ("entryId")
);

CREATE TABLE "penalties" (
  "controlId" INTEGER,
  "entryId" INTEGER,
  "penaltyDuration" TEXT,
  "penaltyDurationMs" INTEGER,
  "penaltyId" INTEGER,
  "reason" TEXT,
  "eventId" INTEGER,
  PRIMARY KEY ("penaltyId"),
  FOREIGN KEY ("entryId") REFERENCES "startlists" ("entryId")
);

CREATE TABLE "retirements" (
  "controlId" INTEGER,
  "entryId" INTEGER,
  "reason" TEXT,
  "retirementDateTime" TEXT,
  "retirementDateTimeLocal" TEXT,
  "retirementId" INTEGER,
  "status" TEXT,
  "eventId" INTEGER,
  PRIMARY KEY ("retirementId"),
  FOREIGN KEY ("entryId") REFERENCES "startlists" ("entryId")
);

CREATE TABLE "stagewinners" (
  "elapsedDuration" TEXT,
  "elapsedDurationMs" INTEGER,
  "entryId" INTEGER,
  "stageId" INTEGER,
  "stageName" TEXT,
  "eventId" INTEGER,
  "rallyId" INTEGER,
  PRIMARY KEY ("stageId"),
  FOREIGN KEY ("entryId") REFERENCES "startlists" ("entryId"),
  FOREIGN KEY ("stageId") REFERENCES "itinerary_stages" ("stageId")
);

CREATE TABLE "controltimes" (
  "controlTimeId" INTEGER PRIMARY KEY,
  "controlId" INTEGER,
  "entryId" INTEGER,
  "dueDateTime" TEXT,
  "dueDateTimeLocal" TEXT,
  "actualDateTime" TEXT,
  "actualDateTimeLocal" TEXT,
  "absoluteActualDateTime" TEXT,
  "absoluteActualDateTimeLocal" TEXT,
  "source" TEXT,
  "status" TEXT
);

CREATE TABLE "split_times" (
  "elapsedDuration" TEXT,
  "elapsedDurationMs" INTEGER,
  "entryId" INTEGER,
  "splitDateTime" TEXT,
  "splitDateTimeLocal" TEXT,
  "splitPointId" INTEGER,
  "splitPointTimeId" INTEGER PRIMARY KEY,
  "stageTimeDuration" TEXT,
  "stageTimeDurationMs" REAL,
  "startDateTime" TEXT,
  "startDateTimeLocal" TEXT,
  "stageId" INTEGER,
  "eventId" INTEGER,
  FOREIGN KEY ("stageId") REFERENCES "itinerary_stages" ("stageId"),
  FOREIGN KEY ("entryId") REFERENCES "startlists" ("entryId")
);
"""

# OLD BELOW

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


# SQL in wrcResults.sql
SETUP_Q = """
CREATE TABLE "itinerary_event" (
  "eventId" INTEGER,
  "itineraryId" INTEGER PRIMARY KEY,
  "name" TEXT,
  "priority" INTEGER
);
CREATE TABLE "itinerary_legs" (
  "itineraryId" INTEGER,
  "itineraryLegId" INTEGER PRIMARY KEY,
  "legDate" TEXT,
  "name" TEXT,
  "order" INTEGER,
  "startListId" INTEGER,
  "status" TEXT,
  FOREIGN KEY ("itineraryId") REFERENCES "itinerary_event" ("itineraryId")
);
CREATE TABLE "itinerary_sections" (
  "itineraryLegId" INTEGER,
  "itinerarySectionId" INTEGER PRIMARY KEY,
  "name" TEXT,
  "order" INTEGER,
  FOREIGN KEY ("itineraryLegId") REFERENCES "itinerary_legs" ("itineraryLegId")
);

CREATE TABLE "startlists" (
  "codriver.abbvName" TEXT,
  "codriver.code" TEXT,
  "codriver.country.countryId" INTEGER,
  "codriver.country.iso2" TEXT,
  "codriver.country.iso3" TEXT,
  "codriver.country.name" TEXT,
  "codriver.countryId" INTEGER,
  "codriver.firstName" TEXT,
  "codriver.fullName" TEXT,
  "codriver.lastName" TEXT,
  "codriver.personId" INTEGER,
  "codriverId" INTEGER,
  "driver.abbvName" TEXT,
  "driver.code" TEXT,
  "driver.country.countryId" INTEGER,
  "driver.country.iso2" TEXT,
  "driver.country.iso3" TEXT,
  "driver.country.name" TEXT,
  "driver.countryId" INTEGER,
  "driver.firstName" TEXT,
  "driver.fullName" TEXT,
  "driver.lastName" TEXT,
  "driver.personId" INTEGER,
  "driverId" INTEGER,
  "eligibility" TEXT,
  "entrant.entrantId" INTEGER,
  "entrant.logoFilename" TEXT,
  "entrant.name" TEXT,
  "entrantId" INTEGER,
  "entryId" INTEGER PRIMARY KEY,
  "eventId" INTEGER,
  "group.name" TEXT,
  "groupId" INTEGER,
  "group.groupId" INTEGER,
  "identifier" TEXT,
  "manufacturer.logoFilename" TEXT,
  "manufacturer.manufacturerId" INTEGER,
  "manufacturer.name" TEXT,
  "manufacturerId" INTEGER,
  "priority" TEXT,
  "status" TEXT,
  "tag" TEXT,
  "tag.name" TEXT,
  "tag.tagId" INTEGER,
  "tagId" INTEGER,
  "tyreManufacturer" TEXT,
  "vehicleModel" TEXT,
  "entryListOrder" INTEGER,
  FOREIGN KEY ("eventId") REFERENCES "itinerary_event" ("eventId")
);
CREATE TABLE "roster" (
  "fiasn" INTEGER,
  "code" TEXT,
  "sas-entryid" INTEGER PRIMARY KEY,
  "roster_num" INTEGER,
  FOREIGN KEY ("sas-entryid") REFERENCES "startlists" ("entryId")
);
CREATE TABLE "startlist_classes" (
  "eventClassId" INTEGER,
  "eventId" INTEGER,
  "name" TEXT,
  "entryId" INTEGER,
  PRIMARY KEY ("eventClassId","entryId"),
  FOREIGN KEY ("eventId") REFERENCES "itinerary_event" ("eventId"),
  FOREIGN KEY ("entryId") REFERENCES "startlists" ("entryId")
);



CREATE TABLE "stage_times_stage" (
  "diffFirst" TEXT,
  "diffFirstMs" INTEGER,
  "diffPrev" TEXT,
  "diffPrevMs" INTEGER,
  "elapsedDuration" TEXT,
  "elapsedDurationMs" INTEGER,
  "entryId" INTEGER,
  "position" INTEGER,
  "source" TEXT,
  "stageId" INTEGER,
  "stageTimeId" INTEGER PRIMARY KEY,
  "status" TEXT,
  FOREIGN KEY ("stageId") REFERENCES "itinerary_stages" ("stageId"),
  FOREIGN KEY ("entryId") REFERENCES "startlists" ("entryId")
);
CREATE TABLE "stage_times_overall" (
  "diffFirst" TEXT,
  "diffFirstMs" INTEGER,
  "diffPrev" TEXT,
  "diffPrevMs" INTEGER,
  "entryId" INTEGER,
  "penaltyTime" TEXT,
  "penaltyTimeMs" INTEGER,
  "position" INTEGER,
  "stageTime" TEXT,
  "stageTimeMs" INTEGER,
  "totalTime" TEXT,
  "totalTimeMs" INTEGER,
  "stageId" INTEGER,
  PRIMARY KEY ("stageId","entryId"),
  FOREIGN KEY ("stageId") REFERENCES "itinerary_stages" ("stageId"),
  FOREIGN KEY ("entryId") REFERENCES "startlists" ("entryId")
);

CREATE TABLE "championship_results" (
  "championshipEntryId" INTEGER,
  "championshipId" INTEGER,
  "dropped" INTEGER,
  "eventId" INTEGER,
  "pointsBreakdown" TEXT,
  "position" INTEGER,
  "publishedStatus" TEXT,
  "status" TEXT,
  "totalPoints" INTEGER,
  PRIMARY KEY ("championshipEntryId","eventId"),
  FOREIGN KEY ("championshipId") REFERENCES "championship_lookup" ("championshipId"),
  FOREIGN KEY ("eventId") REFERENCES "itinerary_event" ("eventId")
);
CREATE TABLE "championship_entries_codrivers" (
  "championshipEntryId" INTEGER PRIMARY KEY,
  "championshipId" INTEGER,
  "entrantId" TEXT,
  "ManufacturerTyre" TEXT,
  "Manufacturer" TEXT,
  "FirstName" TEXT,
  "CountryISO3" TEXT,
  "CountryISO2" TEXT,
  "LastName" TEXT,
  "manufacturerId" INTEGER,
  "personId" INTEGER,
  "tyreManufacturer" TEXT,
  FOREIGN KEY ("championshipId") REFERENCES "championship_lookup" ("championshipId")
);
CREATE TABLE "championship_entries_manufacturers" (
  "championshipEntryId" INTEGER PRIMARY KEY ,
  "championshipId" INTEGER,
  "entrantId" INTEGER,
  "Name" TEXT,
  "LogoFileName" TEXT,
  "Manufacturer" TEXT,
  "manufacturerId" INTEGER,
  "personId" TEXT,
  "tyreManufacturer" TEXT,
  FOREIGN KEY ("championshipId") REFERENCES "championship_lookup" ("championshipId")
);
CREATE TABLE "championship_rounds" (
  "championshipId" INTEGER,
  "eventId" INTEGER,
  "order" INTEGER,
  PRIMARY KEY ("championshipId","eventId"),
  FOREIGN KEY ("championshipId") REFERENCES "championship_lookup" ("championshipId"),
  FOREIGN KEY ("eventId") REFERENCES "itinerary_event" ("eventId")
);
CREATE TABLE "championship_entries_drivers" (
  "championshipEntryId" INTEGER PRIMARY KEY ,
  "championshipId" INTEGER,
  "entrantId" TEXT,
  "ManufacturerTyre" TEXT,
  "Manufacturer" TEXT,
  "FirstName" TEXT,
  "CountryISO3" TEXT,
  "CountryISO2" TEXT,
  "LastName" TEXT,
  "manufacturerId" INTEGER,
  "personId" INTEGER,
  "tyreManufacturer" TEXT,
  FOREIGN KEY ("championshipId") REFERENCES "championship_lookup" ("championshipId")
);
CREATE TABLE "event_metadata" (
  "_id" TEXT,
  "availability" TEXT,
  "date-finish" TEXT,
  "date-start" TEXT,
  "gallery" TEXT,
  "hasdata" TEXT,
  "hasfootage" TEXT,
  "hasvideos" TEXT,
  "id" TEXT,
  "info-based" TEXT,
  "info-categories" TEXT,
  "info-date" TEXT,
  "info-flag" TEXT,
  "info-surface" TEXT,
  "info-website" TEXT,
  "kmlfile" TEXT,
  "logo" TEXT,
  "name" TEXT,
  "org-website" TEXT,
  "poi-Klo im Wald" TEXT,
  "poilistid" TEXT,
  "position" TEXT,
  "rosterid" TEXT,
  "sas-eventid" TEXT,
  "sas-itineraryid" TEXT,
  "sas-rallyid" TEXT,
  "sas-trackingid" TEXT,
  "sitid" TEXT,
  "testid" TEXT,
  "thumbnail" TEXT,
  "time-zone" TEXT,
  "tzoffset" TEXT,
  "year" INTEGER
);
"""
