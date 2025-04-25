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

CREATE TABLE "championship_countries" (
  "countryId" INTEGER PRIMARY KEY,
  "name" TEXT, 
  "iso2" TEXT,
  "iso3" TEXT
);

CREATE TABLE "championship_rounds_detail" (
  "eventId" INTEGER PRIMARY KEY,
  "countryId" INTEGER,
  "eventProfileId" INTEGER,
  "country" TEXT,
  "name" TEXT,
  "slug" TEXT,
  "location" TEXT,
  "startDate" TEXT,
  "finishDate" TEXT,
  "year" INTEGER,
  "Round" INTEGER,
  "timeZoneId" TEXT,
  "timeZoneName" TEXT,
  "timeZoneOffset" INTEGER,
  "surfaces" TEXT,
  "organiserUrl" TEXT,
  "categories" TEXT,
  "mode" TEXT,
  "trackingEventId" INTEGER,
  "clerkOfTheCourse" TEXT,
  "stewards" TEXT,
  "templateFilename" TEXT,
  "locked" TEXT,
  "public" TEXT,
  "applyJumpStarts" TEXT,
  "displayFiaColumns" TEXT,
  "roundShakedownTimes" TEXT,
  "shakedownCount" TEXT,
  "displayLicenseNumber" TEXT
);

CREATE TABLE "championship_overall" (
  "championshipId" INTEGER,
  "championshipEntryId" INTEGER,
  "seasonId" INTEGER,
  "overallPosition" INTEGER,
  "overallPoints" INTEGER,
  "Round" INTEGER,
  "eventId" INTEGER,
  PRIMARY KEY ("championshipEntryId", "championshipId", "eventId"),
  FOREIGN KEY ("championshipId") REFERENCES "championship_lookup" ("championshipId")
);

CREATE TABLE "championship_results" (
  "championshipEntryId" INTEGER,
  "championshipId" INTEGER,
  "seasonId" INTEGER,
  "dropped" INTEGER,
  "eventId" INTEGER,
  "pointsBreakdown" TEXT,
  "position" INTEGER,
  "publishedStatus" TEXT,
  "status" TEXT,
  "totalPoints" INTEGER,
  "entryId" INTEGER,
  PRIMARY KEY ("championshipEntryId", "eventId"),
  FOREIGN KEY ("championshipId") REFERENCES "championship_lookup" ("championshipId"),
  FOREIGN KEY ("eventId") REFERENCES "itinerary_event" ("eventId")
);



CREATE TABLE "championship_entries" (
  "championshipEntryId" INTEGER PRIMARY KEY,
  "championshipId" INTEGER,
  "personId" INTEGER,
  "entrantId" INTEGER,
  "manufacturerId" INTEGER,
  "FirstName" TEXT,
  "LastName" TEXT,
  "CountryISO3" TEXT,
  "Manufacturer" TEXT,
  "TyreManufacturer" TEXT,
  "Name" TEXT
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

CREATE TABLE "event_classes" (
  "eventClassId" INTEGER,
  "eventId" INTEGER,
  "name" TEXT,
  PRIMARY KEY ("eventId", "eventClassId")
);


CREATE TABLE "groups" (
  "groupId" INTEGER PRIMARY KEY,
  "name" TEXT
);

CREATE TABLE "entrants" (
  "entrantId" INTEGER PRIMARY KEY,
  "name" TEXT,
  "logoFilename" TEXT
);


CREATE TABLE "manufacturers" (
  "manufacturerId" INTEGER PRIMARY KEY,
  "name" TEXT,
  "logoFilename" TEXT
);

CREATE TABLE "entries" (
  "entryId" INTEGER PRIMARY KEY,
  "eventId" INTEGER,
  "rallyId" INTEGER,
  "driverId" INTEGER,
  "codriverId" INTEGER,
  "manufacturerId" INTEGER,
  "entrantId" INTEGER,
  "groupId" INTEGER,
  "identifier" INTEGER,
  "vehicleModel" TEXT,
  "entryListOrder" INTEGER,
  "pbf" TEXT,
  "drive" TEXT,
  "eligibility" TEXT,
  "priority" TEXT,
  "status" TEXT,
  "tyreManufacturer" TEXT
);

CREATE TABLE "entries_drivers" (
  "personId" INTEGER PRIMARY KEY,
  "countryId" INTEGER,
  "state" TEXT,
  "firstName" TEXT,
  "lastName" TEXT,
  "abbvName" TEXT,
  "fullName" TEXT,
  "code" TEXT,
  "licenseNumber" TEXT,
  "country.countryId" TEXT,
  "country.name" TEXT,
  "country.iso2" TEXT,
  "country.iso3" TEXT
);

CREATE TABLE "entries_codrivers" (
  "personId" INTEGER PRIMARY KEY,
  "countryId" INTEGER,
  "state" TEXT,
  "firstName" TEXT,
  "lastName" TEXT,
  "abbvName" TEXT,
  "fullName" TEXT,
  "code" TEXT,
  "licenseNumber" TEXT,
  "country.countryId" TEXT,
  "country.name" TEXT,
  "country.iso2" TEXT,
  "country.iso3" TEXT
);

CREATE TABLE "event_rallies" (
  "rallyId" INTEGER,
  "eventId" INTEGER,
  "itineraryId" INTEGER PRIMARY KEY,
  "name" TEXT,
  "isMain" TEXT
);

CREATE TABLE "event_date" (
  "eventId" INTEGER PRIMARY KEY,
  "countryId" INTEGER,
  "eventProfileId" INTEGER,
  "name" TEXT,
  "slug" TEXT,
  "location" TEXT,
  "startDate" TEXT,
  "finishDate" TEXT,
  "timeZoneId" TEXT,
  "timeZoneName" TEXT,
  "timeZoneOffset" INTEGER,
  "surfaces" TEXT,
  "organiserUrl" TEXT,
  "categories" TEXT,
  "mode" TEXT,
  "trackingEventId" INTEGER,
  "clerkOfTheCourse" TEXT,
  "stewards" TEXT,
  "templateFilename" TEXT,
  "locked" TEXT,
  "public" TEXT,
  "applyJumpStarts" TEXT,
  "displayFiaColumns" TEXT,
  "roundShakedownTimes" TEXT,
  "shakedownCount" TEXT,
  "displayLicenseNumber" TEXT,
  "country.countryId" INTEGER,
  "country.name" TEXT,
  "country.iso2" TEXT,
  "country.iso3" TEXT
);


CREATE TABLE "itinerary_legs" (
  "itineraryId" INTEGER,
  "itineraryLegId" INTEGER PRIMARY KEY,
  "eventId" INTEGER,
  "legDate" TEXT,
  "name" TEXT,
  "order" INTEGER,
  "startListId" INTEGER,
  "status" TEXT,
  FOREIGN KEY ("itineraryId") REFERENCES "itinerary_event" ("itineraryId")
);


CREATE TABLE "itinerary_stages" (
  "itinerarySectionId" INTEGER,
  "itineraryLegId" INTEGER,
  "order" INTEGER,
  "name" TEXT,
  "name_" TEXT,
  "stageId" INTEGER PRIMARY KEY,
  "eventId" INTEGER,
  "number" INTEGER,
  "distance" REAL,
  "status" TEXT,
  "stageType" TEXT,
  "timingPrecision" TEXT,
  "locked" TEXT,
  "code" TEXT
);

CREATE TABLE "itinerary_controls" (
  "itinerarySectionId" INTEGER,
  "itineraryLegId" INTEGER,
  "order" INTEGER,
  "name" TEXT,
  "controlId" INTEGER PRIMARY KEY,
  "eventId" INTEGER,
  "stageId" INTEGER,
  "type" TEXT,
  "code" TEXT,
  "location" TEXT,
  "timingPrecision" TEXT,
  "distance" REAL,
  "targetDuration" TEXT,
  "targetDurationMs" INTEGER,
  "firstCarDueDateTime" TEXT,
  "firstCarDueDateTimeLocal" TEXT,
  "status" TEXT,
  "controlPenalties" TEXT,
  "roundingPolicy" TEXT,
  "locked" TEXT,
  "bogey" TEXT,
  "bogeyMs" INTEGER
);

CREATE TABLE "itinerary_sections" (
  "eventId" INTEGER,
  "itineraryLegId" INTEGER,
  "itinerarySectionId" INTEGER PRIMARY KEY,
  "name" TEXT,
  "order" INTEGER,
  FOREIGN KEY ("itineraryLegId") REFERENCES "itinerary_legs" ("itineraryLegId")
);

CREATE TABLE "startlists" (
  "startListItemId" INTEGER PRIMARY KEY,
  "startListId" INTEGER,
  "entryId" INTEGER,
  "startDateTime" TEXT,
  "startDateTimeLocal" TEXT,
  "order" INTEGER,
  "eventId" INTEGER,
  "name" TEXT
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
  "locked" TEXT,
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
  "rallyId" INTEGER,
  PRIMARY KEY ("stageId", "entryId"),
  FOREIGN KEY ("stageId") REFERENCES "itinerary_stages" ("stageId"),
  FOREIGN KEY ("entryId") REFERENCES "entries" ("entryId")
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
  FOREIGN KEY ("entryId") REFERENCES "entries" ("entryId")
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
  FOREIGN KEY ("entryId") REFERENCES "entries" ("entryId")
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
  FOREIGN KEY ("entryId") REFERENCES "entries" ("entryId"),
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

CREATE TABLE "stage_times" (
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
  "eventId" INTEGER,
  "rallyId" INTEGER,
  FOREIGN KEY ("stageId") REFERENCES "itinerary_stages" ("stageId"),
  FOREIGN KEY ("entryId") REFERENCES "entries" ("entryId")
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
  "rallyId" INTEGER,
  FOREIGN KEY ("stageId") REFERENCES "itinerary_stages" ("stageId"),
  FOREIGN KEY ("entryId") REFERENCES "entries" ("entryId")
);

CREATE TABLE "meta_completed_stage_tables" (
  "tableType" TEXT,
  "stageId" INTEGER,
  "status" TEXT,
  PRIMARY KEY ("tableType", "stageId")
);

CREATE TABLE "meta_completed_event_tables" (
  "tableType" TEXT,
  "eventId" INTEGER,
  PRIMARY KEY ("tableType", "eventId")
);

"""
