# wrc-shinylive v1

ShinyLive site for WRC - uses in-browser Python/Pyodide WASM environment to run a Python Shiny application.

__Good for 2024 season as of March 2025.__

View app here: [http://rallydatajunkie.com/wrc-shinylive/](http://rallydatajunkie.com/wrc-shinylive/)

This repo is also currently home to the `wrc_rallydj` package on PyPi.


*Official docs etc conveniently located at e.g. https://www.rallysupport.com/wrc-rally-sweden-2025*

## RallyDataJunkie WRC Timing and Results App

### Home page

- select rally by *year*
- view results for a particular *championship*
- select a rally
- select a stage

![Top level view](images/top_view.png)

### Season Info

List of rallies and rally winners for completed rallies.

![Season Info](images/season_info.png)

*Championship is always WRC?*

## Event Overview

Select data views relating to a particular rally event

![Event options](images/event_overview_options.png)

### Stages Info

View stage information and status for each stage.

![Stages info - list of stages, name, distance, and status](images/stages_info.png)

### Itinerary

Itinerary overview.

![alt text](images/itinerary.png)

### Startlist

Complete start;list (all entrants)

![Complete list of entrants](images/startlist.png)

### Stage Winners

List of stage winners and bar chart of stage win counts.

![List of stage winners and bar chart of stage win counts](images/stage_winners.png)

## Reitements and Penalties

List of all retirements.

![List of all retirements](images/retirements.png)

List of all penalties.

![List of all penalties](images/penalties.png)

## Stage Review

Overall hero display and dropdowns to explore stage results in more detail.

![Stage review hero banner and options](images/stage_review.png)

### Overall position

View of overall rally position at end of stage.

![Stage overall](images/stage_overall.png)

__TO DO — Fix: in overall position, stageTime column gives rally time.__

### Stage times

View of stage times for a selected stage. Rebasing relative to a driver shows the deltas between that driver and the other drivers.

![Stage times](images/stage_times.png)

## Split Analysis

Selectors for exploring split times organised by Split times (overall and in detail) and rebased driver reports:

![Splits analysis options — Split times and rebased reports](images/splits_selectors.png)

### Overall split times

View of split times data, including road position, and split times at each split point.

![Overall split times](images/overall_split_times.png)

### Split times detail

Detailed breakdown of split times on a stage as the time taken to complete each split section. If split distances are available, we can also provide speed and pace over each spilit.

![Split times detail](images/split_times_detail.png)

Time / speed / pace box plot statistical chart, which allows us to compare the pace in each split section.

![Split time/pace/speed as a statistcal box plot](images/split_timepacespeed_distribution.png)

### Rebased driver split reports

Selectors for rebased driver splits reports. A hero banner for a selected rebase driver is displayed. An "ultimate" rebase driver is also available made up from the best time within each split section.

![Selectors and hero for rebased driver split report](images/rebased_driver_split_selection.png)

#### Rebased driver splits heatmap

Show the time delta within each split section betwwen each driver and the rebase selection driver.

![Rebased driver splits heatmap](images/rebased_splits_heatmap.png)

#### Split times group barplots

Two chart types are availabl.

*Split barplot by split section group* shows the deltas, grouped by split section, between the rebase selected driver and each other driver. *This allows us to spot whether a particular driver was strong or weak in each split section.*

![Split barplot by split section group](images/split_barplot_by_split_section_group.png)

*Split barplot by driver group* shows the deltas, grouped by each driver, between the rebase selected driver and each other driver within each split section. *This allows us to spot whether a particular driver was strong or weak compared to another driver.*

![Split barplot by driver group](images/split_barplot_by_driver_group.png)

#### Accumulated slit times linechart

Line chart of rebased accumulated split stage times.

![Rebased split time delta lineplot](images/splits_lineplot.png)

## TO DO

- if a stage has been completed, cache the times;
- offer live updates of times if a stage is running; (the WRC api may help here? or we can use heuristics based on time-of-day? If there is a delay, does the itinerary get updated?)
- provide some stage map views; 
- support URL args so we can bookmark a particular rally/stage selection. Could we also auto-expand and scroll to selected accordion views?
- for retirements and penalties, have options to limit all retirements before/after/during a particular stage.
- in overall position, stageTime column gives rally time.
- splits heatmap should also be available as a pace/speed view.