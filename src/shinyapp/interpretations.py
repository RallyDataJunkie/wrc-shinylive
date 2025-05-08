# Interpretation texts

# Unlike remarks, which are typically generated from one or more data sources,
# interpretation texts should just contain markdown text strings
# Splitting the text out this way should also simplify language packs construction

about_md = """

This website was developed in order to explore the use of Shinylive-Python for developing in-browser reactive Python applications, and to support personal use (reasearch, analysis, visualisation, reporting) of timing and results data arising from WRC rally events. *It takes a long time to load/start-up.*

This website may contain errors resulting from the processing of the data. Please file an issue at https://github.com/RallyDataJunkie/wrc-shinylive/issues if you notice any errors r the application breaks for you. *Add `/edit` to the end of the application URL to view the loading process, and log any errors generated using the application.* Results and timing data may be cached in your browser.

__Usage:__ Select championship (WRC/ERC), year, season, etc. from the sidebar. *Intepretation prompts* (explanatory text displayed regarding the controls and charts) may be disabled using the *Show inpterpreation prompts* switch at the bottom of the sidebar.

__On the use of generative AI:__ although this app displays machine generated text based on on the data, it *is not* generated using generative AI. The text is all generated from simple rules, which may include random selextion of alternative generated texts. As such, the text is guaranteed free from hallucinated names, numbers, events etc. Any errors are due to an error in the underlying data, or an error in the rule(s), which can be corrected. Generateive AI was used to support some of the code used by this app, and some functions originally generated using generateive AI models. *Models used: free plans from Claude.ai, ChatGPT.*

*This website is unofficial and is not associated in any way with WRC Promoter GmbH, The WRC World Rally Championship, the FIA European Rally Championship, the Federation Internationale de l'Automobile or Red Bull GmbH. WRC WORLD RALLY CHAMPIONSHIP and ERC FIA EUROPEAN RALLY CHAMPIONSHIP are trade marks of the FEDERATION INTERNATIONALE DE L'AUTOMOBILE.*

"""

rally_progression_interpretation_md = """

The *rally progression* reports describe the evolution of the rally across the completed stages. They provide a way of looking across all the  stage times, positions, in a single view.

The *overall position* line chart shows how each driver's overall position has evolved over the course of the rally. The chart shows increasing stage number from left to right, with a separate line reporting the position for each driver across stages.

__Things to look for:__  if a line remains horizontal from one stage to another, the driver retained the same position. If the line slopes *up and to the right*. the driver imporved their position going from one stage to the next. If the line goes *down and to the right*, the driver has lost overall position."""

rally_progression_base_interpretation_md = """

The *rally progression report* includes two components. A *rally progression rebase heatmap* table and an *overall rally time progression* line chart.

Both reports rely on the notion of *rebasing*. *Rebased* values are calculated as the delta between each driver and the selected rebase driver.

Two different rebase options are provided:

- __Stage time__: rebase each driver's *stage time* relative to a selected driver. This view allows to identify stages on which particular driver's performed well, or poorly, for example;

- __Overall rally time__: rebase each driver's *overall elapsed rally time* relative to a selected driver. This view allows us to compare the overall gaps between a selected driver and each other driver at the end of each stage.

"""

rally_progression_report_interpretation_md = """

View options for the *rally progression report* include:

- __Stage time (s)__: time, in seconds, taken to complete the stage.

- __Stage position__: rank position on stage.

- __Stage gap (s)__: the __gap__ is the *gap to leader* on the stage. Gives an indication how how off the stage winnner each driver was.

- __Stage diff (s)__: the *diff* is the time difference to the car ahead on stage. Gives an indication of how far off a driver was from improving their position.

- __Stage chase (s)__: the *chase* is the time difference to the car behind. Gives an indication how threatened a driver was from the car behind.

- __Overall rally time (s)__: the elapsed rally time by the end of a particular stage.

- __Overall rally position__: the overall rally poisition by the end of a stage. If the rally stopped at this point, this would be the driver's rally position.

- __Overall rally class position__: the overall position in class (e.g. WRC, WRC2 etc).

- __Overall rally gap (s)__: the gap to the rally leader based on elapsed rally time. Gives an indication of how far off the rally leader a driver is at the end of each stage.

- __Overall rally diff (s)__: in terms of overall rally time, how far a driver is from the car in the position ahead. Gives an indication of how close they are to improving their overall rally position.

- __Overall rally chase (s)__: in terms of overall rally time, how far a driver is from the car in the position behind. Gives an indication of how threatened a driver is by the car in the position behind.

"""

rally_progression_heatmap_interpretation_md = """

The *rally progression rebase heatmap* table displays the rally progression data in a tabular form as a *heatmap*. The cells are colour coded to show whether a driver has a *faster* time (a *negative* delta; by default, *green*) or a *slower* time (*positive* delta; by default, *red*). The deltas for the the selected rebase driver are, of course, zero (*grey*).

The intensity of the colour indicates how far away from zero the value is. The colour palette can be set based on the range of values within each column (i.e. stage), or using a single range calculated over all columns. The maximum colour saturation is at an absolute delta of 30s. For the longest stages (30km), this would mean a pace delta of 1s/km off the pace of the selected rebase driver. The colours used in the rebase palette can also be swapped, giving an indication of the deltas *from the perspective of the rebase driver*. Witn the reverese palette selected, a car with a positive delta (*slower*) than the rebase driver would have a *green* coloured cell and a *faster* car (*negative* delta) woud have *red* coloured cell.

__Things to look for in the heatmap:__

- __faintly coloured cells_ show times for a particular driver that are particularly colose to the selected rebase driver on that stage;

- a __brightly coloured *row* of one colour__ shows that the *corresponding driver* was universally fast / slow compared to the selected rebase driver;

- a __brightly coloured *column* of one colour__ indicates that the *rebase driver* was particularly fast or slow on that stage compared to all the other drivers (i.e. that was a particularly *good* (perhaps counterintuitively, in the default palette, *red*) or, perhaps more likely, *bad* (again, perhaps counterintuitively in the default palette, *green*) stage for the rebase driver. *Select the `Reverse rebase palette` option to switch the sense of the colour interpretations;*
    
- __brightly coloured singleton cells__ indicate a particular good or bad performance from one of the other drivers as compared to the rebase selected driver.

"""

rally_progression_linechart_interpretation_md = """

The *overall rally time progression* line chart displays the rebased rally progression time deltas as a *line chart* of time delta against stage number. The chart is rebased relative to a selected driver.

Note the the vertical `y`-axis, giving the *overall elapsed rally time delta*, is inverted, with negative deltas (cars going *faster* than the selected rebase driver) *above* the origin in the *grey* coloured area of the chart.

__Things to look for in the line chart:__

- how far a way a line is from the `y=0` origin line, indicates an increasing time delta between that driver and the selected rebase driver;

- a line with a __constant gradient__ shows a similar amount of time lost across each stage;

- a rebased line that goes *__into__ the grey area and then __out__ of it again* shows a driver getting *ahead then falling back behind* the selected rebase driver;

- a rebased line that goes *__out of__ the grey area and then __into__ it again* shows a driver *falling behind* the selected rebase driver and then getting *ahead* of them again;

- a __single line that rapidly falls__ at a particular stage indicates that that driver had a particularly *bad* stage. Check the split times barplots to see if there was a particular section where time was lost, or whether time was lost increasingly across stages;*

- the __more lines__ in the grey part of the chart, the *lower* the position of the selected rebase driver. Conversely, the *more* lines below the `y=0` line, the higher ranked the selected rebase driver;

- if __all the lines suddenly go up__, the selected rebase driver lost a lot of time on that stage. *Check the split times barplots to see if there was a particular split section where time was lost, or whether time was lost increasingly across stages.*

*__TO DO__ - provide the option to select rally distance on the `x`-axis, with faint vertical lines indicating each stage; the constant gradient would then indicate a constaint `pace` difference.*

"""

stage_progression_linechart_interpretation_md = """

The *stage progression* line chart displays stage split progression rebased time deltas against stage split number. The chart is rebased relative to a selected driver.

Note the the vertical `y`-axis, giving the *elapsed stage time delta*, is inverted, with negative deltas (cars going *faster* than the selected rebase driver) *above* the origin in the *grey* coloured area of the chart. The maximum colour saturation is at an absolute delta of 30s. For the longest stages (30km), this would mean a pace delta of 1s/km off the pace of the selected rebase driver at the end of the stage using the whole table, rather than in-column) heatmap palette.

__Things to look for in the line chart:__

- how far a way a line is from the `y=0` origin line, indicates an increasing time delta between that driver and the selected rebase driver;

- a line with a __constant gradient__ shows a similar amount of time lost across each split;

  - a rebased line that goes *__into__ the grey area and then __out__ of it again* shows a driver getting *ahead then falling back behind* the selected rebase driver;

- a rebased line that goes *__out of__ the grey area and then __into__ it again* shows a driver *falling behind* the selected rebase driver and then getting *ahead* of them again;

- a __single line that rapidly falls__ at a particular split section indicates that that driver had a particularly *bad* split section.  *Check the split times barplots to see if there was a particular section where time was lost, or whether time was lost increasingly across split sections;*

- the __more lines__ in the grey part of the chart, the *lower* the position of the selected rebase driver. Conversely, the *more* lines below the `y=0` line, the higher ranked the selected rebase driver;

- if __all the lines suddenly go up__, the selected rebase driver lost a lot of time on that split section. *Check the split times barplots to see if there was a particular split section where time was lost, or whether time was lost increasingly across split sections.*

__TO DO__ - provide the option to select stage distance on the `x`-axis, with faint vertical lines indicating each stage; the constant gradient would then indicate a constaint `pace` difference.*
                                        
"""

stage_progression_rebase_select_md = """

The driver rebase selector sets a selected rebase driver against whom delta times for all the other drivers are caluclated at each split point.

The selector also includes an *ULTIMATE* driver, whose times are made up from the best *in-section* split duration for each split (that is, the quickest time recorded to get from one split to the next, irrespective of the overall elasped time at each split point.

*Note that the *ULTIMATE* time may not represent an achievable time. For example, a driver might record a particularly fast time on one split section but aat the cost of doing so much damage to the tyres that it is difficult, it not inmpossible, to record any good times on later split sections in the same stage. Changeable weather conditions may also affect times on particular split sections at different times when the stage is running, and so on.*

"""

stage_progression_interpretation_md = """

View options for the stage progression report include:

- __Time (s) within each split__: the time taken *within each split*, i.e. the time to get from one split point to the next. *__Lower__ is better.* Use this to see which split sections a driver gained / lost time on.

- __Speed (km/h) within each split__: the within split time divided by the distance between split points. *__Higher__ is better. Gives a sense of whether a particular section was fast or slow.*

- __Pace (s/km) within each split__: the distance between split points divided by the within split time. *__Lower__ is better.* Comparison allows you to see how much time was gained / lost per km, compared to other driver.

- __Accumulated time (s) across all splits__: view elapsed / accumulated stage time (in seconds) across each split. Use this to get a sense of how the stage times progressed across the stage.  *Lower* is better.*  If the split was the stage end, this would be the stage time. Use this to see how the "overall" stage time evolved across the splits.

- __Rank position within split__: view "elapsed" time rank at each split point. *__Lower__ is better ("higher" rank).* Treat the split as a "stage" in its own right. Use this to how the driver ranked ourely on the basis of this split section.

- __Rank position of accumulated time at each split__: view "elapsed" time rank at each split point. *__Lower__ is better ("higher" rank).* If the split was the stage end, this would be the stage position. Use this to see how the "overall" stage position evolved across the splits.

"""

stage_progression_barchart_interpretation_md = """

The *stage progression bar chart* uses a grouped horizontal bar chart to display time deltas to a selected rebase driver in two possible ways: *split section groups* and *driver groups*. In each case, *rebased* values are calculated as the delta between each driver and the selected rebase driver.

Two different rebase options are provided

 - __Split section groups__: group the bars within split sections. Each bar in the group corresponds to a different driver, in road position order;

  - __Driver groups__: group the bars by driver. Each bar in the group corresponds to a split section, in split section order.

The bars are coloured with respect to each driver's delta, so *negative* deltas (other driver is quicker than selected rebase driver) are coloured *green*, and *positive* deltas (other driver is *slower*) are coloured *red*. The hotizontal axis is reversed, so *positive* deltas (*red*, other driver is *slower* compared to selected rebase driver) extend to the *left* and *negative* deltas (*green*, other driver is *slower* compared to selected rebase driver) extend to the *right*.

__Things to look for__:

  - __Split section groups__: if all the bars in a particular group have __green bars to the right__, all the other drivers compared to the selected rebase driver were faster on that split section, so the selected rebase driver had a *bad* stage. If the bars within a single group are all *red* and to the *left*, all the other drivers performed worse than the selected rebase driver, so the selected rebase driver had a *good* stage. If a single bar consistently points in the opposite direction compared to the other bars across all groups, the driver associated with that bar (given by road order /  the order in the rebase driver selection box) fared counter to all the other drivers on that stage. So for example, if all the bars but one are red and the the left, and the same single bar across the driver groups is consistently green and to the right, that singleton driver was doing better than the selected rease driver, and all the other drivers were doing worse. If a single bar extends a long way red and to the left, that driver on that split had a particularly bad time.

__Driver groups__: if all the bars point in the same direction within a driver group, that indicates that the driver performed better (green, to the right) or worse (red, to the left) than the selected rebase driver. If a single bar extends a long way red and to the left, that driver on that split had a particularly bad time.

"""

split_times_heat_interpretation_md = """

The split times heatmap uses colour to indicate the magnitude of time deltas at each split point.

*Positive* rebase time deltas (driver has *lost* time relative to rebase selected driver), are colourd *red*. Negative rebase time deltas (driver has gained time) are coloured *green*. *The palette can also be reversed to indicate time deltas "from the perspective of the rebase selected driver".*

The colour palette by default is generated on a *per column* basis, using the full colour range *within* a column. The palette can also be defined *across* the table as whole, using the maximum positive and minimum negative deltas across the whole stage to define the colour range.

The heatmap can be used to display two views: the (default) accumulated (elapsed) stage time delta*, and the *within section time delta*.

- __Accumulated (elapsed) stage time delta (s)__: the deltas give the delta on the accumulated (elapsed) stage time at each split point. *This is the time typically depicted on the WRC/Rally.tv graphics.*

- __Within section time delta__: this is the time taken to complete each split section, e.g. treating it as a "mini-stage" in its own right.

__Things to look for__: the heatmap can be used to identify patterns or behaviour (full column, or full row) divergent colourings, as well as individual driver/split section features:

- __divergent *row* colouring__: if all the cells in a row (associated with a particular driver) are strongly coloured the same way, that shows the driver had a particularly *good* (by default, *green*) or particularly *bad* (by default, *red*) stage *at each split point*, compared to the selected rebase driver.

- __divergent *column* colouring__: if all the cells in a *column* are strongly coloured *green*, that shows the selected rebase driver has lost time on that section. If the rebase driver fixes a puncture in a split section, for example, we would expect that split section on the *within splits* view to show all the other cars fared better. If the selected rebase driver has a puncture but cominues, we might expect to see them losing time to all the other drivers at each split point across several splits to the end of the stage.

"""

live_map_interpretation_md = """

The map uses a live data feed sampled every 5s or so to display car positions on a map, indicated by car number. The colour of the label identifies whether or not the car is moving (*blue*) or stationary (orange).

"""

