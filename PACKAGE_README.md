# README

`wrc_rallydj` is a Python package for working with the WRC (World Rally Championship) live timing and data API.

Install as: `pip install wrc_rallydj`

The package was developed in order to support personal use (research, analysis, visualisation, reporting) of timing and results data generated from WRC rally events.

*This website is unofficial and is not associated in any way with WRC Promoter GmbH. WRC WORLD RALLY CHAMPIONSHIP is a trade mark of the FEDERATION INTERNATIONALE DE L'AUTOMOBILE.*

## Usage

Import as:

`from wrc_rallydj.livetiming_api import WRCLiveTimingAPIClient`

Initialise:

```python
wrc = WRCLiveTimingAPIClient()
wrc.initialise() # YOU MUST DO THIS

wrc.getResultsCalendar() # etc
```

This package works in Python and Pyodide environments.

*More docs to follow...*
