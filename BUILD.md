# BUILD

To try out the shinylive app:

- `shinylive export src/shinyapp  shinysite   --subdir app1 --full-shinylive`
- `python3 -m http.server 8126 --directory shinysite`

quarto add --no-prompt r-wasm/quarto-live
quarto add --no-prompt quarto-ext/shinylive  
quarto render src/load_full_telemetry.Rmd --output-dir ../dist 
quarto render src/ --output-dir ../dist

- `python3 -m http.server 8127 -d dist`


Package

python -m build --wheel
python -m twine upload dist/*


## Quarto dashboards

As live dashboards using shinylive:

https://shiny.posit.co/py/docs/user-interfaces.html

More generally (not live):

- https://quarto.org/docs/dashboards/
- https://quarto.org/docs/dashboards/interactivity/shiny-python/index.html

## Shinyapps

pip install rsconnect-python

https://docs.posit.co/shinyapps.io/guide/getting_started/#working-with-shiny-for-python

FOR SHINY PYHTON (NOT PYODIDE) REMOVE sqlite3 from requirements.txt. May also need to add in other requirements.

rsconnect deploy shiny shinyapp --name psychemedia --title RallyDJ-WRC