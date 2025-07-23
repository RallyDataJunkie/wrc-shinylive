# TO DO


Access url params:

from shiny.express import session

Then in a reactive scope:
url_protocol, url_hostname, url_pathname, url_port, url_search

from urllib.parse import urlparse
urlparse(session.input['.clientdata_url_search']())