# Web-Scraper-Google-Maps-To-PostGIS

A tool to scrape location and business details from Google Maps Places API. This project was created with the intent of finding specific types of businesses within a certain radius. This tool currently exports the data into a .csv file which includes business name, ratings, phone number, website, unique identifier, and latitude and longitude which are in the merrcator projection (spherical normal). Data is also exported as a shapefile and the geodataframe is exported to a local PostGIS database. The next step will be to display this to a Leaflet map.
