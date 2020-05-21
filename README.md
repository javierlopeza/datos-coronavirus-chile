# Datos Coronavirus Chile

![Check for new reporte diario](https://github.com/javierlopeza/datos-coronavirus-chile/workflows/Check%20for%20new%20reporte%20diario/badge.svg)

![Check for new informe epidemiologico](https://github.com/javierlopeza/datos-coronavirus-chile/workflows/Check%20for%20new%20informe%20epidemiologico/badge.svg)

This repository is used to process data from different sources to feed [COVID-19 en tu comuna](https://covid19entucomuna.cl). 

It started using data uploaded to [MinCiencia/Datos-COVID19](https://github.com/MinCiencia/Datos-COVID19) but it was changed to a process where official PDF reports are parsed to get all required data.

## Using this repo as data source

We strongly recommend using the **production** branch of this repo for data fetching purposes. That branch will always be available in a reliable state.

If you want to fetch raw JSON data, you should always be fetching the following URL:

```https://raw.githubusercontent.com/javierlopeza/datos-coronavirus-chile/production/data/chile.json```

## JSON Data

The main output of this repo is [`chile.json`](https://github.com/javierlopeza/datos-coronavirus-chile/blob/production/data/chile.json). It contains all relevant information to feed [COVID-19 en tu comuna](https://covid19entucomuna.cl). It is also structured in a way is easy for programmers to navigate.

`country > regions > communes`

A minified version of this file is also published along with it: [`chile-minified.json`](https://github.com/javierlopeza/datos-coronavirus-chile/blob/production/data/chile-minified.json).

## CSV Data

These are the files that keep all records and that are used to build [`chile.json`](https://github.com/javierlopeza/datos-coronavirus-chile/blob/production/data/chile.json). File names are kept the same as [Datos-COVID19](https://github.com/MinCiencia/Datos-COVID19).

* `CasosTotalesCumulativo.csv`: cummulative confirmed cases by region.

* `FallecidosCumulativo.csv`: cummulative deaths by region.

* `TotalesNacionales.csv`: cummulative country totals.

* `CasosActivosPorComuna.csv`: total active cases by commune.

* `Quarantines.csv`: current active quarantines by commune.

## `data_organizer.py` - Data Organizer

It is in charge of collecting all the information spread across CSV files to generate a single `/data/chile.json` file that is served to [COVID-19 en tu comuna](https://covid19entucomuna.cl).

It also generates a minified version named `/data/chile-minified.json`.

## `informe_parser.py` - Informe Epidemiológico Parser

This is a program built to parse a PDF file uploaded to [COVID-19 government website](https://www.gob.cl/coronavirus/cifrasoficiales/#informes). It parses every table in the document, scrapping total coronavirus active cases for each commune and region.

## `reporte_parser.py` - Reporte Diario Parser

This is a program built to parse a PDF file uploaded to [COVID-19 government website](https://www.gob.cl/coronavirus/cifrasoficiales/#reportes). It parses the first tables in the document, scrapping coronavirus data about Chile and about each region.

## `reporte_scraper.py` - Reporte Diario Scraper

This is a program built to scrap daily new info about COVID-19 cases in the country and for each region. The source of the data is [MINSAL official website](https://www.minsal.cl/nuevo-coronavirus-2019-ncov/casos-confirmados-en-chile-covid-19/).

_NOTE_: as of May 20, we are not using this scraper, we are using Reporte Diario Parser instead.
