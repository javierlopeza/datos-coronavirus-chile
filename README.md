# Datos Coronavirus Chile

This repository is used to process data from different sources to feed [COVID-19 en tu comuna](https://covid19entucomuna.cl) in case there is a delay in the delivery of official information on [Datos-COVID19](https://github.com/MinCiencia/Datos-COVID19).

## Reporte Diario Parser

This is a program built to scrap daily new info about COVID-19 cases in the country and for each region. The source of the data is [MINSAL official website](https://www.minsal.cl/nuevo-coronavirus-2019-ncov/casos-confirmados-en-chile-covid-19/).

## Informe Epidemiológico Parser

This is a program built to parse a PDF file uploaded to [COVID-19 government website](https://www.gob.cl/coronavirus/cifrasoficiales/#informes). These files are uploaded every 3-4 days. It parses every table in the document, scrapping total coronavirus active cases for each commune and region.

## Data Organizer

It is in charge of collecting all the information spread across CSV files to generate a single `/data/chile.json` file that is served to [COVID-19 en tu comuna](https://covid19entucomuna.cl).

## CSV Data

File names are kept the same as [Datos-COVID19](https://github.com/MinCiencia/Datos-COVID19).

* `CasosTotalesCumulativo.csv`: cummulative confirmed cases by region.

* `FallecidosCumulativo.csv`: cummulative deaths by region.

* `TotalesNacionales.csv`: cummulative country totals.

* `CasosActivosPorComuna.csv`: total active cases by commune.

* `Quarantines.csv`: current active quarantines by commune.
