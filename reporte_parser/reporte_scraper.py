import json
import requests
import bs4 as bs
from copy import deepcopy
import csv
import pendulum
import dateparser
from dateparser.search import search_dates
import re

MINSAL_URL = "https://www.minsal.cl/nuevo-coronavirus-2019-ncov/casos-confirmados-en-chile-covid-19/"

BASE_PLACE = {
    "confirmados": None,
    "nuevos": None,
    "nuevos_con_sintomas": None,
    "nuevos_sin_sintomas": None,
    "fallecidos": None,
    "recuperados": None,
    "activos": None,
}


def load_json(file_name):
    with open(file_name) as json_file:
        return json.load(json_file)

def parse_number(number):
    if number is not None:
        return int(number.replace(".", ""))


class ReporteScraper:
    def load_input(self):
        # Load regiones
        self.regiones_minsal = load_json("../names/regiones_minsal.json")
        self.regiones = {
            region: deepcopy(BASE_PLACE)
            for region in load_json("../names/regiones_es.json")
        }
        # Load fixed regiones names
        self.fixed_regiones = load_json("../names/fixed_regiones.json")
        # Load metrics columns indexes
        self.metrics_columns_indexes = load_json("../minsal_keys/metrics_columns_indexes.json")

    def fix_region(self, region):
        return self.fixed_regiones.get(region, region)

    def is_new_report(self, soup_minsal):
        try:
            # Get actual report date
            report_date_text_node = soup_minsal.find(string=re.compile("Informe corresponde al"))
            report_date_text = str(report_date_text_node).strip()
            self.report_date = search_dates(report_date_text, languages=["es"])[0][1].strftime('%Y-%m-%d')
            # Get last scraped report date
            with open("../minciencia_data/TotalesNacionales.csv", 'r') as csv_file:
                reader = csv.reader(csv_file)
                header = next(reader)
            last_report_date = header[-1]
            # Compare dates
            if self.report_date > last_report_date:
                print("Action: Proceed to scrap report.")
                return True
            print("No Action: Report already retrieved.")
        except Exception as error:
            print(error)
        return False

    def scrap_minsal(self):
        # Fetch MINSAL site
        source_minsal = requests.get(MINSAL_URL, timeout=10)
        soup_minsal = bs.BeautifulSoup(source_minsal.content, features="html.parser")

        # Check if it is a new report
        if not self.is_new_report(soup_minsal):
            return

        # Scrap regiones
        for region_minsal in self.regiones_minsal:
            region_row = soup_minsal.find("td", string=region_minsal).parent.find_all("td")
            region_data = list(region_row)
            region = self.fix_region(region_minsal)
            for metric in self.metrics_columns_indexes:
                self.regiones[region][metric] =\
                    region_data[self.metrics_columns_indexes[metric]].string

        # Scrap Chile
        self.chile = deepcopy(BASE_PLACE)
        total_row = soup_minsal.find("td", string="100%").parent.find_all("td")
        total_data = list(total_row)
        for metric in self.metrics_columns_indexes:
            self.chile[metric] =\
                total_data[self.metrics_columns_indexes[metric]].string
        # Scrap recuperados Chile
        recuperados_text_td = soup_minsal.find("td", string="Casos recuperados a nivel nacional")
        recuperados = recuperados_text_td.find_next("td").string
        self.chile["recuperados"] = recuperados
        
        # Parse all scraped numbers
        self.parse_numbers()

        # Calculate activos Chile
        self.chile["activos"] =\
            self.chile["confirmados"] - self.chile["recuperados"] - self.chile["fallecidos"]

        # Save results
        self.save_new_values()

    def parse_numbers(self):
        for region, metrics in self.regiones.items():
            for metric in metrics:
                metrics[metric] = parse_number(metrics[metric])
        for metric, metric_value in self.chile.items():
            self.chile[metric] = parse_number(metric_value)

    def save_new_regions_metrics(self, path, metric):
        with open(path, 'r') as csv_file:
            reader = iter(list(csv.reader(csv_file)))
        with open(path, 'w') as csv_file:
            writer = csv.writer(csv_file, lineterminator='\n')
            # Add today's date header
            new_rows = []
            row = next(reader)
            today = pendulum.now("America/Santiago").format("YYYY-MM-DD")
            row.append(today)
            new_rows.append(row)
            # Add metric's values for each region
            for row in reader:
                if row[0] != "Total":
                    region = self.fix_region(row[0])
                    row.append(self.regiones[region][metric])
                else:
                    row.append(self.chile[metric])
                new_rows.append(row)
            # Write new rows
            writer.writerows(new_rows)

    def save_new_chile_metrics(self):
        path = "../minciencia_data/TotalesNacionales.csv"
        with open(path, 'r') as csv_file:
            reader = iter(list(csv.reader(csv_file)))
        with open(path, 'w') as csv_file:
            writer = csv.writer(csv_file, lineterminator='\n')
            # Add today's date header
            new_rows = []
            row = next(reader)
            today = pendulum.now("America/Santiago").format("YYYY-MM-DD")
            row.append(today)
            new_rows.append(row)
            # Add metric's values
            totales_nacionales_keys = load_json("../minciencia_keys/TotalesNacionales.json")
            for row in reader:
                metric = totales_nacionales_keys[row[0]]
                row.append(self.chile[metric])
                new_rows.append(row)
            # Write new rows
            writer.writerows(new_rows)

    def save_new_values(self):
        self.save_new_regions_metrics("../minciencia_data/CasosTotalesCumulativo.csv", "confirmados")
        self.save_new_regions_metrics("../minciencia_data/FallecidosCumulativo.csv", "fallecidos")
        self.save_new_chile_metrics()

scraper = ReporteScraper()
scraper.load_input()
scraper.scrap_minsal()
