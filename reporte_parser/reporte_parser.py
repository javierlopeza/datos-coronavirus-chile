import camelot
import json
from copy import deepcopy
import csv
import pendulum

import bs4 as bs
import dateparser
import sys
import os
import os.path
import requests

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


class ReporteParser:
    def load_input(self):
        # Load regiones names
        self.regiones_es = load_json("../names/regiones_es.json")
        self.regiones_extra = load_json("../names/regiones_extra.json")
        self.all_regiones = {*self.regiones_es, *self.regiones_extra}
        # Load fixed regiones names
        self.fixed_regiones = load_json("../names/fixed_regiones.json")
        # Load metrics columns indexes
        self.regiones_metrics_columns_indexes = load_json("../minsal_keys/regiones_metrics_columns_indexes.json")
        self.chile_metrics_columns_indexes = load_json("../minsal_keys/chile_metrics_columns_indexes.json")

    def download_reporte(self):
        GOV_URL = "https://www.gob.cl/coronavirus/cifrasoficiales/"
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.76 Safari/537.36', "Upgrade-Insecure-Requests": "1",
                "DNT": "1", "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", "Accept-Language": "en-US,en;q=0.5", "Accept-Encoding": "gzip, deflate"}

        source = requests.get(GOV_URL, timeout=10, headers=headers)
        soup = bs.BeautifulSoup(source.content, features="html.parser")

        last_reporte_anchor = soup.find(id="reportes").find("a")
        last_reporte_date_str = last_reporte_anchor.text.strip()
        self.last_reporte_date = dateparser.parse(last_reporte_date_str, languages=['es']).strftime('%Y-%m-%d')
        if os.path.isfile("./input/tablas_reporte_{}.pdf".format(self.last_reporte_date)):
            print("No Action: last reporte already exists.")

        last_reporte_url = last_reporte_anchor["href"]
        response = requests.get(last_reporte_url, stream=True)
        with open('./input/tablas_reporte_{}.pdf'.format(self.last_reporte_date), 'wb') as f:
            f.write(response.content)

    def parse_tables(self):
        tables = camelot.read_pdf(
            "./input/tablas_reporte_{}.pdf".format(self.last_reporte_date),
            pages="2,3",
            flavor="stream",
        )
        self.table_regiones, self.table_nacional = tables[:2]

    def fix_region(self, region):
        if region in self.fixed_regiones:
            return self.fixed_regiones[region]
        return region

    def scrap_table_regiones(self):
        self.regiones = {
            region: deepcopy(BASE_PLACE)
            for region in self.regiones_es
        }
        for row in self.table_regiones.df.itertuples(index=True, name="Pandas"):
            if row[1] in self.all_regiones:
                region_name = self.fix_region(row[1])
                for metric, index in self.regiones_metrics_columns_indexes.items():
                    self.regiones[region_name][metric] =\
                        row[index]

    def scrap_table_nacional(self):
        self.chile = deepcopy(BASE_PLACE)
        rows = list(self.table_nacional.df.itertuples(index=True, name="Pandas"))
        formatted_reporte_date = dateparser.parse(self.last_reporte_date).strftime('%d-%m-%Y')
        total_row = list(filter(lambda row: row[1] == formatted_reporte_date, rows))[0]
        for metric, index in self.chile_metrics_columns_indexes.items():
            self.chile[metric] = total_row[index]

    def parse_numbers(self):
        for region, metrics in self.regiones.items():
            for metric in metrics:
                metrics[metric] = parse_number(metrics[metric])
        for metric, metric_value in self.chile.items():
            self.chile[metric] = parse_number(metric_value)

    def save_new_regions_metrics(self, path, metric):
        with open(path, 'r') as csv_file:
            reader = iter(list(csv.reader(csv_file)))
        # Check if we need to update anything
        row = next(reader)
        last_data_date = row[-1]
        if last_data_date >= self.last_reporte_date:
            print("No Action: data already obtained.")
            return
        with open(path, 'w') as csv_file:
            writer = csv.writer(csv_file, lineterminator='\n')
            # Add last reporte date header
            new_rows = []
            row.append(self.last_reporte_date)
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
        # Check if we need to update anything
        row = next(reader)
        last_data_date = row[-1]
        if last_data_date >= self.last_reporte_date:
            print("No Action: data already obtained.")
            return
        with open(path, 'w') as csv_file:
            writer = csv.writer(csv_file, lineterminator='\n')
            # Add last reporte date header
            new_rows = []
            row.append(self.last_reporte_date)
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

    
parser = ReporteParser()
parser.download_reporte()
parser.load_input()
parser.parse_tables()
parser.scrap_table_regiones()
parser.scrap_table_nacional()
parser.parse_numbers()
parser.save_new_values()
