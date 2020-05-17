import camelot
import json
from copy import deepcopy
import csv
import pendulum

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

    def parse_tables(self):
        today = pendulum.now("America/Santiago").format("YYYY-MM-DD")
        tables = camelot.read_pdf(
            "./input/tablas_reporte_{}.pdf".format(today),
            pages="all",
            flavor="stream",
        )
        self.table_regiones, self.table_nacional = tables

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
        today = pendulum.now("America/Santiago").format("DD-MM-YYYY")
        rows = list(self.table_nacional.df.itertuples(index=True, name="Pandas"))
        total_row = list(filter(lambda row: row[1] == today, rows))[0]
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

    
parser = ReporteParser()
parser.load_input()
parser.parse_tables()
parser.scrap_table_regiones()
parser.scrap_table_nacional()
parser.parse_numbers()
parser.save_new_values()
