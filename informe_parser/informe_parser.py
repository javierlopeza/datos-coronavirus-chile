import camelot
import json
import csv
from collections import OrderedDict

INPUT_DATE = "2020-08-13"
PARSE_PDF = False


def load_json(file_name):
    with open(file_name) as json_file:
        return json.load(json_file)


def parse_str_int(activos):
    try:
        return int(float(activos))
    except:
        return -1


def add_column_to_csv(csv_path, fix_place, str_data_dict, col_header=INPUT_DATE):
    with open(csv_path, 'r') as f:
        reader = iter(list(csv.reader(f)))
    with open(csv_path, 'w') as f:
        writer = csv.writer(f, lineterminator='\n')
        # Add new header
        new_rows = []
        header = next(reader)
        header.append(col_header)
        new_rows.append(header)
        # Add new data column
        for row in reader:
            place = fix_place(row[0])
            data = parse_str_int(str_data_dict[place])
            new_rows.append(row + [data])
        # Write new rows
        writer.writerows(new_rows)


class InformeParser:
    def load_input(self):
        # Load comunas names (es, en, extra)
        self.comunas_en = load_json("../names/comunas_en.json")
        self.comunas_es = load_json("../names/comunas_es.json")
        self.comunas_extra = load_json("../names/comunas_extra.json")
        self.all_comunas = {*self.comunas_es, *self.comunas_en, *self.comunas_extra}
        # Load fixed comunas names
        self.fixed_comunas = {
            **load_json("../names/fixed_comunas.json"),
            **load_json("../names/fixed_comunas_extra.json")
        }
        # Load regiones names
        self.regiones_es = load_json("../names/regiones_es.json")
        self.regiones_extra = load_json("../names/regiones_extra.json")
        self.all_regiones = {*self.regiones_es, *self.regiones_extra}
        # Load fixed regiones names
        self.fixed_regiones = load_json("../names/fixed_regiones.json")
        # Load region-comuna associations
        self.regiones_comunas = load_json("../names/regiones_comunas.json")

    def parse_tables(self):
        self.tables = camelot.read_pdf(
            "./input/tablas_informe_{}.pdf".format(INPUT_DATE),
            pages="all",
            flavor="stream",
            strip_text="&",
        )

    def fix_comuna(self, comuna):
        if comuna in self.fixed_comunas:
            return self.fixed_comunas[comuna]
        return comuna

    def fix_region(self, region):
        if region in self.fixed_regiones:
            return self.fixed_regiones[region]
        return region

    def scrap_comunas(self):
        self.confirmados_por_comuna = OrderedDict({comuna: None for comuna in self.comunas_es})
        self.fallecidos_por_comuna = OrderedDict({comuna: None for comuna in self.comunas_es})
        self.activos_por_comuna = OrderedDict({comuna: None for comuna in self.comunas_es})
        for table in self.tables:
            for row in table.df.itertuples(index=True, name="Pandas"):
                if row[1] in self.all_comunas:
                    nombre_comuna = self.fix_comuna(row[1])
                    if self.confirmados_por_comuna[nombre_comuna] is None:
                        confirmados_comuna = row[4]
                        self.confirmados_por_comuna[nombre_comuna] = confirmados_comuna
                    if self.fallecidos_por_comuna[nombre_comuna] is None:
                        fallecidos_comuna = row[6]
                        self.fallecidos_por_comuna[nombre_comuna] = fallecidos_comuna
                    if self.activos_por_comuna[nombre_comuna] is None:
                        activos_comuna = row[10]
                        self.activos_por_comuna[nombre_comuna] = activos_comuna

    def save_outputs(self):
        # Activos por comuna (output)
        activos_por_comuna_data = [
            {"comuna": comuna, "activos": activos}
            for comuna, activos in self.activos_por_comuna.items()
        ]
        with open("./output/activos_por_comuna_{}.csv".format(INPUT_DATE), "w") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=["comuna", "activos"])
            writer.writeheader()
            for data in activos_por_comuna_data:
                writer.writerow(data)
        
        # Confirmados por comuna (output)
        confirmados_por_comuna_data = [
            {"comuna": comuna, "confirmados": confirmados}
            for comuna, confirmados in self.confirmados_por_comuna.items()
        ]
        with open("./output/confirmados_por_comuna_{}.csv".format(INPUT_DATE), "w") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=["comuna", "confirmados"])
            writer.writeheader()
            for data in confirmados_por_comuna_data:
                writer.writerow(data)

        # Fallecidos por comuna (output)
        fallecidos_por_comuna_data = [
            {"comuna": comuna, "fallecidos": fallecidos}
            for comuna, fallecidos in self.fallecidos_por_comuna.items()
        ]
        with open("./output/fallecidos_por_comuna_{}.csv".format(INPUT_DATE), "w") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=["comuna", "fallecidos"])
            writer.writeheader()
            for data in fallecidos_por_comuna_data:
                writer.writerow(data)

    def load_outputs(self):
        # Activos por comuna (output)
        with open("./output/activos_por_comuna_{}.csv".format(INPUT_DATE), "r") as f:
            rows = list(csv.DictReader(f, fieldnames=["comuna", "activos"]))[1:]
            self.activos_por_comuna = {row["comuna"]: parse_str_int(row["activos"]) for row in rows}
        
        # Fallecidos por comuna (output)
        with open("./output/fallecidos_por_comuna_{}.csv".format(INPUT_DATE), "r") as f:
            rows = list(csv.DictReader(f, fieldnames=["comuna", "fallecidos"]))[1:]
            self.fallecidos_por_comuna = {row["comuna"]: parse_str_int(row["fallecidos"]) for row in rows}

        # Confirmados por comuna (output)
        with open("./output/confirmados_por_comuna_{}.csv".format(INPUT_DATE), "r") as f:
            rows = list(csv.DictReader(f, fieldnames=["comuna", "confirmados"]))[1:]
            self.confirmados_por_comuna = {row["comuna"]: parse_str_int(row["confirmados"]) for row in rows}

    def merge_outputs(self):
        add_column_to_csv("../raw_data/comunas/series_activos_comunas.csv", self.fix_comuna, self.activos_por_comuna)
        add_column_to_csv("../raw_data/comunas/series_fallecidos_comunas.csv", self.fix_comuna, self.fallecidos_por_comuna)
        add_column_to_csv("../raw_data/comunas/series_confirmados_comunas.csv", self.fix_comuna, self.confirmados_por_comuna)

    def calculate_data_regiones(self):
        # Activos por region
        activos_por_region = {region: 0 for region in self.regiones_es}
        for comuna, activos in self.activos_por_comuna.items():
            region = self.regiones_comunas[self.fix_comuna(comuna)]
            activos_por_region[region] += parse_str_int(activos)
        add_column_to_csv("../raw_data/regiones/series_activos_regiones.csv", self.fix_region, activos_por_region)
        
        # Confirmados por region (got them from reporte diario)
        # confirmados_por_region = {region: 0 for region in self.regiones_es}
        # for comuna, confirmados in self.confirmados_por_comuna.items():
        #     region = self.regiones_comunas[self.fix_comuna(comuna)]
        #     confirmados_por_region[region] += confirmados
        # add_column_to_csv("../raw_data/regiones/series_confirmados_regiones.csv", self.fix_region, confirmados_por_region)


parser = InformeParser()
parser.load_input()
if PARSE_PDF:
    parser.parse_tables()
    parser.scrap_comunas()
    parser.save_outputs()
else:
    parser.load_outputs()
parser.merge_outputs()
parser.calculate_data_regiones()
