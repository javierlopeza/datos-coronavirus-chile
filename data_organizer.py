import json
import csv
from collections import OrderedDict
from copy import deepcopy

CHILEAN_POPULATION = 19458310
BASE_PLACE = {
    "poblacion": None,
    "confirmados": None,
    "activos": None,
    "recuperados": None,
    "fallecidos": None,
    "previous": {
        "confirmados": None,
        "activos": None,
        "recuperados": None,
        "fallecidos": None,
    },
    "series": {
        "nuevos_con_sintomas": [],
        "nuevos_sin_sintomas": [],
        "nuevos": [],
        "confirmados": [],
        "activos": [],
        "recuperados": [],
        "fallecidos": [],
    },
}


def parse_string_int(s):
    return int(float(s)) if s != "" else None

def load_json(file_name):
    with open(file_name) as json_file:
        return json.load(json_file)


class DataOrganizer:
    def __init__(self):
        self.chile = deepcopy(BASE_PLACE)
        self.regiones_es = []
        self.regiones_extra = []
        self.fixed_regiones = {}
        self.fixed_comunas = {}
        self.regiones_comunas = []

    def load_input(self):
        # Load regiones names
        self.regiones_es = load_json("./names/regiones_es.json")
        self.regiones_extra = load_json("./names/regiones_extra.json")
        self.all_regiones = {*self.regiones_es, *self.regiones_extra}
        # Load fixed regiones names
        self.fixed_regiones = load_json("./names/fixed_regiones.json")
        # Load comunas names
        self.comunas_es = load_json("./names/comunas_es.json")
        self.comunas_en = load_json("./names/comunas_en.json")
        self.comunas_extra = load_json("./names/comunas_extra.json")
        self.all_comunas = {
            *self.comunas_es,
            *self.comunas_en,
            *self.comunas_extra,
        }
        # Load fixed comunas names
        self.fixed_comunas = {
            **load_json("./names/fixed_comunas.json"),
            **load_json("./names/fixed_comunas_extra.json"),
        }
        # Load {comuna: region} associations
        self.regiones_comunas = load_json("./names/regiones_comunas.json")

    def fix_region(self, region):
        return self.fixed_regiones.get(region, region)

    def fix_comuna(self, comuna):
        return self.fixed_comunas.get(comuna, comuna)

    def fill_chile_data(self):
        # Poblacion
        self.chile["poblacion"] = CHILEAN_POPULATION
        # Cifras totales
        totales_nacionales = csv.DictReader(
            open("./minciencia_data/TotalesNacionales.csv"))
        totales_nacionales_keys = json.load(
            open("./minciencia_keys/TotalesNacionales.json"))
        for row in totales_nacionales:
            data_label = totales_nacionales_keys[row["Fecha"]]
            data_values = [
                {"date": date, "value": parse_string_int(value)}
                for date, value in row.items() if date != "Fecha"
            ]
            self.chile["series"][data_label] = data_values
            self.chile[data_label] = data_values[-1]["value"]
            self.chile["previous"][data_label] = data_values[-2]["value"]

    def fill_regiones_data(self):
        self.chile["regiones"] = {
            region: {**deepcopy(BASE_PLACE), "comunas": {}}
            for region in self.regiones_es
        }
        # Confirmados
        casos_totales_cumulativo = csv.DictReader(
            open("./minciencia_data/CasosTotalesCumulativo.csv"))
        for row in casos_totales_cumulativo:
            if row["Region"] in self.all_regiones:
                region = self.fix_region(row["Region"])
                data_values = [
                    {"date": date, "value": parse_string_int(value)}
                    for date, value in row.items() if date != "Region"
                ]
                self.chile["regiones"][region]["series"]["confirmados"] = data_values
                self.chile["regiones"][region]["confirmados"] = data_values[-1]["value"]
                self.chile["regiones"][region]["previous"]["confirmados"] = data_values[-2]["value"]
        # Fallecidos
        fallecidos_cumulativo = csv.DictReader(
            open("./minciencia_data/FallecidosCumulativo.csv"))
        for row in fallecidos_cumulativo:
            if row["Region"] in self.all_regiones:
                region = self.fix_region(row["Region"])
                data_values = [
                    {"date": date, "value": parse_string_int(value)}
                    for date, value in row.items() if date != "Region"
                ]
                self.chile["regiones"][region]["series"]["fallecidos"] = data_values
                self.chile["regiones"][region]["fallecidos"] = data_values[-1]["value"]
                self.chile["regiones"][region]["previous"]["fallecidos"] = data_values[-2]["value"]

    def fill_comunas_data(self):
        for comuna in self.comunas_es:
            self.chile["regiones"][self.regiones_comunas[comuna]]["comunas"][comuna] = deepcopy(BASE_PLACE)
        # Activos
        casos_activos_por_comuna = csv.DictReader(
            open("./minciencia_data/CasosActivosPorComuna.csv"))
        for row in casos_activos_por_comuna:
            region = self.fix_region(row["Region"])
            poblacion = parse_string_int(row["Poblacion"])
            data_values = [
                {"date": date, "value": parse_string_int(value)}
                for date, value in row.items() if date.startswith("20")
            ]
            # Activos - Comuna
            if row["Comuna"] in self.all_comunas:
                comuna = self.fix_comuna(row["Comuna"])
                self.chile["regiones"][region]["comunas"][comuna]["poblacion"] = poblacion
                self.chile["regiones"][region]["comunas"][comuna]["series"]["activos"] = data_values
                self.chile["regiones"][region]["comunas"][comuna]["activos"] = data_values[-1]["value"]
                self.chile["regiones"][region]["comunas"][comuna]["previous"]["activos"]  = data_values[-2]["value"]
            # Activos - Region
            elif row["Comuna"] == "Total":
                self.chile["regiones"][region]["poblacion"] = poblacion
                self.chile["regiones"][region]["series"]["activos"] = data_values
                self.chile["regiones"][region]["activos"] = data_values[-1]["value"]
                self.chile["regiones"][region]["previous"]["activos"]  = data_values[-2]["value"]

    def calculate_recuperados_regiones(self):
        for region in self.chile["regiones"]:
            current = self.chile["regiones"][region]
            previous = self.chile["regiones"][region]["previous"]
            self.chile["regiones"][region]["recuperados"] = current["confirmados"] - current["activos"] - current["fallecidos"]
            self.chile["regiones"][region]["previous"]["recuperados"] = previous["confirmados"] - previous["activos"] - previous["fallecidos"]

    def save_data(self, minify=True):
        with open("chile.json", "w") as outfile:
            if minify:
                json.dump(self.chile, outfile)
            else:
                json.dump(self.chile, outfile, indent=4)


organizer = DataOrganizer()
organizer.load_input()
organizer.fill_chile_data()
organizer.fill_regiones_data()
organizer.fill_comunas_data()
organizer.calculate_recuperados_regiones()
organizer.save_data(minify=True)
