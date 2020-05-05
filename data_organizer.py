import json
import compress_json
import csv
from collections import OrderedDict
from copy import deepcopy

CHILEAN_POPULATION = 19458310
BASE_PLACE = {
    "poblacion": None,
    "tasa_activos": {"date": None, "value": None},
    "confirmados": {"date": None, "value": None},
    "activos": {"date": None, "value": None},
    "recuperados": {"date": None, "value": None},
    "fallecidos": {"date": None, "value": None},
    "previous": {
        "confirmados": {"date": None, "value": None},
        "activos": {"date": None, "value": None},
        "recuperados": {"date": None, "value": None},
        "fallecidos": {"date": None, "value": None},
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

    def load_input(self):
        # Load regiones names
        self.regiones_es = load_json("./names/regiones_es.json")
        self.regiones_extra = load_json("./names/regiones_extra.json")
        self.all_regiones = {*self.regiones_es, *self.regiones_extra}
        # Load fixed regiones names
        self.fixed_regiones = load_json("./names/fixed_regiones.json")
        # Load regiones complete names
        self.complete_regiones = load_json("./names/complete_regiones.json")
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
            self.chile[data_label] = data_values[-1]
            self.chile["previous"][data_label] = data_values[-2]
        # Tasa activos
        self.chile["tasa_activos"] = {
            "date": self.chile["activos"]["date"],
            "value": (self.chile["activos"]["value"] / self.chile["poblacion"]) * 100000,
        }

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
                self.chile["regiones"][region]["confirmados"] = data_values[-1]
                self.chile["regiones"][region]["previous"]["confirmados"] = data_values[-2]
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
                self.chile["regiones"][region]["fallecidos"] = data_values[-1]
                self.chile["regiones"][region]["previous"]["fallecidos"] = data_values[-2]

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
                self.chile["regiones"][region]["comunas"][comuna]["activos"] = data_values[-1]
                self.chile["regiones"][region]["comunas"][comuna]["previous"]["activos"]  = data_values[-2]
                # Tasa activos - Comuna
                activos = self.chile["regiones"][region]["comunas"][comuna]["activos"]
                poblacion = self.chile["regiones"][region]["comunas"][comuna]["poblacion"]
                self.chile["regiones"][region]["comunas"][comuna]["tasa_activos"] = {
                    "date": activos["date"],
                    "value": (activos["value"] / poblacion) * 100000,
                }
            # Activos - Region
            elif row["Comuna"] == "Total":
                self.chile["regiones"][region]["poblacion"] = poblacion
                self.chile["regiones"][region]["series"]["activos"] = data_values
                self.chile["regiones"][region]["activos"] = data_values[-1]
                self.chile["regiones"][region]["previous"]["activos"]  = data_values[-2]
                # Tasa activos - Region
                activos = self.chile["regiones"][region]["activos"]
                poblacion = self.chile["regiones"][region]["poblacion"]
                self.chile["regiones"][region]["tasa_activos"] = {
                    "date": activos["date"],
                    "value": (activos["value"] / poblacion) * 100000,
                }

    def calculate_recuperados_regiones(self):
        for region in self.chile["regiones"]:
            current = self.chile["regiones"][region]
            previous = self.chile["regiones"][region]["previous"]
            self.chile["regiones"][region]["recuperados"] = {
                "date": current["confirmados"]["date"],
                "value": current["confirmados"]["value"] - current["activos"]["value"] - current["fallecidos"]["value"]
            }
            self.chile["regiones"][region]["previous"]["recuperados"] = {
                "date": previous["confirmados"]["date"],
                "value": previous["confirmados"]["value"] - previous["activos"]["value"] - previous["fallecidos"]["value"]
            }
    
    def add_regiones_complete_names(self):
        for region in self.chile["regiones"]:
            self.chile["regiones"][region]["complete_name"] = self.complete_regiones[region]

    def save_data(self, compress=False, minify=False):
        if compress:
            compress_json.dump(self.chile, "./data/chile.json.gz")
            return
        with open("./data/chile.json", "w") as outfile:
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
organizer.add_regiones_complete_names()
organizer.save_data(compress=False, minify=True)