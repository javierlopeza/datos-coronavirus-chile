import json
import compress_json
import csv
from collections import OrderedDict
from copy import deepcopy
import pendulum

CHILEAN_POPULATION = 19458310
BASE_PLACE = {
    "poblacion": None,
    "confirmados": {"date": None, "value": None},
    "activos": {"date": None, "value": None},
    "fallecidos": {"date": None, "value": None},
    "tasa_activos": {"date": None, "value": None},
    "previous": {
        "confirmados": {"date": None, "value": None},
        "activos": {"date": None, "value": None},
        "fallecidos": {"date": None, "value": None},
        "tasa_activos": {"date": None, "value": None},
    },
    "series": {
        "confirmados": [],
        "activos": [],
        "fallecidos": [],
    },
    "quarantine": {
        "text": None,
        "is_active": False,
    },
}


def parse_string_int(s):
    try:
        return int(float(s))
    except:
        return None


def parse_series_values(serie):
    for e in serie:
        e["value"] = parse_string_int(e["value"])
    return serie


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
        # Load comunas codes
        self.comunas_codes = load_json("./names/comunas_codes.json")
        # Load {comuna: region} associations
        self.regiones_comunas = load_json("./names/regiones_comunas.json")

    def fix_region(self, region):
        return self.fixed_regiones.get(region, region)

    def fix_comuna(self, comuna):
        return self.fixed_comunas.get(comuna, comuna)

    def fill_chile_data(self):
        # Poblacion
        self.chile["poblacion"] = CHILEAN_POPULATION

        # Confirmados
        serie_confirmados_csv = csv.DictReader(
            open("./raw_data/chile/serie_confirmados_chile.csv"))
        serie_confirmados = parse_series_values(list(serie_confirmados_csv))
        # Fallecidos
        serie_fallecidos_csv = csv.DictReader(
            open("./raw_data/chile/serie_fallecidos_chile.csv"))
        serie_fallecidos = parse_series_values(list(serie_fallecidos_csv))
        # Activos
        serie_activos_csv = csv.DictReader(
            open("./raw_data/chile/serie_activos_chile.csv"))
        serie_activos = parse_series_values(list(serie_activos_csv))

        # Add all series
        self.chile["series"] = {
            "confirmados": serie_confirmados,
            "fallecidos": serie_fallecidos,
            "activos": serie_activos,
        }
        # Add current values
        self.chile["confirmados"] = serie_confirmados[-1]
        self.chile["fallecidos"] = serie_fallecidos[-1]
        self.chile["activos"] = serie_activos[-1]
        # Add previous values
        self.chile["previous"]["confirmados"] = serie_confirmados[-2]
        self.chile["previous"]["fallecidos"] = serie_fallecidos[-2]
        self.chile["previous"]["activos"] = next(e for e in reversed(serie_activos[:-1]) if e["value"] is not None)
        serie_activos[-2]
        # Add tasa activos
        self.chile["tasa_activos"] = {
            "date": self.chile["activos"]["date"],
            "value": round((self.chile["activos"]["value"] / self.chile["poblacion"]) * 100000, 2),
        }
        # Add previous tasa activos
        self.chile["previous"]["tasa_activos"] = {
            "date": self.chile["previous"]["activos"]["date"],
            "value": round((self.chile["previous"]["activos"]["value"] / self.chile["poblacion"]) * 100000, 2),
        }

    def fill_regiones_data(self):
        self.chile["regiones"] = {
            region: {**deepcopy(BASE_PLACE), "comunas": {}}
            for region in self.regiones_es
        }
        # Add poblaciones
        poblaciones_csv = csv.DictReader(open("./raw_data/regiones/poblaciones_regiones.csv"))
        poblaciones = parse_series_values(list(poblaciones_csv))
        for row in poblaciones:
            region = self.fix_region(row["region"])
            poblacion = parse_string_int(row["value"])
            self.chile["regiones"][region]["poblacion"] = poblacion

        # Add confirmados
        series_confirmados_csv = csv.DictReader(open("./raw_data/regiones/series_confirmados_regiones.csv"))
        for row in series_confirmados_csv:
            region = self.fix_region(row["region"])
            serie_confirmados = [
                {"date": date, "value": parse_string_int(value)}
                for date, value in row.items() if date != "region"
            ]
            self.chile["regiones"][region]["series"]["confirmados"] = serie_confirmados
            self.chile["regiones"][region]["confirmados"] = serie_confirmados[-1]
            self.chile["regiones"][region]["previous"]["confirmados"] = serie_confirmados[-2]
        # Add fallecidos
        series_fallecidos_csv = csv.DictReader(open("./raw_data/regiones/series_fallecidos_regiones.csv"))
        for row in series_fallecidos_csv:
            region = self.fix_region(row["region"])
            serie_fallecidos = [
                {"date": date, "value": parse_string_int(value)}
                for date, value in row.items() if date != "region"
            ]
            self.chile["regiones"][region]["series"]["fallecidos"] = serie_fallecidos
            self.chile["regiones"][region]["fallecidos"] = serie_fallecidos[-1]
            self.chile["regiones"][region]["previous"]["fallecidos"] = serie_fallecidos[-2]
        # Add activos
        series_activos_csv = csv.DictReader(open("./raw_data/regiones/series_activos_regiones.csv"))
        for row in series_activos_csv:
            region = self.fix_region(row["region"])
            serie_activos = [
                {"date": date, "value": parse_string_int(value)}
                for date, value in row.items() if date != "region"
            ]
            self.chile["regiones"][region]["series"]["activos"] = serie_activos
            self.chile["regiones"][region]["activos"] = serie_activos[-1]
            self.chile["regiones"][region]["previous"]["activos"] = serie_activos[-2]
            # Add tasa activos
            current_activos = self.chile["regiones"][region]["activos"]
            poblacion = self.chile["regiones"][region]["poblacion"]
            self.chile["regiones"][region]["tasa_activos"] = {
                "date": current_activos["date"],
                "value": round((current_activos["value"]/ poblacion) * 100000, 2),
            }
            # Add previous tasa activos
            previous_activos = self.chile["regiones"][region]["previous"]["activos"]
            poblacion = self.chile["regiones"][region]["poblacion"]
            self.chile["regiones"][region]["previous"]["tasa_activos"] = {
                "date": previous_activos["date"],
                "value": round((previous_activos["value"]/ poblacion) * 100000, 2),
            }

    def fill_comunas_data(self):
        for comuna in self.comunas_es:
            self.chile["regiones"][self.regiones_comunas[comuna]]["comunas"][comuna] = deepcopy(BASE_PLACE)
            self.chile["regiones"][self.regiones_comunas[comuna]]["comunas"][comuna]["name"] = comuna

        # Add regiones
        for region in self.chile["regiones"]:
            for comuna in self.chile["regiones"][region]["comunas"]:
                self.chile["regiones"][region]["comunas"][comuna]["region"] = region
        
        # Add poblaciones
        poblaciones_csv = csv.DictReader(open("./raw_data/comunas/poblaciones_comunas.csv"))
        poblaciones = parse_series_values(list(poblaciones_csv))
        for row in poblaciones:
            comuna = self.fix_comuna(row["comuna"])
            region = self.fix_region(self.regiones_comunas[comuna])
            poblacion = parse_string_int(row["value"])
            self.chile["regiones"][region]["comunas"][comuna]["poblacion"] = poblacion

        # Add confirmados
        series_confirmados_csv = csv.DictReader(open("./raw_data/comunas/series_confirmados_comunas.csv"))
        for row in series_confirmados_csv:
            comuna = self.fix_comuna(row["comuna"])
            region = self.fix_region(self.regiones_comunas[comuna])
            serie_confirmados = [
                {"date": date, "value": parse_string_int(value)}
                for date, value in row.items() if date != "comuna"
            ]
            self.chile["regiones"][region]["comunas"][comuna]["series"]["confirmados"] = serie_confirmados
            self.chile["regiones"][region]["comunas"][comuna]["confirmados"] = serie_confirmados[-1]
            self.chile["regiones"][region]["comunas"][comuna]["previous"]["confirmados"] = serie_confirmados[-2]

        # Add fallecidos
        series_fallecidos_csv = csv.DictReader(open("./raw_data/comunas/series_fallecidos_comunas.csv"))
        for row in series_fallecidos_csv:
            comuna = self.fix_comuna(row["comuna"])
            region = self.fix_region(self.regiones_comunas[comuna])
            serie_fallecidos = [
                {"date": date, "value": parse_string_int(value)}
                for date, value in row.items() if date != "comuna"
            ]
            self.chile["regiones"][region]["comunas"][comuna]["fallecidos"] = serie_fallecidos[-1]

        # Add activos
        series_activos_csv = csv.DictReader(open("./raw_data/comunas/series_activos_comunas.csv"))
        for row in series_activos_csv:
            comuna = self.fix_comuna(row["comuna"])
            region = self.fix_region(self.regiones_comunas[comuna])
            serie_activos = [
                {"date": date, "value": parse_string_int(value)}
                for date, value in row.items() if date != "comuna"
            ]
            self.chile["regiones"][region]["comunas"][comuna]["series"]["activos"] = serie_activos
            self.chile["regiones"][region]["comunas"][comuna]["activos"] = serie_activos[-1]
            self.chile["regiones"][region]["comunas"][comuna]["previous"]["activos"] = serie_activos[-2]
            # Add tasa activos
            current_activos = self.chile["regiones"][region]["comunas"][comuna]["activos"]
            poblacion = self.chile["regiones"][region]["comunas"][comuna]["poblacion"]
            self.chile["regiones"][region]["comunas"][comuna]["tasa_activos"] = {
                "date": current_activos["date"],
                "value": round((current_activos["value"] / poblacion) * 100000, 2),
            }
            # Add previous tasa activos
            previous_activos = self.chile["regiones"][region]["comunas"][comuna]["previous"]["activos"]
            poblacion = self.chile["regiones"][region]["comunas"][comuna]["poblacion"]
            self.chile["regiones"][region]["comunas"][comuna]["previous"]["tasa_activos"] = {
                "date": previous_activos["date"],
                "value": round((previous_activos["value"] / poblacion) * 100000, 2),
            }
            # Add delta activos
            self.chile["regiones"][region]["comunas"][comuna]["delta"] = {
                "activos": {
                    "from_date": previous_activos["date"],
                    "to_date": current_activos["date"],
                    "value": current_activos["value"] - previous_activos["value"],
                },
                "tasa_activos": {
                    "from_date": previous_activos["date"],
                    "to_date": current_activos["date"],
                    "value": round(((current_activos["value"] - previous_activos["value"]) / poblacion) * 100000, 2),
                },
            }


    def add_regiones_complete_names(self):
        for region in self.chile["regiones"]:
            self.chile["regiones"][region]["complete_name"] = self.complete_regiones[region]

    def add_quarantines_to_communes(self):
        quarantines = csv.DictReader(
            open("./raw_data/quarantines.csv"))
        for row in quarantines:
            region = self.fix_region(row["region"])
            commune = self.fix_comuna(row["commune"])
            # Add quarantine info to commune
            self.chile["regiones"][region]["comunas"][commune]["quarantine"] = {
                "is_active": row["is_active"] == "TRUE",
                "text": row["text"],
            }

    def remove_unused_data(self):
        """
        Chile
        - tasa_activos
        - previous
        - series.confirmados
        - series.fallecidos
        """
        del self.chile["tasa_activos"]
        del self.chile["previous"]
        del self.chile["series"]["confirmados"]
        del self.chile["series"]["fallecidos"]

        """
        Regiones
        - tasa_activos
        - previous
        - series.confirmados
        - series.fallecidos
        """
        for region in self.chile["regiones"]:
            del self.chile["regiones"][region]["tasa_activos"]
            del self.chile["regiones"][region]["previous"]
            del self.chile["regiones"][region]["series"]["confirmados"]
            del self.chile["regiones"][region]["series"]["fallecidos"]

        """
        Comunas
        - previous.confirmados
        - previous.fallecidos
        - series.confirmados
        - series.fallecidos
        """

        # All previous data in regiones
        for region in self.chile["regiones"]:
            for comuna in self.chile["regiones"][region]["comunas"]:
                comuna_obj = self.chile["regiones"][region]["comunas"][comuna]
                del comuna_obj["previous"]["confirmados"]
                del comuna_obj["previous"]["fallecidos"]
                del comuna_obj["series"]["confirmados"]
                del comuna_obj["series"]["fallecidos"]

    def save_data(self):
        with open("./data/chile.json", "w") as outfile:
            json.dump(self.chile, outfile, indent=4)
        with open("./data/chile-minified.json", "w") as outfile:
            json.dump(self.chile, outfile)


organizer = DataOrganizer()
organizer.load_input()
organizer.fill_chile_data()
organizer.fill_regiones_data()
organizer.fill_comunas_data()
organizer.add_regiones_complete_names()
organizer.add_quarantines_to_communes()
organizer.remove_unused_data()
organizer.save_data()
