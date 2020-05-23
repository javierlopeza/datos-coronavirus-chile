import camelot
import json
import csv
from collections import OrderedDict

INPUT_DATE = "2020-05-22"


def load_json(file_name):
    with open(file_name) as json_file:
        return json.load(json_file)


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

    def scrap_activos_por_comuna(self):
        self.activos_por_comuna = OrderedDict({comuna: None for comuna in self.comunas_es})
        for table in self.tables:
            for row in table.df.itertuples(index=True, name="Pandas"):
                if row[1] in self.all_comunas:
                    nombre_comuna = self.fix_comuna(row[1])
                    activos_comuna = row[7]
                    self.activos_por_comuna[nombre_comuna] = activos_comuna

    def save_activos_por_comuna(self):
        activos_por_comuna_data = [
            {"comuna": comuna, "activos": activos}
            for comuna, activos in self.activos_por_comuna.items()
        ]
        with open("./output/activos_por_comuna_{}.csv".format(INPUT_DATE), "w") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=["comuna", "activos"])
            writer.writeheader()
            for data in activos_por_comuna_data:
                writer.writerow(data)

    def merge(self):
        last_data_path = "../minciencia_data/CasosActivosPorComuna.csv"
        with open(last_data_path, 'r') as csv_file:
            reader = iter(list(csv.reader(csv_file)))
        with open(last_data_path, 'w') as csv_file:
            writer = csv.writer(csv_file, lineterminator='\n')
            # Add new informe's date header
            new_rows = []
            header = next(reader)
            header.append(INPUT_DATE)
            new_rows.append(header)
            # Add comunas' new activos
            region_activos = 0
            for row in reader:
                if row[2] != "Total":
                    comuna = self.fix_comuna(row[2])
                    comuna_activos = int(self.activos_por_comuna[comuna]) if self.activos_por_comuna[comuna] else 0
                    row.append(comuna_activos)
                    region_activos += comuna_activos
                else:
                    row.append(region_activos)
                    region_activos = 0
                new_rows.append(row)
            # Write new rows
            writer.writerows(new_rows)


parser = InformeParser()
parser.load_input()
parser.parse_tables()
parser.scrap_activos_por_comuna()
parser.save_activos_por_comuna()
parser.merge()
