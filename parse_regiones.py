import csv

def clean_list_string_numbers(l):
    l = [int(float(e)) if e else None for e in l]
    if l[0] is None:
        l[0] = l[1]
    for i in range(1, len(l)):
        if l[i] is None:
            l[i] = l[i - 1]
    return l

def write_values(filename, region, dates, data):
    with open('./raw_data/regiones/' + filename, 'w', newline='') as csvfile:
        fieldnames = ['region'] + dates
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for i in range(len(region)):
            d = {'region': region[i]}
            d.update({dates[j]: data[i][j] for j in range(len(dates))})
            writer.writerow(d)

def parse_dataset(filename):
    with open('./input_files/' + filename, newline='') as csvfile:
        reader = csv.reader(csvfile)
        datos = []
        omit_last = False
        for row in reader:
            if row[-1] == 'Tasa':
                omit_last = True
            if row[0] == 'Region':
                dates = row[5:-1] if omit_last else row[5:]
            if row[2].startswith("Total"):
                datos.append(clean_list_string_numbers(row[5:-1] if omit_last else row[5:]))
        return dates, datos

with open('./raw_data/regiones/series_activos_regiones.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    regiones = [row["region"] for row in reader]

dates_activos, activos = parse_dataset('CasosActivosPorComuna.csv')
dates_fallecidos, fallecidos = parse_dataset('CasosFallecidosPorComuna.csv')

write_values('series_activos_regiones.csv', regiones, dates_activos, activos)
write_values('series_fallecidos_regiones.csv', regiones, dates_fallecidos, fallecidos)

# parse totales (confirmados)
with open('./input_files/CasosTotalesCumulativo.csv', newline='') as csvfile:
    reader = csv.reader(csvfile)
    datos_totales = []
    for row in reader:
        if row[0] == "Region":
            dates_confirmados = row[1:]
        if row[0] != "Region" and row[0] != "Total":
            datos_totales.append(clean_list_string_numbers(row[1:]))
write_values('series_confirmados_regiones.csv', regiones, dates_confirmados, datos_totales)





