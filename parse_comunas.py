import csv

def clean_list_string_numbers(l):
    l = [int(float(e)) if e else None for e in l]
    if l[0] is None:
        l[0] = l[1]
    for i in range(1, len(l)):
        if l[i] is None:
            l[i] = l[i - 1]
    return l

def write_values(filename, comunas, dates, data):
    with open('./raw_data/comunas/' + filename, 'w', newline='') as csvfile:
        fieldnames = ['comuna'] + dates
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for i in range(len(comunas)):
            d = {'comuna': comunas[i]}
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
            if row[2].startswith("Desconocido") or row[2].startswith("Total") or row[2] == "Comuna":
                continue
            datos.append(clean_list_string_numbers(row[5:-1] if omit_last else row[5:]))
        return dates, datos

with open('./raw_data/comunas/series_activos_comunas.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    comunas = [row["comuna"] for row in reader]

dates_activos, activos = parse_dataset('CasosActivosPorComuna.csv')
dates_fallecidos, fallecidos = parse_dataset('CasosFallecidosPorComuna.csv')
dates_totales, totales = parse_dataset('Covid-19.csv')

write_values('series_activos_comunas.csv', comunas, dates_activos, activos)
write_values('series_fallecidos_comunas.csv', comunas, dates_fallecidos, fallecidos)
write_values('series_confirmados_comunas.csv', comunas, dates_totales, totales)


