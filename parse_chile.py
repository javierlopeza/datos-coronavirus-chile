import csv

def clean_list_string_numbers(l):
    l = [int(float(e)) if e else None for e in l]
    if l[0] is None:
        l[0] = l[1]
    for i in range(1, len(l)):
        if l[i] is None:
            l[i] = l[i - 1]
    return l

def write_values(filename, dates, source):
    with open('./raw_data/chile/' + filename, 'w', newline='') as csvfile:
        fieldnames = ['date', 'value', 'is_from_informe_epi']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for i in range(len(dates)):
            writer.writerow({'date': dates[i], 'value': source[i], 'is_from_informe_epi': 'TRUE'})

with open('./input_files/TotalesNacionales.csv', newline='') as csvfile:
    reader = csv.reader(csvfile, delimiter=',')
    for row in reader:
        if row[0] == "Fecha":
            dates = row[1:]
        if row[0] == "Casos totales":
            totales = clean_list_string_numbers(row[1:])
        if row[0] == "Fallecidos":
            fallecidos = clean_list_string_numbers(row[1:])
        # if row[0] == "Casos activos":
        #     activos = clean_list_string_numbers(row[1:])

write_values("serie_confirmados_chile.csv", dates, totales)
write_values("serie_fallecidos_chile.csv", dates, fallecidos)

with open('./input_files/activos_vs_recuperados.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile, delimiter=',')
    dates = []
    activos = []
    for row in reader:
        dates.append(row['fecha_primeros_sintomas'])
        activos.append(row['activos'])
write_values("serie_activos_chile.csv", dates, activos)