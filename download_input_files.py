# COMUNA
# - confirmados: https://raw.githubusercontent.com/MinCiencia/Datos-COVID19/master/output/producto1/Covid-19.csv
# - activos: https://raw.githubusercontent.com/MinCiencia/Datos-COVID19/master/output/producto46/activos_vs_recuperados.csv
# - fallecidos: https://raw.githubusercontent.com/MinCiencia/Datos-COVID19/master/output/producto38/CasosFallecidosPorComuna.csv

# REGION
# - confirmados: https://raw.githubusercontent.com/MinCiencia/Datos-COVID19/master/output/producto3/CasosTotalesCumulativo.csv
# - activos: --
# - fallecidos: --

# PAIS
# https://raw.githubusercontent.com/MinCiencia/Datos-COVID19/master/output/producto5/TotalesNacionales.csv

import requests

def download_csv(url, name):
    req = requests.get(url)
    url_content = req.content
    csv_file = open('./input_files/' + name, 'wb')
    csv_file.write(url_content)
    csv_file.close()

csvs = [
    'https://raw.githubusercontent.com/MinCiencia/Datos-COVID19/master/output/producto1/Covid-19.csv',
    'https://raw.githubusercontent.com/MinCiencia/Datos-COVID19/master/output/producto19/CasosActivosPorComuna.csv',
    'https://raw.githubusercontent.com/MinCiencia/Datos-COVID19/master/output/producto46/activos_vs_recuperados.csv',
    'https://raw.githubusercontent.com/MinCiencia/Datos-COVID19/master/output/producto38/CasosFallecidosPorComuna.csv',
    'https://raw.githubusercontent.com/MinCiencia/Datos-COVID19/master/output/producto3/CasosTotalesCumulativo.csv',
    'https://raw.githubusercontent.com/MinCiencia/Datos-COVID19/master/output/producto5/TotalesNacionales.csv',
]

for url in csvs:
    download_csv(url, url.split("/")[-1])