import bs4 as bs
import dateparser
import pendulum
import sys
import os
import os.path
import requests
import re

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.76 Safari/537.36', "Upgrade-Insecure-Requests": "1",
           "DNT": "1", "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", "Accept-Language": "en-US,en;q=0.5", "Accept-Encoding": "gzip, deflate"}

# Gobierno - Cifras Oficiales
GOV_URL = "https://www.gob.cl/coronavirus/cifrasoficiales/"
source = requests.get(GOV_URL, timeout=10, headers=headers)
soup = bs.BeautifulSoup(source.content, features="html.parser")

informes_table = soup.find(id="informes")
last_informe_date_str = informes_table.find("a").text.strip()
last_informe_date = dateparser.parse(last_informe_date_str, languages=['es']).strftime('%Y-%m-%d')
new_informe_gov = not os.path.isfile("../informe_parser/input/tablas_informe_{}.pdf".format(last_informe_date))

# MINSAL
MINSAL_URL = "https://www.minsal.cl/nuevo-coronavirus-2019-ncov/informe-epidemiologico-covid-19/"
source = requests.get(MINSAL_URL, timeout=10, headers=headers)
soup = bs.BeautifulSoup(source.content, features="html.parser")
last_informe_date_str = soup.find("td", string=re.compile("PDF")).text.strip().split(" – ")[0]
last_informe_date = dateparser.parse(last_informe_date_str, languages=['es']).strftime('%Y-%m-%d')
new_informe_minsal = not os.path.isfile("../informe_parser/input/tablas_informe_{}.pdf".format(last_informe_date))

if not (new_informe_gov or new_informe_minsal):
    print("No Action: last informe already exists.")
    sys.exit(1)
else:
    print("Action: a new informe was published, let's create an issue for that.")
    sys.exit(0)
