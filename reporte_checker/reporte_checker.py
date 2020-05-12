import bs4 as bs
import dateparser
import pendulum
import sys
import os
import os.path
import requests

GOV_URL = "https://www.gob.cl/coronavirus/cifrasoficiales/"
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.76 Safari/537.36', "Upgrade-Insecure-Requests": "1",
           "DNT": "1", "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", "Accept-Language": "en-US,en;q=0.5", "Accept-Encoding": "gzip, deflate"}

source = requests.get(GOV_URL, timeout=10, headers=headers)
soup = bs.BeautifulSoup(source.content, features="html.parser")

last_reporte_anchor = soup.find(id="reportes").find("a")
last_reporte_date_str = last_reporte_anchor.text.strip()
last_reporte_date = dateparser.parse(last_reporte_date_str, languages=['es']).strftime('%Y-%m-%d')

if os.path.isfile("../reporte_parser/input/tablas_reporte_{}.pdf".format(last_reporte_date)):
    print("No Action: last reporte already exists.")
    sys.exit(1)
else:
    print("Action: a new reporte was published, let's create an issue for that.")
    sys.exit(0)
