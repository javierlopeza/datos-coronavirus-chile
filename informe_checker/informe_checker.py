import bs4 as bs
import dateparser
import pendulum
import sys, os

source = ""
for line in sys.stdin:
    source += line

soup = bs.BeautifulSoup(source, features="html.parser")

informes_table = soup.find(id="informes").find("table")
last_informe_date_str = informes_table.find("tbody").find("tr").find("td").string
last_informe_date = dateparser.parse(last_informe_date_str, languages=['es']).strftime('%Y-%m-%d')
today = pendulum.now("America/Santiago").format("YYYY-MM-DD")

if (last_informe_date == today):
    sys.exit(0)
else:
    sys.exit(1)
