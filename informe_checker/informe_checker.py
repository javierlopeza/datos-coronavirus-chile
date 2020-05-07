import bs4 as bs
import dateparser
import pendulum
import sys
import os
import os.path

source = ""
for line in sys.stdin:
    source += line

soup = bs.BeautifulSoup(source, features="html.parser")

informes_table = soup.find(id="informes").find("table")
last_informe_date_str = informes_table.find("tbody").find("tr").find("td").string
last_informe_date = dateparser.parse(last_informe_date_str, languages=['es']).strftime('%Y-%m-%d')

if os.path.isfile("../informe_parser/input/tablas_informe_{}.pdf".format(last_informe_date)):
    sys.exit(1)
else:
    sys.exit(0)
