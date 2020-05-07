import bs4 as bs
import pycurl
from io import BytesIO
import dateparser
import pendulum
import sys, os

GOV_URL = "https://www.gob.cl/coronavirus/cifrasoficiales/"


def curl(url):
    b_obj = BytesIO()
    crl = pycurl.Curl()
    # Set URL value
    crl.setopt(crl.URL, GOV_URL)
    # Write bytes that are utf-8 encoded
    crl.setopt(crl.WRITEDATA, b_obj)
    # Perform a file transfer
    crl.perform()
    # End curl session
    crl.close()
    # Get the content stored in the BytesIO object (in byte characters)
    get_body = b_obj.getvalue()
    # Decode the bytes stored in get_body to HTML and return the result
    return get_body.decode('utf8')


source = curl(GOV_URL)
soup = bs.BeautifulSoup(source, features="html.parser")

informes_table = soup.find(id="informes").find("table")
last_informe_date_str = informes_table.find("tbody").find("tr").find("td").string
last_informe_date = dateparser.parse(last_informe_date_str, languages=['es']).strftime('%Y-%m-%d')
today = pendulum.now("America/Santiago").format("YYYY-MM-DD")

if (last_informe_date == today):
    sys.exit(0)
else:
    sys.exit(1)
