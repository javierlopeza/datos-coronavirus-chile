import json
import dateparser
import time

with open('./data/chile-minified.json') as f:
  data = json.load(f)

date_activos = data["regiones"]["Metropolitana"]["comunas"]["Vitacura"]["activos"]["date"]
date = dateparser.parse(date_activos)

months = [
    "enero",
    "febrero",
    "marzo",
    "abril",
    "mayo",
    "junio",
    "julio",
    "agosto",
    "septiembre",
    "octubre",
    "noviembre",
    "diciembre"
]

with open('./notifications/current_notification.json') as f:
  curr_notif = json.load(f)

curr_msg = curr_notif["message"]
new_msg = "Se han actualizado los datos de todas las comunas al {} de {}.".format(date.day, months[date.month - 1])

if (curr_msg != new_msg):
    notif = {
        "timestamp": int(time.time()),
        "title": "Datos Actualizados",
        "message": "Se han actualizado los datos de todas las comunas al {} de {}.".format(date.day, months[date.month - 1])
    }

    print(notif)

    with open('./notifications/current_notification.json', 'w') as json_file:
        json.dump(notif, json_file, indent = 4)

else:
    print("No new comunas data")