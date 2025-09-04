import requests
import json
import csv
from datetime import datetime, timedelta
import time
import os

TOKEN = "aquivaeltoken"


def descargar_track_con_id(id_vuelo, directorio, num_vuelo):
    url = "https://fr24api.flightradar24.com/api/flight-tracks"
    params = {
        'flight_id': id_vuelo
    }
    headers = {
        'Accept': 'application/json',
        'Accept-Version': 'v1',
        'Authorization': f'Bearer {TOKEN}'
    }

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        tracks = response.json()[0]["tracks"]
        with open(f'{directorio}/{num_vuelo}_{id_vuelo}.csv', 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=tracks[0].keys())
            writer.writeheader()
            writer.writerows(tracks)
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Exception as err:
        print(f"An error occurred: {err}")
    time.sleep(6.5)

def sumar_dias(fecha):
    fecha_form = datetime.strptime(fecha, "%Y-%m-%d")
    return (fecha_form + timedelta(days=13)).strftime("%Y-%m-%d")


def descargar_por_num_vuelo(numero_vuelo, inicio, es_callsign, destino):
    # Construir parámetros
    param_vuelo = "callsigns" if es_callsign else "flights"
    url = "https://fr24api.flightradar24.com/api/flight-summary/light"
    params = {
        param_vuelo: numero_vuelo,
        'flight_datetime_from': f'{inicio}T00:00:00',
        'flight_datetime_to': f'{sumar_dias(inicio)}T23:59:59'
    }
    headers = {
        'Accept': 'application/json',
        'Accept-Version': 'v1',
        'Authorization': f'Bearer {TOKEN}'
    }
    
    flight_ids = list()
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        flight_ids = [item["fr24_id"] for item in response.json()["data"]
                      if not os.path.exists(f"datos_crudos_{destino}/{numero_vuelo}_{item['fr24_id']}.csv")]
        print(f"Encontrados {len(flight_ids)} vuelos correspondientes a {numero_vuelo}")
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Exception as err:
        print(f"An error occurred: {err}")
    time.sleep(6.5)

    # Filtrar ids de vuelos para no descargar lo que ya tenemos
    if len(flight_ids) > 0:
        for id_vuelo in flight_ids:
            descargar_track_con_id(id_vuelo, f"datos_crudos_{destino}", numero_vuelo)
