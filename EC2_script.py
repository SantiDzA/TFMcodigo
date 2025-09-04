import requests
from geopy.distance import distance
import numpy as np
from keras.models import load_model
import csv
import time
import io
import boto3

TOKEN = "????"
MAX_LAT, MIN_LAT, MIN_LON, MAX_LON = (44.889, 41.292, -5.041, -0.209)
GRID_SIZE = 500
INTERP = GRID_SIZE*2
aeropuertos = ("BIO", "EAS", "PNA", "VIT")
orig = (43.09, -2.66)
distancia = 200

def descargar_datos():
    url = "https://fr24api.flightradar24.com/api/live/flight-positions/full"
    params = {
        'bounds': f'{MAX_LAT},{MIN_LAT},{MIN_LON},{MAX_LON}',
        "airports": "inbound:VIT,inbound:BIO,inbound:PNA,inbound:EAS"
    }
    headers = {
        'Accept': 'application/json',
        'Accept-Version': 'v1',
        'Authorization': f'Bearer {TOKEN}'
    }

    try:
        response = requests.get(url, headers=headers, params=params)
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()["data"]
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        return list()
    except Exception as err:
        print(f"An error occurred: {err}")
        return list()

def descargar_track_con_id(id_vuelo):
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
        return response.json()[0]["tracks"]
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Exception as err:
        print(f"An error occurred: {err}")

def filtrar_datos(lista):
    lista_filtrada = list()
    for elem in lista:
        distancia = distance(orig, (elem['lat'], elem['lon'])).km
        if distancia < 200:
            lista_filtrada.append(elem)
    return lista_filtrada

def recortar_trayectoria(track):
    final = list()
    for p in track:
        if distance(orig, (p['lat'], p['lon'])).km < 200 and p["alt"] > 0:
            final.append((p["lat"], p["lon"]))
    return final

# Rejilla
def convertir_rejilla(lat, lon, min_lat, max_lat, min_lon, max_lon, grid_size):
    rej_x = int((lon - min_lon) / (max_lon - min_lon) * (grid_size - 1))
    rej_y = int((lat - min_lat) / (max_lat - min_lat) * (grid_size - 1))
    return rej_x, rej_y

def interpolar_rejilla(grid_points):
    interpolated_points = []
    for i in range(len(grid_points) - 1):
        x1, y1 = grid_points[i]
        x2, y2 = grid_points[i + 1]
        # Número de pasos
        num_steps = max(abs(x2 - x1), abs(y2 - y1))
        # Interpolar
        x_interp = np.linspace(x1, x2, num_steps).astype(int)
        y_interp = np.linspace(y1, y2, num_steps).astype(int)
        interpolated_points.extend(list(zip(x_interp, y_interp)))
    return interpolated_points

def crear_rejilla(lista_puntos):
    puntos_rejilla = []
    for p in lista_puntos:
        x, y = convertir_rejilla(p[0], p[1], MIN_LAT, MAX_LAT, MIN_LON, MAX_LON, GRID_SIZE)
        puntos_rejilla.append((x, y))
    interpolados = interpolar_rejilla(puntos_rejilla)
    rejilla = np.zeros((GRID_SIZE, GRID_SIZE))
    for x, y in interpolados:
        rejilla[y, x] = 1
    return np.expand_dims(rejilla, axis=(0, -1))

#######
def principal():
    datos_base = descargar_datos()[:9] # Recortamos a 9 para hacer como mucho 10 llamadas a la API
    datos_filtrados = filtrar_datos(datos_base)
    resultados = dict()
    destinos = dict()
    for vuelo in datos_filtrados:
        datos_trayectoria = descargar_track_con_id(vuelo["fr24_id"])
        trayectoria_filtrada = recortar_trayectoria(datos_trayectoria)
        rejilla = crear_rejilla(trayectoria_filtrada)
        resultados[vuelo["fr24_id"]] = rejilla
        destinos[vuelo["fr24_id"]] = vuelo["dest_iata"]

    # Evaluación
    rows = list()
    model = load_model("modelo2D_3_capas.keras")

    for k, v in resultados.items():
        prediction = model.predict(v)
        pred_label = aeropuertos[np.argmax(prediction)]
        rows.append((k, destinos[k], pred_label))
    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)
    timestamp = str(int(time.time()))
    writer.writerow(['ID', 'Destino Real', 'Destino Predicho'])
    writer.writerows(rows)
    s3 = boto3.client('s3')
    s3.put_object(Bucket="tfm-aviones", Key=f"results/{timestamp}.csv", Body=csv_buffer.getvalue())

principal()
