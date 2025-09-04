import pandas as pd
import folium
import glob
import os
from geopy.distance import distance

# Variables principales
distancia = 200 # kilómetros
min_inicio = 100 # kilómetros
max_final = 3 

# Constantes
centro = (43.09, -2.66)
lista_coords = {"BIO": (43.30, -2.91), "EAS": (43.36, -1.79), "PNA": (42.77, -1.64), "VIT": (42.88, -2.73)}
lista_colores = {"BIO": "red", "VIT": "green", "EAS": "black", "PNA": "yellow"}

# Inicializar mapa
m = folium.Map(location=centro, zoom_start=9)

# Iterar por destino
for apt in lista_coords:
    # Variables básicas
    csv_folder = "./datos_crudos_" + apt
    color = lista_colores[apt]
    coords = lista_coords[apt]
    # Crear directorio si no existe
    if not os.path.exists(f"datos_filtrados_{apt}"):
        os.makedirs(f"datos_filtrados_{apt}")
    # Iteramos por cada vuelo de cada aeropuerto
    for file_path in glob.glob(os.path.join(csv_folder, "*.csv")):
        # Obtener latitud y longitud. Eliminamos las columnas originales para que todos los CSV tengan el mismo formato
        df = pd.read_csv(file_path, delimiter=',', quotechar='"')
        if "Position" in df.columns:
            df[['Latitude', 'Longitude']] = df['Position'].str.split(',', expand=True).astype(float)
            df.drop(columns=['Position'], inplace=True)
        else:
            df[['Latitude', 'Longitude']] = df[['lat', 'lon']].astype(float)
            df.drop(columns=['lat', "lon"], inplace=True)

        # Filtrar puntos a más de 200 kilómetros
        df['Distance'] = df.apply(lambda row: distance(centro, (row['Latitude'], row['Longitude'])).km, axis=1)
        df = df[df['Distance'] < distancia]
        # Puede que hayamos descartado todos los puntos, en tal caso pasar al siguiente directamente
        if len(df) == 0:
            print(f"Trayectoria errónea en {file_path}. Motivo: todos los puntos demasiado alejados")
            continue
        
        # Distancias al inicio y al final
        coordinates = list(zip(df['Latitude'], df['Longitude']))
        distancia_inicio = distance(coords, coordinates[0]).kilometers
        distancia_final = distance(coords, coordinates[-1]).kilometers

        # En caso de desvío o similar saltar y pasar al siguiente
        if distancia_inicio < min_inicio or distancia_final > max_final:
            print(f"Trayectoria errónea en {file_path}. Motivo: inicio demasiado cercano o final en otro aeropuerto.")
            continue

        # Eliminar recorrido en tierra
        if "alt" in df.columns:
            df = df.rename(columns={'alt': 'Altitude'})
        df = df[df["Altitude"] > 0]

        # Limpieza del resto de variables. No las utilizamos, pero vamos a homogeneizarlas en caso de modificación
        if "UTC" in df.columns:
            df = df.drop(columns=["Timestamp", "Callsign"])
            df = df.rename(columns={'UTC': 'Timestamp', "Direction": "Track"})
        else:
            df = df.rename(columns=
                {'timestamp': 'Timestamp', "gspeed": "Speed", "track": "Track"})
            df = df.drop(columns=["source", "vspeed", "callsign"])

        # Guardar datos
        folium.PolyLine(list(zip(df['Latitude'], df['Longitude'])), color=color, weight=2.5, opacity=1).add_to(m)
        filtered_csv_path = file_path.replace('crudos', 'filtrados')
        df.to_csv(filtered_csv_path, index=False)
        print(f"Guardado {file_path} con éxito.")

# Save map to HTML
m.save('flight_paths_map.html')
