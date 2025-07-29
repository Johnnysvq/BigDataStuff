import pandas as pd

df = pd.read_parquet('Yellow_Trip/yellow_tripdata_2025-01.parquet', engine="pyarrow")

# Carga el CSV y filtra los LocationID por Manhattan
df_locations = pd.read_csv('Yellow_Trip/taxi_zone_lookup.csv')
manhattanIds = df_locations[df_locations['Borough'] == 'Manhattan']['LocationID'].tolist()

pd.set_option('display.max_colwidth', None)

columnasUtilez = ['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'PULocationID', 'DOLocationID']

df_filtrado = df[columnasUtilez].copy()

# Duracion de cada viaje
df_filtrado['trip_duration'] = (df_filtrado['tpep_dropoff_datetime'] - df_filtrado['tpep_pickup_datetime']).dt.total_seconds() /60
df_filtrado['trip_duration'] = df_filtrado['trip_duration'].apply(lambda x: f"{int(x):02d}:{int((x%1)*60):02d}")

print(df_filtrado.head(50))


# Viajes en inician y terminan en Manhattan
pickup_manhattan = df_filtrado[df_filtrado['PULocationID'].isin(manhattanIds)]
dropoff_manhattan = df_filtrado[df_filtrado['DOLocationID'].isin(manhattanIds)]

pickup_and_dropoff_manhattan = df_filtrado[df_filtrado['PULocationID'].isin(manhattanIds) | df_filtrado['DOLocationID'].isin(manhattanIds)]
print('Viajes que inician o terminan en Manhattan')
print(pickup_and_dropoff_manhattan.head(50))

print("Total de viajes sin filtrar :", df_filtrado.shape[0])
print("Viajes que inician o terminan en Manhattan:", pickup_and_dropoff_manhattan.shape[0])

# Guarda en un archivo nuevo de Parquet
pickup_and_dropoff_manhattan.to_parquet('Yellow_Trip/pickup_and_dropoff_manhattan.parquet', index=False, engine="pyarrow")