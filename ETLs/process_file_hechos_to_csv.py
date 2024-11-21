import pandas as pd

# Cargar tablas
medallero = pd.read_csv("../other-data/medals-2024.csv")
atletas = pd.read_csv("atletas.csv")
pais = pd.read_csv("pais.csv")
#inversion = pd.read_csv("inversion.csv")

# Combinar tablas
merged = medallero.merge(atletas, on="atleta_id") \
    .merge(pais, on="pais_id")

# Crear columna de desempeño promedio
merged['Desempeño_Promedio'] = (merged['Medallas_Oro'] * 3 +
                                merged['Medallas_Plata'] * 2 +
                                merged['Medallas_Bronce'])

# Agrupar y consolidar datos
hechos = merged.groupby(['Evento', 'Año', 'Deporte', 'País', 'Continente']).agg({
    'Medallas_Oro': 'sum',
    'Medallas_Plata': 'sum',
    'Medallas_Bronce': 'sum',
    'Participantes': 'count',
    'Desempeño_Promedio': 'mean'
}).reset_index()

# Guardar como CSV
hechos.to_csv("HechosDeportivos.csv", index=False)
