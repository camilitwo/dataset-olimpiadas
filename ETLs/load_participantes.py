import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# Configuración de la base de datos
DB_CONFIG = {
    "dbname": "udp_resultados_deportivos",
    "user": "default",
    "password": "8yajkRZE5dAl",
    "host": "ep-jolly-haze-a4z1k35b-pooler.us-east-1.aws.neon.tech",
    "port": "5432",
    "sslmode": "require"
}

# Conexión a la base de datos
conn = psycopg2.connect(**DB_CONFIG)

# Carga de datos desde los CSV
eventos_df = pd.read_csv("../clean-data/results.csv")
atletas_df = pd.read_csv("../clean-data/athletes new.csv")

# Transformar datos
eventos_df['medal'] = eventos_df['medal'].str.strip().str.upper()
atletas_df['name'] = atletas_df['name'].str.upper()

# Obtener los nombres de los deportistas desde la base de datos
query = "SELECT DISTINCT atleta FROM HechosDeportivos"
db_nombres = pd.read_sql(query, conn)
db_nombres['atleta'] = db_nombres['atleta'].str.upper()

# Unir datos de género a nombres desde atletas_df
merged_df = db_nombres.merge(
    atletas_df,
    left_on='atleta',
    right_on='name',
    how='left'
)[['atleta', 'gender']]

# Reemplazar NaN por "Desconocido"
merged_df['gender'] = merged_df['gender'].fillna("Desconocido")

# Preparar datos para actualización masiva
updates = [(row['gender'], row['atleta']) for _, row in merged_df.iterrows()]

# Actualización masiva con execute_values
update_sql = """
    UPDATE hechosdeportivos
    SET genero = data.genero
    FROM (VALUES %s) AS data(genero, atleta)
    WHERE upper(hechosdeportivos.atleta) = data.atleta;
"""

try:
    with conn.cursor() as cur:
        execute_values(cur, update_sql, updates)
        conn.commit()
        print("Actualización completada exitosamente.")
except Exception as e:
    conn.rollback()
    print(f"Error al ejecutar la actualización masiva: {e}")
finally:
    conn.close()
