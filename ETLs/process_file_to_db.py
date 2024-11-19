import os

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import uuid
import pycountry
import pycountry_convert as pc
from unidecode import unidecode
import json

# Configuración de la conexión a la base de datos
DB_CONFIG = {
    "dbname": "udp_resultados_deportivos",
    "user": "default",
    "password": "8yajkRZE5dAl",
    "host": "ep-jolly-haze-a4z1k35b-pooler.us-east-1.aws.neon.tech",
    "port": "5432",
    "sslmode": "require"

}

def normalize_text(text):
    return unidecode(text).lower().replace("'", "").strip()

def get_country_approximation(country_name):
    normalized_input = normalize_text(country_name)

    # Recorrer todos los países disponibles en pycountry
    for country in pycountry.countries:
        normalized_pycountry_name = normalize_text(country.name)
        if normalized_input == normalized_pycountry_name:
            return country

    # Si no se encuentra coincidencia exacta, buscar en nombres alternativos
    for country in pycountry.countries:
        if hasattr(country, "official_name"):
            normalized_official_name = normalize_text(country.official_name)
            if normalized_input == normalized_official_name:
                return country

    # Si no se encuentra coincidencia
    return None


def insert_data(conn, table_name, data, columns):
    """
    Inserta datos en la tabla especificada.
    """
    columns_sql = ", ".join(columns)
    query = f"INSERT INTO {table_name} ({columns_sql}) VALUES %s"
    values = [[row[col] for col in columns] for row in data]
    try:
        with conn.cursor() as cur:
            execute_values(cur, query, values)
            conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error al insertar datos en {table_name}: {e}")
        raise

# Generar IDs únicos para valores distintos
def generate_unique_ids(data, column_name):
    unique_values = data[column_name].dropna().unique()
    return {value: str(uuid.uuid4()) for value in unique_values}

manual_country_to_continent = {
    "Bolivia": "South America",
    "China PR": "Asia",
    "Chinese Taipei": "Asia",
    "Congo DR": "Africa",
    "Curaçao": "North America",
    "England": "Europe",
    "IR Iran": "Asia",
    "Korea DPR": "Asia",
    "Korea Republic": "Asia",
    "Kosovo": "Europe",
    "Laos": "Asia",
    "Macau": "Asia",
    "Moldova": "Europe",
    "Northern Ireland": "Europe",
    "Palestine": "Asia",
    "Republic of Ireland": "Europe",
    "Russia": "Europe",
    "São Tomé and Príncipe": "Africa",
    "Scotland": "Europe",
    "St. Kitts and Nevis": "North America",
    "St. Lucia": "North America",
    "St. Vincent and the Grenadines": "North America",
    "Syria": "Asia",
    "Tahiti": "Oceania",
    "Tanzania": "Africa",
    "Timor-Leste": "Asia",
    "Turkey": "Europe",
    "US Virgin Islands": "North America",
    "USA": "North America",
    "Venezuela": "South America",
    "Vietnam": "Asia",
    "Wales": "Europe",
    "British Virgin Islands": "North America",
    "Czech Republic": "Europe",
    "Kyrgyz Republic": "Asia"
}

# Procesar regiones
def get_continent(country_name):
    try:
        # Normalización del nombre del país
        normalized_name = normalize_text(country_name)

        # Buscar en el diccionario manual
        if country_name in manual_country_to_continent:
            return manual_country_to_continent[country_name]

        # Buscar en pycountry por aproximación
        for country in pycountry.countries:
            if normalize_text(country.name) == normalized_name:
                country_alpha2 = country.alpha_2
                continent_code = pc.country_alpha2_to_continent_code(country_alpha2)
                return pc.convert_continent_code_to_continent_name(continent_code)

        # Si no se encuentra el país
        raise ValueError(f"No se encontró el país: {country_name}")
    except Exception as e:
        print(f"Error al obtener el continente para {country_name}: {e}")
        return None

# Procesar regiones
def process_regions(data):
    """
    Procesa los datos de países y asigna continentes basados en pycountry con normalización y mapeo manual.
    """
    # Crear un mapeo de países a continentes
    country_continent_map = {}
    for country in data["name"].unique():
        continent = get_continent(country)
        if continent:
            country_continent_map[country] = continent

    # Crear regiones únicas basadas en continentes
    unique_regions = {continent: str(uuid.uuid4()) for continent in set(country_continent_map.values()) if continent}
    regions = [{"id": unique_regions[continent], "nombre": continent, "continente": continent} for continent in unique_regions]

    # Devolver las regiones y un mapeo de continentes a IDs de región
    return regions, unique_regions

# Procesar países
def process_countries(data, country_continent_map, region_map):
    """
    Procesa los datos de países y asigna el ID de la región basada en el continente.
    """
    unique_countries = data.drop_duplicates(subset=["name"])
    countries = []

    for _, row in unique_countries.iterrows():
        country_name = row["name"]
        continent = country_continent_map.get(country_name)

        if not continent:
            print(f"Advertencia: No se encontró el continente para el país {country_name}.")
            continue

        region_id = region_map.get(continent)

        if not region_id:
            print(f"Advertencia: No se encontró el ID de la región para el continente {continent}.")
            continue

        countries.append({
            "id": str(uuid.uuid4()),
            "nombre": country_name,
            "codigo": row.get("short_code", None),
            "logo_url": row.get("logo", None),
            "region_id": region_id,
        })

        unique_countries = generate_unique_ids(data, "name")

    return countries, unique_countries

# Procesar disciplinas y categorías
def process_categories_and_disciplines(data):
    unique_categories = generate_unique_ids(data, "event")
    categories = [{"id": unique_categories[value], "nombre": value} for value in unique_categories]

    unique_disciplines = generate_unique_ids(data, "discipline")
    disciplines = [
        {
            "id": unique_disciplines[value],
            "nombre": value,
            "tipo": data[data["discipline"] == value]["type"].iloc[0],
            "categoria_id": unique_categories[data[data["discipline"] == value]["event"].iloc[0]],
        }
        for value in unique_disciplines
    ]
    return categories, disciplines

# Procesar estadios
def process_stadiums(data):
    unique_stadiums = generate_unique_ids(data, "nombre_estadio")
    stadiums = [
        {
            "id": unique_stadiums[value],
            "nombre": value,
            "ubicacion": data[data["nombre_estadio"] == value]["ubicacion"].iloc[0],
            "capacidad": data[data["nombre_estadio"] == value]["capacidad"].iloc[0],
        }
        for value in unique_stadiums
    ]
    return stadiums, unique_stadiums

# Procesar competiciones
def process_competitions(data):
    unique_competitions = generate_unique_ids(data, "event")
    competitions = [
        {
            "id": unique_competitions[value],
            "nombre": value,
            "tipo": data[data["event"] == value]["type"].iloc[0],
        }
        for value in unique_competitions
    ]
    return competitions, unique_competitions

# Procesar jugadores
def process_players(data, country_map):
    players = [
        {
            "id": str(uuid.uuid4()),
            "nombre": row["nombre"],
            "pais": country_map[row["pais"]],
            "edad": row.get("edad"),
            "ranking_mundial": row.get("ranking"),
            "altura": row.get("altura"),
            "peso": row.get("peso"),
            "hometown": row.get("hometown"),
            "turned_pro": row.get("turned_pro"),
        }
        for _, row in data.iterrows()
    ]
    return players

# Procesar otros datos (puedes añadir más funciones aquí)

# Flujo principal
def main():
    try:
        conn = psycopg2.connect(**DB_CONFIG)

        # Cargar datos desde los archivos proporcionados
        fifa_countries = pd.read_csv("datos_a_procesar/kaggle_fifa_countries.csv", sep=";")
        libertadores_results = pd.read_csv("datos_a_procesar/libertadores-results-ds.csv", sep=",")
        player_data = pd.read_csv("datos_a_procesar/player_data_all_years.csv")
        results_data = pd.read_csv("datos_a_procesar/results.csv", sep=",")

        # 1. Procesar regiones
        regions, region_map = process_regions(fifa_countries)
        insert_data(conn, "Region", regions, ["id", "nombre", "continente"])

        # 2. Procesar países
        country_continent_map = {country: get_continent(country) for country in fifa_countries["name"].unique()}

        # Procesar países
        countries, country_map = process_countries(fifa_countries, country_continent_map, region_map)
        insert_data(conn, "Pais", countries, ["id", "nombre", "codigo", "logo_url", "region_id"])

        # 3. Procesar categorías y disciplinas
        categories, disciplines = process_categories_and_disciplines(results_data)
        insert_data(conn, "Categoria_Disciplina", categories, ["id", "nombre"])
        insert_data(conn, "Disciplina", disciplines, ["id", "nombre", "tipo", "categoria_id"])

        # 4. Procesar estadios
        #stadiums, stadium_map = process_stadiums(results_data)
        #insert_data(conn, "Estadio", stadiums, ["id", "nombre", "ubicacion", "capacidad"])

        # 5. Procesar competiciones
        competitions, competition_map = process_competitions(results_data)
        insert_data(conn, "Competicion", competitions, ["id", "nombre", "tipo"])

        # 6. Procesar jugadores
        players = process_players(player_data, country_map)
        #insert_data(conn, "Jugador", players, ["id", "nombre", "pais", "edad", "ranking_mundial", "altura", "peso", "hometown", "turned_pro"])

        print("ETL completado exitosamente.")
    except Exception as e:
        print(f"Error en el proceso ETL: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()

