import requests
from bs4 import BeautifulSoup
import pandas as pd
import json

# URL de la página del jugador
url = 'https://www.pgachampionship.com/player/joaquin-niemann'

# Realizar la solicitud HTTP
response = requests.get(url)
if response.status_code == 200:
    # Parsear el HTML con BeautifulSoup
    soup = BeautifulSoup(response.text, 'html.parser')

    # Nombre del jugador
    player_name = soup.find('h1', class_='PlayerPage-name')
    if not player_name:
        player_name = soup.find('h2', class_='PlayerPage-name')
    player_name = player_name.text.strip() if player_name else "Nombre no encontrado"

    # País y URL de la bandera
    country_div = soup.find('div', class_='PlayerPage-country')
    country_name = country_div.find('div',
                                    class_='PlayerPage-country-name').text.strip() if country_div else "País no encontrado"
    flag_img = country_div.find('img', class_='Image')['src'] if country_div and country_div.find('img',
                                                                                                  class_='Image') else "URL de bandera no encontrada"

    # Estadísticas generales
    stats = {}
    stats_section = soup.find('dl', class_='PlayerPage-left-data')
    if stats_section:
        for stat in stats_section.find_all('dt', class_='title'):
            stat_title = stat.text.strip()
            stat_value = stat.find_next_sibling('dd', class_='text').text.strip()
            stats[stat_title] = stat_value

    # Actuaciones anteriores
    performance_data = []
    for row in soup.select('.SitesRow-tr'):
        year = row.select_one('.SitesRow-year').text.strip() if row.select_one(
            '.SitesRow-year') else "Año no encontrado"
        location = row.select_one('.SitesRow-winner-firstName').text.strip() if row.select_one(
            '.SitesRow-winner-firstName') else "Ubicación no encontrada"
        finish = row.select_one('.SitesRow-position').text.strip() if row.select_one(
            '.SitesRow-position') else "Posición no encontrada"
        score = row.select_one('.SitesRow-overallPar').text.strip() if row.select_one(
            '.SitesRow-overallPar') else "Puntuación no encontrada"

        # Añadir información adicional de cada fila
        performance_data.append({
            'Año': year,
            'Nombre del Jugador': player_name,
            'País': country_name,
            'Ubicación': location,
            'Posición': finish,
            'Puntuación': score,
            **stats  # Agregar todas las estadísticas generales al registro
        })

    # Convertir datos a JSON
    output_data = {
        "Jugador": player_name,
        "País": country_name,
        "Estadísticas Generales": stats,
        "Actuaciones": performance_data
    }

    # Guardar como JSON
    output_file = 'other-data/player_data_all_years.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, ensure_ascii=False, indent=4)

    print(f'Todos los datos han sido guardados en {output_file}')
else:
    print(f"No se pudo recuperar la información. Código de estado: {response.status_code}")
