import requests
from bs4 import BeautifulSoup
import pandas as pd
import os

url = 'https://www.pgachampionship.com/player/joaquin-niemann'


response = requests.get(url)
if response.status_code == 200:

    soup = BeautifulSoup(response.text, 'html.parser')


    player_name = soup.find('h1', class_='PlayerPage-name')
    if not player_name:
        player_name = soup.find('h2', class_='PlayerPage-name')
    player_name = player_name.text.strip() if player_name else "Nombre no encontrado"


    country_div = soup.find('div', class_='PlayerPage-country')
    country_name = country_div.find('div',
                                    class_='PlayerPage-country-name').text.strip() if country_div else "País no encontrado"
    flag_img = country_div.find('img', class_='Image')['src'] if country_div and country_div.find('img',
                                                                                                  class_='Image') else "URL de bandera no encontrada"


    stats = {}
    stats_section = soup.find('dl', class_='PlayerPage-left-data')
    if stats_section:
        for stat in stats_section.find_all('dt', class_='title'):
            stat_title = stat.text.strip()
            stat_value = stat.find_next_sibling('dd', class_='text').text.strip()
            stats[stat_title] = stat_value


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


        performance_data.append({
            'Año': year,
            'Nombre del Jugador': player_name,
            'País': country_name,
            'Ubicación': location,
            'Posición': finish,
            'Puntuación': score,
            **stats
        })

    performance_df = pd.DataFrame(performance_data)

    output_file = 'other-data/player_data_all_years.csv'
    performance_df.to_csv(output_file, index=False, encoding='utf-8')
    print(f'Todos los datos han sido guardados en {output_file}')
else:
    print(f"No se pudo recuperar la información. Código de estado: {response.status_code}")
