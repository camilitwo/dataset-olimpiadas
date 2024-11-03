# Olympics-Dataset

Este repositorio contiene un conjunto de datos completo sobre los atletas olímpicos de verano e invierno y sus resultados entre 1896 y 2022 (se actualizará con los resultados de 2024 después de los próximos Juegos de París)

![Olympic Flame](./assets/olympic_flame.jpeg)

## Información del conjunto de datos y proceso de recopilación

Estos datos provienen de [olympedia.org](https://www.olympedia.org/) y se extrajeron de la web con la biblioteca Python Beautiful Soup (consulte [scrape_data.py](./scrape_data.py))

- [athletes/bios.csv](./athletes/bios.csv) contiene la información biográfica sin procesar de cada atleta<br/>
- [results/results.csv](./results/results.csv) contiene un desglose fila por fila de cada evento en el que compitieron los atletas y sus resultados En ese caso.

Tenga en cuenta que, en el proceso de extracción de este conjunto de datos, se crearon archivos CSV temporales para controlar el progreso de la extracción. Para simplificar, estos archivos con puntos de control se eliminaron del repositorio.

## Datos limpios

Se pueden encontrar datos más fáciles de analizar en la carpeta [clean-data/](./clean-data/). Además de los resultados y la información de las biografías, puede encontrar archivos de datos con datos adicionales de ubicación de latitud y longitud de los atletas, códigos de región de los CON y poblaciones históricas de los países a lo largo del tiempo.