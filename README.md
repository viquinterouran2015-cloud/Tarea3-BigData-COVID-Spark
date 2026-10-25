# Análisis de casos de COVID-19 en Colombia con Apache Spark

Tarea 3 del curso Big Data, UNAD. Procesamiento en batch con PySpark.

**Autora:** Laura Victoria Quintero Urán

## Problema

Identificar qué departamentos, grupos de edad y sexos concentraron más casos y muertes por COVID-19 en Colombia, y calcular la tasa de letalidad por grupo de edad.

## Conjunto de datos

**Casos positivos de COVID-19 en Colombia**, publicado por el Instituto Nacional de Salud en el portal de datos abiertos de Colombia.

- Fuente: https://www.datos.gov.co/Salud-y-Protecci-n-Social/Casos-positivos-de-COVID-19-en-Colombia/gt2j-8ykr
- Muestra utilizada: 1.000.000 de registros descargados desde la API del portal.
- Columnas usadas: id del caso, departamento, edad, sexo, estado, recuperado y fecha de muerte.

## Qué hace el código

1. **Carga** el archivo CSV en un DataFrame de Spark.
2. **Limpia** los datos: normaliza los nombres de columnas, elimina registros sin departamento o edad, estandariza el texto, descarta edades fuera de 0 a 110 años y elimina duplicados según el id del caso.
3. **Transforma** los datos: crea grupos de edad y una columna que marca los casos fallecidos.
4. **Analiza** los datos: estadísticas de la edad, top 10 de departamentos con más casos, casos y letalidad por grupo de edad, casos por sexo y un conteo por departamento con RDD usando `map` y `reduceByKey`.
5. **Visualiza** los resultados en tres gráficas.

## Principales resultados

- La edad promedio de los casos es de 39,6 años.
- Bogotá concentra cerca del 30 % de los casos de la muestra, seguida de Antioquia y Valle.
- La letalidad aumenta con la edad: 0,15 % en menores de 20 años y 33,15 % en mayores de 80.
- El 52,7 % de los casos corresponde a mujeres y el 47,3 % a hombres.

## Cómo ejecutarlo en Google Colab

1. Crear un cuaderno nuevo en https://colab.research.google.com
2. Instalar PySpark:
   ```
   !pip install pyspark
   ```
3. Descargar los datos:
   ```
   !wget -O covid.csv 'https://www.datos.gov.co/resource/gt2j-8ykr.csv?$limit=1000000'
   ```
4. Copiar y ejecutar el contenido de `analisis_covid_spark.py` por bloques, en el orden de los comentarios numerados.

## Tecnologías

Python, Apache Spark con PySpark y Matplotlib.
