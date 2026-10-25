# Análisis de casos de COVID-19 en Colombia con Apache Spark

Tarea 3 del curso Big Data, UNAD. Procesamiento en batch y en tiempo real con Apache Spark y Apache Kafka.

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

## Procesamiento en tiempo real con Kafka y Spark Streaming

Archivos:

- `kafka_producer.py`: productor que simula 10 sensores y envía cada segundo un registro con temperatura, humedad y marca de tiempo al tema `sensor_data` de Kafka.
- `spark_streaming_consumer.py`: consumidor con Spark Structured Streaming que lee el tema, convierte el JSON a columnas y calcula el promedio de temperatura y humedad por sensor en ventanas de 1 minuto.

Entorno: máquina virtual Ubuntu Server con Hadoop 3.3.6, Spark 3.5.8, Kafka 3.9.2 y Python 3.

Pasos de ejecución:

1. Iniciar ZooKeeper y Kafka:
   ```
   /opt/Kafka/bin/zookeeper-server-start.sh -daemon /opt/Kafka/config/zookeeper.properties
   /opt/Kafka/bin/kafka-server-start.sh -daemon /opt/Kafka/config/server.properties
   ```
2. Crear el tema:
   ```
   /opt/Kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic sensor_data
   ```
3. En una terminal, ejecutar el productor:
   ```
   python3 kafka_producer.py
   ```
4. En otra terminal, ejecutar el consumidor:
   ```
   spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 spark_streaming_consumer.py
   ```
5. Revisar el procesamiento en la interfaz web de Spark, en el puerto 4040.

Resultado: Spark procesa un lote nuevo cada vez que llegan datos y actualiza la tabla de promedios por sensor y minuto. En la prueba se procesaron más de 500 lotes, cada uno en unos 2 segundos.

## Tecnologías

Python, Apache Spark con PySpark, Spark Structured Streaming, Apache Kafka y Matplotlib.
