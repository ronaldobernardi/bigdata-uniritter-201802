#######################################################################################
# NÃO MODIFICAR ANTES DO COMENTÁRIO QUE DEMARCA O INÍCIO DAS ATIVIDADES
#######################################################################################

# Importação de funções utilizadas em operações com Dataframe API
# Podem ser utilizadas no desenvolvimento das atividades.
# Se identificarem outras funções necessárias, insiram uma nova linha após a marcação 
from pyspark.sql.functions import udf, col, lit, expr, broadcast
from pyspark.sql.functions import count, min, max

# Importação de definições de tipos de dados necessárias para declaração das UDFs abaixo
from pyspark.sql.types import StringType, DoubleType

# Funções que foram utilizadas nas UDFs
from math import cos, asin, sqrt


#######################################################################################
# Arquivos de Dados 
#######################################################################################

# Leitura dos arquivos no S3. 
# Cada arquivo possui um formato diferente dos demais, 
# sendo necessário ajuste de schema para possibilitar a união em um único DataFrame
taxi_2013 = spark.read.parquet("s3a://aula-spark/green_tripdata/parquet/green_tripdata_2013.parquet") 
taxi_2014 = spark.read.parquet("s3a://aula-spark/green_tripdata/parquet/green_tripdata_2014.parquet") 
taxi_2015 = spark.read.parquet("s3a://aula-spark/green_tripdata/parquet/green_tripdata_2015.parquet") 
taxi_2016 = spark.read.parquet("s3a://aula-spark/green_tripdata/parquet/green_tripdata_2016.parquet") 

# Ajustes no Schema dos dados de 2016
t2016 = taxi_2016 \
    .withColumnRenamed("lpep_pickup_datetime", "pickup_datetime") \
    .withColumnRenamed("Lpep_dropoff_datetime", "dropoff_datetime") \
    .withColumnRenamed("RateCodeID", "rate_code") \
    .withColumnRenamed("improvement_surcharge", "Improvement_surcharge") \
    .drop("VendorID") \
    .drop("PULocationID") \
    .drop("DOLocationID")

# Ajustes no Schema dos dados de 2015
t2015 = taxi_2015.drop("VendorID")

# Ajustes no Schema dos dados de 2014
t2014 = taxi_2014.drop("VendorID").withColumn("Improvement_surcharge", lit(0.0))

# Ajustes no Schema dos dados de 2013
t2013 = taxi_2013.drop("vendor_id").withColumn("Improvement_surcharge", lit(0.0))


#######################################################################################
# Criação do DataFrame que deverá ser utilizado nas atividades
#######################################################################################

green_taxis = t2016.unionAll(t2015).unionAll(t2014).unionAll(t2013)

# Repartition para obter uma distribuição mais homogênea de registros por partição
green_taxis = green_taxis.repartition(96, "Pickup_latitude", "dropoff_datetime")

# Registro de tabela no catálogo do Spark SQL para quem quiser utilizar SQL
green_taxis.createOrReplaceTempView("green_taxis")


#######################################################################################
# Registro de User Defined Functions (UDFs) que serão utilizadas nas atividades
#######################################################################################


###### Distância entre 2 pares de coordenadas pela Fórmula de Haversine
def distance(lat1, lon1, lat2, lon2):
    p = 0.017453292519943295     #Pi/180
    a = 0.5 - cos((lat2 - lat1) * p)/2 + cos(lat1 * p) * cos(lat2 * p) * (1 - cos((lon2 - lon1) * p)) / 2
    return 12742 * asin(sqrt(a))

# Dataframe API
distance_udf = udf(lambda lat1, lon1, lat2, lon2: distance(lat1, lon1, lat2, lon2), DoubleType())

# SQL
sqlContext.udf.register("distance_udf", distance)


#######################################################################################
##############                DESENVOLVIMENTO INICIA AQUI                ##############
#######################################################################################
from pyspark import StorageLevel

from pysparkling import *
h2oConf = H2OConf(spark)

porta = ???? # USAR 54320 + número do grupo

h2oConf.set_client_port_base(porta)
h2oConf.set_node_base_port(porta)

hc = H2OContext.getOrCreate(spark, h2oConf)


corridas_com_coordenadas = green_taxis \
    .where((col("Pickup_latitude") != 0) & (col("Pickup_longitude") != 0 ) & \
           (col("Dropoff_latitude") != 0) & (col("Dropoff_longitude") != 0 )) \
    .select("*", \
        (green_taxis.Trip_distance * 1.609).alias('Trip_distance_km'), \
        expr('tip_amount / fare_amount as tip_rate'), \
        distance_udf("Pickup_latitude", "Pickup_longitude", \
                     "Dropoff_latitude", "Dropoff_longitude").alias("calc_distance"))

corridas_ratio = corridas_com_coordenadas \
    .select("*", \
        (corridas_com_coordenadas.calc_distance / corridas_com_coordenadas.Trip_distance_km).alias('dist_ratio')) \
    .persist(StorageLevel.DISK_ONLY)


h2o_taxi_rate = hc.as_h2o_frame(corridas_ratio, "corridas_ratio")

### POR ENQUANTO DEPOIS VAMOS MUDAR ISSO
train, validation, test = h2o_taxi_rate.split_frame(ratios=[.4, .1], seed=1234)


from h2o.estimators.glm import *

glm_regression = H2OGeneralizedLinearEstimator(family="gaussian", lambda_search=False, alpha=0)

glm_regression.train( y="tip_rate", training_frame = train, validation_frame = validation, \
                      x=['Passenger_count', 'Trip_distance', 'Fare_amount', 'Extra', 'Tolls_amount'])

glm_regression._model_json['output']
