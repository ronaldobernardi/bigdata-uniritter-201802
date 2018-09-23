library(sparklyr)
library(dbplyr)

Sys.setenv(SPARK_HOME="/usr/lib/spark")
sc <- spark_connect(master = "yarn-client", version = "2.3.0")

# Cria uma tabela no catálogo do Spark.
flights_tbl <- sdf_copy_to( sc, nycflights13::flights, "flights" )
flights_parquet <- spark_read_parquet(sc = sc, name = "flight_parquet", path = "/user/hadoop/airlines_parquet", memory = FALSE, repartition = 40)

# Lista tabelas do catálogo
src_dbi(sc)

# Dimensões do DataFrame
sdf_dim(flights_tbl)
sdf_dim(flights_parquet)

sdf_schema(flights_parquet) %>% dplyr::bind_rows() %>% View()

# Summary funciona?
summary(flights_tbl)
summary(flights_parquet)

# Neste caso precisamos de uma função diferente.
sdf_describe(flights_tbl)
sdf_describe(flights_parquet, cols = c("distance", "arrdelay", "depdelay"))

spark_disconnect(sc)

spark_disconnect_all()
