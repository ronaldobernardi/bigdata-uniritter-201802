library(sparklyr)
library(tidyverse)

# Inicializa processo do Spark 
sc <- spark_connect(master = "local")

# Cria uma tabela no catálogo do Spark. 
flights_tbl <- copy_to( sc, nycflights13::flights, "flights" )

# Gera a saída nos formatos parquet e csv
spark_write_parquet(flights_tbl, "flights_tbl.parquet")
spark_write_csv(flights_tbl, "flights_tbl.csv")

tbl_uncache(sc, "flights")

flights_tbl <- spark_read_parquet(sc, path = "flights_tbl.parquet", name = "flights")

sdf_describe( flights_tbl )

## Repetir os passos para da atividade anterior para treinar uma RF e estimar a variável gain no dataset de testes
## Salvar o resultado do predict em um arquivo parquet.
## desconectar a conexão do spark, conectar novamente, ler o arquivo parquet recém escrito e calcular o RMSE do dataframe 