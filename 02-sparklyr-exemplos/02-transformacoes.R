library(sparklyr)
library(dbplyr)
library(dplyr)

# Inicializa processo do Spark 
Sys.setenv(SPARK_HOME="/usr/lib/spark")
sc <- spark_connect(master = "yarn-client", version = "2.3.0")

# Cria uma tabela no catálogo do Spark. Ver na aba Connections, navegar no conteúdo
flights_tbl <- sdf_copy_to( sc, nycflights13::flights, "flights" )

# Nessa não precisa navegar :)
flights_parquet <- spark_read_parquet(sc = sc, name = "flights_parquet", path = "/user/hadoop/airlines_parquet", memory = FALSE, repartition = 40)

# Uma operação simples de média.
# Uso de dplyr para calcular a média de atraso de partida (dep_delay) por companhia aérea (carrier) 

# Primeiro, utilizando o data.frame do R
nycflights13::flights %>%
  group_by( carrier ) %>%
  summarise(media_atraso_partida = mean( dep_delay, na.rm = TRUE )) %>%
  ungroup() 

# Segundo, utilizando o flights_tbl do Spark
flights_tbl %>%
  group_by( carrier ) %>%
  summarise(media_atraso_partida = mean( dep_delay, na.rm = TRUE )) %>%
  ungroup() 

## Agora façam o mesmo ordenando por carrier
flights_tbl %>%
  group_by( carrier ) %>%
  summarise(media_atraso_partida = mean( dep_delay, na.rm = TRUE )) %>%
  ungroup() %>%
  arrange(carrier)

## Terceiro, utilizando o dataframe flights_parquet
flights_parquet %>%
  mutate( carrier = as.character(carrier)) %>%
  group_by( carrier ) %>%
  summarise(media_atraso_partida = mean( depdelay, na.rm = TRUE )) %>%
  ungroup() %>%
  arrange(carrier)


### LAZY OPERATIONS
flights_parquet %>%
  mutate( carrier = as.character(carrier)) %>%
  group_by( carrier ) %>%
  summarise(media_atraso_partida = mean( depdelay, na.rm = TRUE )) %>%
  ungroup() %>%
  arrange(carrier) -> delay_per_carrier

explain(delay_per_carrier)
show_query(delay_per_carrier)
sdf_debug_string(delay_per_carrier, print = TRUE)


### Recuperando resultados para o R
media_atraso_partidas_companhias <-
  flights_parquet %>%
  mutate( carrier = as.character(carrier)) %>%
  group_by( carrier ) %>%
  summarise( media_atraso_partida = mean( depdelay, na.rm = TRUE )) %>%
  ungroup() %>%
  arrange( desc( media_atraso_partida ))

show_query(media_atraso_partidas_companhias)
resultado_media_atraso_partida <- collect(media_atraso_partidas_companhias)

resultado_media_atraso_partida


### CACHES

# Agora atribuir o resultado da operação sparklyr para uma variável chamada media_atraso_partidas_companhias
tbl_atraso_medio <- sdf_register( media_atraso_partidas_companhias, name = "media_atraso" )

# Persistindo (cache) em memória
sdf_persist( tbl_atraso_medio, storage.level = "MEMORY_ONLY" )

# Qual o tipo de dado do retorno do collect?
collect(media_atraso_partidas_companhias)

# Como o spark irá processar este Data Frame?
show_query( media_atraso_partidas_companhias )

# E este?
show_query( tbl_atraso_medio )


###### ATIVIDADE PARA OS GRUPOS ######
## Façam o mesmo com flights_parquet, mas calculando além da média o desvio padrão e o coeficiente de variação.
## Ordenem pelo maior coeficiente de variação e armazenem em cache o resultado da consulta.




# Um pouco mais de complexidade.

# Criar uma variável chamada atraso_chegada_maior_partida, que será 1 quando arr_delay for maior que dep_delay e 0 caso contrário
# Calcular as seguintes sumarizações por companhia aérea, no mesmo dataframe:
#   * Média de atraso de partida (dep_delay)
#   * Média de atraso de chegada (arr_delay)
#   * Quantidade de voos por companhia
#   * Quantidade de voos onde houve atraso na partida e a companhia não conseguiu reduzir o atraso na chegada (usar a nova variável criada)
#   * Ordenar o resultado pela taxa de voos nos quais a companhia não conseguiu reduzir o atraso na chegada, em ordem decrescente (atrasos / total)
# atribuir o resultado a uma variável chamada ranking_piores_atrasos
ranking_piores_atrasos <- ...

show_query( ranking_piores_atrasos )

sdf_describe( ranking_piores_atrasos )

# Visualização
ranking_piores_atrasos %>%
  collect() %>%
  View( title = "Spark" )

# Comparação com resultado local
# Repetir a mesma operação com o data frame local, nycflights13::flights. Vamos comparar o resultado

spark_disconnect(sc)
