# Dados de voos já utilizados em outra disciplina.
# Esta versão possui algumas diferenças em nomes de colunas
install.packages( "nycflights13" )

library(sparklyr)
library(tidyverse)

# Inicializa processo do Spark (ver processo Java em monitor de tarefas / monitor de atividades)
sc <- spark_connect(master = "local")

# Cria uma tabela no catálogo do Spark. Ver na aba Connections, navegar no conteúdo
flights_tbl <- copy_to( sc, nycflights13::flights, "flights" )

# Lista tabelas do catálogo
src_tbls(sc)

# Abre a página principal da interface gráfica do Spark. 
# Navegar um pouco e mencionar sem entrar em muitos detalhes, pois teremos outras aulas.
# Anotar tamanho da tabela em Storage
spark_web(sc)

# Comparar tamanho original com tamanho no Spark DataFrame
format( object.size(nycflights13::flights), "MB" )

# Summary funciona?
summary(flights_tbl)

# Neste caso precisamos de uma função diferente. Ver o resultado na interface gráfica
sdf_describe(flights_tbl)

nycflights13::flights %>%
  group_by( carrier ) %>%
  summarise(media_atraso_partida = mean( dep_delay, na.rm = TRUE )) %>%
  ungroup()


# Uma operação simples de média. Comentar sobre o resultado exibido
flights_tbl %>%
  group_by( carrier ) %>%
  summarise(media_atraso_partida = mean( dep_delay, na.rm = TRUE )) %>%
  ungroup()

# Atribuindo para uma variável. Por que não está na aba Connections?
media_atraso_partidas_companhias <-
  flights_tbl %>%
  group_by( carrier ) %>%
  summarise( media_atraso_partida = mean( dep_delay, na.rm = TRUE )) %>%
  ungroup() %>%
  arrange( desc( media_atraso_partida ))

# Agora sim. Olhar Storage. Por que não está lá?
tbl_atraso_medio <- sdf_register( media_atraso_partidas_companhias, name = "media_atraso" )

# Agora está. Mencionar as diferentes formas de storage.level
sdf_persist( tbl_atraso_medio, storage.level = "MEMORY_ONLY" )

# Qual o tipo de dado do retorno do collect?
collect(media_atraso_partidas_companhias)

# Como o spark irá processar este Data Frame?
show_query( media_atraso_partidas_companhias )

# E este?
show_query( tbl_atraso_medio )

# Um pouco mais de complexidade
ranking_piores_atrasos <- 
  flights_tbl %>%
    mutate( atraso_chegada_maior_partida = if_else( coalesce( arr_delay, 0 ) > coalesce( dep_delay, 0 ), 1, 0 )) %>%
    group_by( carrier ) %>%
    summarise( media_atraso_partida = mean( dep_delay, na.rm = TRUE )
             , media_atraso_chegada = mean( arr_delay, na.rm = TRUE )
             , voos                 = n()
             , nao_recuperou_tempo  = sum( atraso_chegada_maior_partida, na.rm = TRUE )) %>%
    ungroup() %>%
    arrange( desc( nao_recuperou_tempo / voos ))

show_query( ranking_piores_atrasos )

# Visualização
ranking_piores_atrasos %>%
  collect() %>%
  View( title = "Spark" )

# Comparação com resultado local
nycflights13::flights %>%
  mutate( atraso_chegada_maior_partida = if_else( coalesce( arr_delay, 0 ) > coalesce( dep_delay, 0 ), 1, 0 )) %>%
  group_by( carrier ) %>%
  summarise( media_atraso_partida = mean( dep_delay, na.rm = TRUE )
             , media_atraso_chegada = mean( arr_delay, na.rm = TRUE )
             , voos                 = n()
             , nao_recuperou_tempo  = sum( atraso_chegada_maior_partida, na.rm = TRUE )) %>%
  ungroup() %>%
  arrange( desc( nao_recuperou_tempo / voos )) %>%
  collect() %>%
  View( title = "Local" )


# Regressão linear, exemplo das disciplinas anteriores
model_data_tbl <- 
  flights_tbl %>%
  filter( !is.na( arr_delay ) & !is.na( dep_delay ) & !is.na( distance )) %>%
  filter( dep_delay > 15 & dep_delay < 240 ) %>%
  filter( arr_delay > -60 & arr_delay < 360 ) %>%
  mutate( gain = dep_delay - arr_delay ) %>%
  filter( gain > -100 ) %>%
  select( year, month, arr_delay, dep_delay, distance, carrier, origin, dest, gain )

sparklyr::ml_linear_regression( model_data_tbl, gain ~ distance + dep_delay + carrier ) -> lm_model_spark
  
model_data <- 
  nycflights13::flights %>%
  filter( !is.na( arr_delay ) & !is.na( dep_delay ) & !is.na( distance )) %>%
  filter( dep_delay > 15 & dep_delay < 240 ) %>%
  filter( arr_delay > -60 & arr_delay < 360 ) %>%
  mutate( gain = dep_delay - arr_delay ) %>%
  filter( gain > -100 ) %>%
  select( year, month, arr_delay, dep_delay, distance, carrier, origin, dest, gain )

lm( gain ~ distance + dep_delay + carrier, model_data_tbl ) -> lm_model_local

# Comparação dos resultados
sparklyr::ml_predict( lm_model_spark, model_data_tbl )

model_data %>% 
  add_column( prediction = predict( lm_model_local ))

# E os modelos???
sparklyr::ml_summary(lm_model_spark)

summary(lm_model_local)

# predição retorna um data frame utilizável com outras operações
erros_previsao_recuperacao_atraso <- 
  sparklyr::ml_predict( lm_model_spark, model_data_tbl ) %>%
    mutate( erro_sinal = sign( gain ) != sign( prediction )) %>%
    count( erro_sinal )

# Vamos olhar os detalhes da query de CreateViewCommand
show_query(erros_previsao_recuperacao_atraso)
collect( erros_previsao_recuperacao_atraso )

# features per tree = 1/3
ml_random_forest_regressor( x = model_data_tbl
                          , formula = gain ~ distance + dep_delay + carrier
                          , num_trees = 75
                          , subsampling_rate = 0.8
                          , max_depth = 15
                          , min_instances_per_node = 2
                          , seed = 1235 ) -> spark_rf_regressor

# Ops, e agora?
sparklyr::ml_summary( spark_rf_regressor )

print(spark_rf_regressor)

ml_tree_feature_importance(spark_rf_regressor)

sparklyr::ml_predict( spark_rf_regressor, model_data_tbl ) %>%
  ml_regression_evaluator( label_col = "gain" )

sparklyr::ml_predict( lm_model_spark, model_data_tbl ) %>%
  ml_regression_evaluator( label_col = "gain" )

ml_random_forest_regressor( x = model_data_tbl
                            , formula = gain ~ dep_delay + distance + carrier + origin + dest
                            , num_trees = 75
                            , subsampling_rate = 0.8
                            , max_depth = 15
                            , min_instances_per_node = 2
                            , seed = 1235 ) -> spark_rf_regressor_complete


sparklyr::ml_predict( spark_rf_regressor_complete, model_data_tbl ) %>%
  ml_regression_evaluator( label_col = "gain" )

ml_tree_feature_importance(spark_rf_regressor_complete)

