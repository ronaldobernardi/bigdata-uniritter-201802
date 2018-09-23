library(sparklyr)
library(dplyr)
library(ggplot2)
library(tibble)

# Inicializa processo do Spark 
Sys.setenv(SPARK_HOME="/usr/lib/spark")
sc <- spark_connect(master = "yarn-client", version = "2.3.0")

# Cria uma tabela no catálogo do Spark. 
flights_tbl <- copy_to( sc, nycflights13::flights, "flights" )
flights_parquet <- spark_read_parquet(sc = sc, name = "flights_parquet", path = "/user/hadoop/airlines_parquet", memory = FALSE, repartition = 40)

# Regressão linear, exemplo das disciplinas anteriores (explicar)
model_data_tbl <- 
  flights_parquet %>%
  mutate(carrier = as.character(carrier), origin = as.character(origin), dest = as.character(dest)) %>%
  filter( !is.na( arrdelay ) & !is.na( depdelay ) & !is.na( distance )) %>%
  filter( depdelay > 15 & depdelay < 240 ) %>%
  filter( arrdelay > -60 & arrdelay < 360 ) %>%
  mutate( gain = depdelay - arrdelay ) %>%
  filter( gain > -100 ) %>%
  select( arrdelay, depdelay, distance, carrier, origin, dest, gain )

explain(model_data_tbl)

model_data_tbl <- sdf_sample(model_data_tbl, fraction = 0.01, seed = 1235)

explain(model_data_tbl)

head(model_data_tbl, 10)

train_tbl <- sdf_sample(model_data_tbl, fraction = 0.6, seed = 1235) %>% 
  sdf_repartition(40) %>% 
  sdf_persist( storage.level = "MEMORY_ONLY" )

test_tbl <- setdiff(model_data_tbl, train_tbl)

sparklyr::ml_linear_regression( train_tbl, gain ~ distance + depdelay + carrier ) -> lm_model_spark

lm_model_spark
summary(lm_model_spark)

sparklyr::ml_linear_regression( train_tbl, gain ~ distance + depdelay + carrier, 
                                elastic_net_param = 0.5, reg_param = 0.001 ) -> lm_model_spark_enet

summary(lm_model_spark_enet)
sparklyr::ml_summary(lm_model_spark_enet)


ml_random_forest_regressor( x = train_tbl
                            , formula = gain ~ distance + depdelay + carrier
                            , num_trees = 50
                            , feature_subset_strategy = "onethird"
                            , subsampling_rate = 0.63
                            , max_depth = 11
                            , min_instances_per_node = 15
                            , seed = 1235 ) -> spark_rf_regressor

sparklyr::ml_summary(spark_rf_regressor, metric = "RMSE")
summary(spark_rf_regressor)
print(spark_rf_regressor)

ml_tree_feature_importance( spark_rf_regressor )

sparklyr::ml_predict( spark_rf_regressor, test_tbl ) %>%
  ml_regression_evaluator( label_col = "gain", metric_name = "rmse" )

sparklyr::ml_predict( lm_model_spark_enet, test_tbl ) %>%
  ml_regression_evaluator( label_col = "gain", metric_name = "rmse" )

# Usando o ggplot, criar gráficos de comparação previsto vs gain nos resultados dos modelos linear e RF aplicados no dataset de testes 




# Criar outra regressão com Random Forest utilizando as seguintes features: dep_delay + distance + carrier + origin + dest
# Calcular a métrica de RMSE no spark data frame de testes
# Listar a importância das features
# Criar gráfico comparativo previsto vs gain



spark_disconnect(sc)
