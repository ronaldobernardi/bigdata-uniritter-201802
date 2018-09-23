library(sparklyr)
library(dbplyr)
library(readr)
library(dplyr)
library(stringr)
library(purrr)

Sys.setenv(SPARK_HOME="/usr/lib/spark")
sc <- spark_connect(master = "yarn-client", version = "2.3.1")

ratings_music <- spark_read_json(sc, name = "ratings_music", path = "/user/hadoop/meta_Digital_Music.strict.json.gz", memory = FALSE)

sdf_schema(ratings_music)

sdf_dim(ratings_music)

head(ratings_music)

ratings_music %>%
  filter(asin == "5555991584") %>%
  collect()

ratings_music %>%
  filter(asin == "5555991584") %>%
  show_query()

# Quais são as 100 marcas (brands) mais comuns? Das 100 mais comuns, exiba todas aquelas com no mínimo 2 ocorrências e mostre na console a query resultante desta análise
ratings_music %>%
  count(brand) %>%
  arrange(desc(n)) %>%
  head(100) %>%
  filter(n > 1) %>%
  print(n = 100)

ratings_music %>%
  count(brand, sort = TRUE) %>%
  head(100) %>%
  filter(n > 1) %>% show_query()

library(sparkavro)

spark_write_avro(ratings_music, path = "subset_meta_Digital_Music.avro", options = list(compression = "snappy"))

## COPIAR ARQUIVO AVRO PARA HOME
## USAR avro-tools PARA VER O SCHEMA (getschema) DO ARQUIVO

# java -jar ./avro-tools-1.7.7.jar getschema ~/Documents/Uniritter/2018/big-data/03-formatos-arquivos/data/subset_meta_Digital_Music.avro/*.avro
# java -jar ./avro-tools-1.7.7.jar tojson --pretty ~/Documents/Uniritter/2018/big-data/03-formatos-arquivos/data/subset_meta_Digital_Music.avro/*.avro | head -1000


spark_disconnect(sc)
spark_disconnect_all()
