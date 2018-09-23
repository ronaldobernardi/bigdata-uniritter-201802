library(sparklyr)
library(dbplyr)
library(readr)
library(dplyr)
library(stringr)
library(purrr)
library(sparkavro)
library(sparklyr.nested)

Sys.setenv(SPARK_HOME="/usr/lib/spark")
sc <- spark_connect(master = "yarn-client", version = "2.3.1")

ratings_music <- spark_read_avro(sc, name = "ratings_music", path = "subset_meta_Digital_Music.avro", memory = TRUE)

sdf_schema(ratings_music)

ratings_music %>%
  sdf_select(asin, related$bought_together) %>%
  sdf_explode(bought_together) %>%
  arrange(asin) %>%
  count(asin, sort = TRUE)

ratings_music %>%
  select(asin, related) %>% 
  arrange(asin) %>%
  count(asin, sort = TRUE)

ratings_music %>%
  select(title)

spark_write_parquet(ratings_music, path = "subset_meta_Digital_Music.parquet")

## no terminal: wget http://central.maven.org/maven2/org/apache/parquet/parquet-tools/1.9.0/parquet-tools-1.9.0.jar
## no terminal: hadoop jar parquet-tools-1.9.0.jar meta subset_meta_Digital_Music.parquet

spark_disconnect(sc)
